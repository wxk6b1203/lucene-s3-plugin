package com.github.wxk6b1203.store;

import com.github.wxk6b1203.metadata.common.IndexFile;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.metadata.provider.mem.MemMockProvider;
import com.github.wxk6b1203.store.common.PathUtil;
import com.github.wxk6b1203.store.directory.S3CachingDirectory;
import com.github.wxk6b1203.store.directory.S3DirectoryOptions;
import com.github.wxk6b1203.store.directory.S3LockFactory;
import com.github.wxk6b1203.store.manifest.ManifestManager;
import com.github.wxk6b1203.store.manifest.ManifestOptions;
import com.github.wxk6b1203.store.object.RemoteObjectStore;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class S3CachingDirectoryTest {
    @TempDir
    Path tempDir;

    @Test
    public void testCommitPublishesAfterSegmentsFile() throws IOException {
        MemMockProvider metadata = new MemMockProvider();
        try (S3CachingDirectory directory = newDirectory("test-index", metadata)) {
            writeFile(directory, "_0.si", new byte[]{1, 2, 3});
            directory.sync(List.of("_0.si"));
            assertEquals(0, metadata.listAll("test-index", List.of(IndexFileStatus.DIRTY, IndexFileStatus.CLEAN)).size());

            writeFile(directory, "segments_1", new byte[]{4});
            directory.sync(List.of("segments_1"));
            directory.syncMetaData();

            waitForUploads(metadata, "test-index", 2);
            assertEquals(2, metadata.listAll("test-index", List.of(IndexFileStatus.CLEAN)).size());
        }
    }

    @Test
    public void testReadPrefersWalOverSharedCache() throws IOException {
        MemMockProvider metadata = new MemMockProvider();
        String indexName = "test-index";
        try (S3CachingDirectory directory = newDirectory(indexName, metadata)) {
            Files.write(PathUtil.sharedDataPath(tempDir, indexName).resolve("segments_1"), new byte[]{1});
            writeFile(directory, "segments_1", new byte[]{2});

            byte[] actual = new byte[1];
            try (IndexInput input = directory.openInput("segments_1", IOContext.DEFAULT)) {
                input.readBytes(actual, 0, actual.length);
            }
            assertArrayEquals(new byte[]{2}, actual);
        }
    }

    @Test
    public void testDeleteFileRemovesWalAndSharedCacheCopies() throws IOException {
        MemMockProvider metadata = new MemMockProvider();
        String indexName = "test-index";
        try (S3CachingDirectory directory = newDirectory(indexName, metadata)) {
            writeFile(directory, "segments_1", new byte[]{1});
            Files.write(PathUtil.sharedDataPath(tempDir, indexName).resolve("_0.si"), new byte[]{2});

            directory.deleteFile("segments_1");
            directory.deleteFile("_0.si");

            assertFalse(Files.exists(PathUtil.walDataPath(tempDir, indexName).resolve("segments_1")));
            assertFalse(Files.exists(PathUtil.sharedDataPath(tempDir, indexName).resolve("_0.si")));
            assertArrayEquals(new String[0], directory.listAll());
        }
    }

    @Test
    public void testDeleteFileKeepsCommittedRemoteSnapshotMetadata() throws IOException {
        MemMockProvider metadata = new MemMockProvider();
        int cleanEpoch = metadata.commitFile(new IndexFile("test-index", "segments_1", 1, 0));
        metadata.updateFileStatus("test-index", "segments_1", cleanEpoch, IndexFileStatus.UPLOADING);
        metadata.updateFileStatus("test-index", "segments_1", cleanEpoch, IndexFileStatus.CLEAN);

        try (S3CachingDirectory directory = newDirectory("test-index", metadata)) {
            directory.deleteFile("segments_1");
        }

        try (S3CachingDirectory directory = newReadOnlyDirectory("test-index", metadata)) {
            assertArrayEquals(new String[]{"segments_1"}, directory.listAll());
        }
    }

    @Test
    public void testReadOnlySnapshotUsesLastCleanRemoteCommitWhenNewerUploadExists() throws IOException {
        MemMockProvider metadata = new MemMockProvider();
        int cleanEpoch = metadata.commitFile(new IndexFile("test-index", "segments_1", 1, 0));
        metadata.updateFileStatus("test-index", "segments_1", cleanEpoch, IndexFileStatus.UPLOADING);
        metadata.updateFileStatus("test-index", "segments_1", cleanEpoch, IndexFileStatus.CLEAN);
        int uploadingEpoch = metadata.commitFile(new IndexFile("test-index", "segments_2", 1, 0));
        metadata.updateFileStatus("test-index", "segments_2", uploadingEpoch, IndexFileStatus.UPLOADING);

        try (S3CachingDirectory directory = newReadOnlyDirectory("test-index", metadata)) {
            assertArrayEquals(new String[]{"segments_1"}, directory.listAll());
            assertEquals(1, directory.fileLength("segments_1"));
            assertThrows(NoSuchFileException.class, () -> directory.fileLength("segments_2"));
        }
    }

    @Test
    public void testReadOnlySnapshotDoesNotExposeWalFiles() throws IOException {
        MemMockProvider metadata = new MemMockProvider();
        try (S3CachingDirectory directory = newDirectory("test-index", metadata)) {
            writeFile(directory, "segments_1", new byte[]{1});
            directory.sync(List.of("segments_1"));
        }

        try (S3CachingDirectory directory = newReadOnlyDirectory("test-index", metadata)) {
            assertArrayEquals(new String[0], directory.listAll());
            assertThrows(NoSuchFileException.class, () -> directory.fileLength("segments_1"));
        }
    }

    @Test
    public void testReadOnlySnapshotDoesNotUseUnreadableSharedCache() throws IOException {
        MemMockProvider metadata = new MemMockProvider();
        int uploadingEpoch = metadata.commitFile(new IndexFile("test-index", "segments_1", 1, 0));
        metadata.updateFileStatus("test-index", "segments_1", uploadingEpoch, IndexFileStatus.UPLOADING);
        Files.createDirectories(PathUtil.sharedDataPath(tempDir, "test-index"));
        Files.write(PathUtil.sharedDataPath(tempDir, "test-index").resolve("segments_1"), new byte[]{1});

        try (S3CachingDirectory directory = newReadOnlyDirectory("test-index", metadata)) {
            assertArrayEquals(new String[0], directory.listAll());
            assertThrows(NoSuchFileException.class, () -> directory.fileLength("segments_1"));
        }
    }

    @Test
    public void testSharedCacheIsRefreshedWhenRemoteMetadataChangesForSameLogicalFile() throws IOException {
        MemMockProvider metadata = new MemMockProvider();
        MapRemoteObjectStore remote = new MapRemoteObjectStore();
        String indexName = "test-index";
        String fileName = "segments_1";
        byte[] stale = new byte[]{1};
        byte[] current = new byte[]{2};
        Path sharedFile = PathUtil.sharedDataPath(tempDir, indexName).resolve(fileName);
        Files.createDirectories(sharedFile.getParent());
        Files.write(sharedFile, stale);

        String objectKey = indexName + "/_data/" + fileName + ".current";
        remote.putObject(objectKey, current);
        int epoch = metadata.commitFile(new IndexFile(
                indexName,
                fileName,
                "_data",
                objectKey,
                current.length,
                crc32(current),
                System.currentTimeMillis()
        ));
        metadata.updateFileStatus(indexName, fileName, epoch, IndexFileStatus.UPLOADING);
        metadata.updateFileStatus(indexName, fileName, epoch, IndexFileStatus.CLEAN);

        ManifestManager manager = new ManifestManager(new ManifestOptions("test-bucket"), remote, metadata);
        try (S3CachingDirectory directory = new S3CachingDirectory(
                new S3DirectoryOptions(tempDir, indexName),
                new S3LockFactory(),
                manager,
                List.of(IndexFileStatus.CLEAN, IndexFileStatus.PINNED),
                false
        )) {
            byte[] actual = new byte[1];
            try (IndexInput input = directory.openInput(fileName, IOContext.DEFAULT)) {
                input.readBytes(actual, 0, actual.length);
            }
            assertArrayEquals(current, actual);
        }
    }

    @Test
    public void testPublishLocalCommitRetriesFailedUploadWithoutNewCommit() throws IOException {
        MemMockProvider metadata = new MemMockProvider();
        FailingRemoteObjectStore remote = new FailingRemoteObjectStore("_0.si");
        ManifestManager manager = new ManifestManager(new ManifestOptions("test-bucket"), remote, metadata);
        try (S3CachingDirectory directory = new S3CachingDirectory(
                new S3DirectoryOptions(tempDir, "test-index"),
                new S3LockFactory(),
                manager
        )) {
            writeFile(directory, "_0.si", new byte[]{1});
            directory.sync(List.of("_0.si"));
            writeFile(directory, "segments_1", new byte[]{2});
            directory.sync(List.of("segments_1"));
            directory.syncMetaData();

            waitUntil(() -> metadata.fileMetadata("test-index", "_0.si") != null
                    && metadata.fileMetadata("test-index", "_0.si").getStatus() == IndexFileStatus.UPLOADING);

            remote.failName(null);
            waitUntil(() -> {
                try {
                    directory.publishLocalCommit();
                    return metadata.listAll("test-index", List.of(IndexFileStatus.CLEAN)).size() == 2;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            assertEquals(2, metadata.listAll("test-index", List.of(IndexFileStatus.CLEAN)).size());
        }
    }

    private S3CachingDirectory newDirectory(String indexName, MemMockProvider metadata) throws IOException {
        ManifestOptions manifestOptions = new ManifestOptions("test-bucket");
        ManifestManager manager = new ManifestManager(manifestOptions, new NoopRemoteObjectStore(), metadata);
        return new S3CachingDirectory(new S3DirectoryOptions(tempDir, indexName), new S3LockFactory(), manager);
    }

    private S3CachingDirectory newReadOnlyDirectory(String indexName, MemMockProvider metadata) throws IOException {
        ManifestOptions manifestOptions = new ManifestOptions("test-bucket");
        ManifestManager manager = new ManifestManager(manifestOptions, new NoopRemoteObjectStore(), metadata);
        return new S3CachingDirectory(
                new S3DirectoryOptions(tempDir, indexName),
                new S3LockFactory(),
                manager,
                List.of(IndexFileStatus.CLEAN, IndexFileStatus.PINNED),
                false
        );
    }

    private void writeFile(S3CachingDirectory directory, String name, byte[] bytes) throws IOException {
        try (IndexOutput output = directory.createOutput(name, IOContext.DEFAULT)) {
            output.writeBytes(bytes, bytes.length);
        }
    }

    private long crc32(byte[] bytes) {
        CRC32 crc32 = new CRC32();
        crc32.update(bytes);
        return crc32.getValue();
    }

    private void waitForUploads(MemMockProvider metadata, String indexName, int expectedCleanFiles) {
        waitUntil(() -> metadata.listAll(indexName, List.of(IndexFileStatus.CLEAN)).size() == expectedCleanFiles);
        assertEquals(expectedCleanFiles, metadata.listAll(indexName, List.of(IndexFileStatus.CLEAN)).size());
    }

    private void waitUntil(BooleanSupplier condition) {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("condition was not met before timeout");
    }

    private static class NoopRemoteObjectStore implements RemoteObjectStore {
        @Override
        public void put(String key, Path source) throws IOException {
        }

        @Override
        public void get(String key, Path target) throws IOException {
            throw new IOException("remote get is not configured in this test");
        }

        @Override
        public void delete(Collection<String> keys) throws IOException {
        }
    }

    private static class FailingRemoteObjectStore extends NoopRemoteObjectStore {
        private String failedName;

        private FailingRemoteObjectStore(String failedName) {
            this.failedName = failedName;
        }

        private void failName(String failedName) {
            this.failedName = failedName;
        }

        @Override
        public void put(String key, Path source) throws IOException {
            if (failedName != null && key.contains(failedName)) {
                throw new IOException("upload intentionally failed");
            }
        }
    }

    private static class MapRemoteObjectStore extends NoopRemoteObjectStore {
        private final Map<String, byte[]> objects = new HashMap<>();

        private void putObject(String key, byte[] bytes) {
            objects.put(key, bytes);
        }

        @Override
        public void get(String key, Path target) throws IOException {
            byte[] bytes = objects.get(key);
            if (bytes == null) {
                throw new NoSuchFileException(key);
            }
            Files.write(target, bytes);
        }
    }
}
