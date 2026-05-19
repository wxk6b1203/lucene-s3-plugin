package com.github.wxk6b1203.store;

import com.github.wxk6b1203.metadata.common.CommitingIndexFile;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.metadata.provider.mem.MemMockProvider;
import com.github.wxk6b1203.store.manifest.ManifestManager;
import com.github.wxk6b1203.store.manifest.ManifestOptions;
import com.github.wxk6b1203.store.object.RemoteObjectStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ManifestManagerTest {
    @TempDir
    Path tempDir;

    @Test
    public void testMissingRemoteObjectStoreDoesNotMarkClean() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ManifestOptions options = new ManifestOptions("bucket");
        ManifestManager manager = new ManifestManager(options, (software.amazon.awssdk.services.s3.S3Client) null, metadata);
        Path file = tempDir.resolve("segments_1");
        Files.write(file, new byte[]{1});

        manager.commit(List.of(new CommitingIndexFile("books", file)));
        Thread.sleep(200);
        manager.close();

        assertEquals(1, metadata.listAll(List.of(IndexFileStatus.DIRTY)).size());
        assertEquals(0, metadata.listAll(List.of(IndexFileStatus.CLEAN)).size());
    }

    @Test
    public void testSegmentsFileIsUploadedAfterDataFiles() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        RecordingRemoteObjectStore remote = new RecordingRemoteObjectStore();
        ManifestOptions options = new ManifestOptions("bucket");
        ManifestManager manager = new ManifestManager(options, remote, metadata);
        Path data = tempDir.resolve("_0.si");
        Path segments = tempDir.resolve("segments_1");
        Files.write(data, new byte[]{1});
        Files.write(segments, new byte[]{2});

        manager.commit(List.of(
                new CommitingIndexFile("books", data),
                new CommitingIndexFile("books", segments)
        ));
        waitUntil(() -> metadata.fileMetadata("books", "segments_1").getStatus() == IndexFileStatus.CLEAN);
        manager.close();

        assertEquals(List.of("books/_data/_0.si", "books/_data/segments_1"), remote.puts);
    }

    @Test
    public void testSegmentsFileStaysDirtyWhenDataFileUploadFails() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        RecordingRemoteObjectStore remote = new RecordingRemoteObjectStore();
        remote.failName("_0.si");
        ManifestOptions options = new ManifestOptions("bucket");
        ManifestManager manager = new ManifestManager(options, remote, metadata);
        Path data = tempDir.resolve("_0.si");
        Path segments = tempDir.resolve("segments_1");
        Files.write(data, new byte[]{1});
        Files.write(segments, new byte[]{2});

        manager.commit(List.of(
                new CommitingIndexFile("books", data),
                new CommitingIndexFile("books", segments)
        ));
        waitUntil(() -> metadata.fileMetadata("books", "_0.si").getStatus() == IndexFileStatus.UPLOADING);
        manager.close();

        assertEquals(IndexFileStatus.UPLOADING, metadata.fileMetadata("books", "_0.si").getStatus());
        assertEquals(IndexFileStatus.DIRTY, metadata.fileMetadata("books", "segments_1").getStatus());
    }

    @Test
    public void testDirtyFilesCanBeRetriedWithoutNewMetadataEpoch() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        RecordingRemoteObjectStore remote = new RecordingRemoteObjectStore();
        ManifestOptions options = new ManifestOptions("bucket");
        ManifestManager manager = new ManifestManager(options, remote, metadata);
        Path data = tempDir.resolve("_0.si");
        Path segments = tempDir.resolve("segments_1");
        Files.write(data, new byte[]{1});
        Files.write(segments, new byte[]{2});

        remote.failName("_0.si");
        manager.commit(List.of(
                new CommitingIndexFile("books", data),
                new CommitingIndexFile("books", segments)
        ));
        waitUntil(() -> metadata.fileMetadata("books", "_0.si").getStatus() == IndexFileStatus.UPLOADING);

        remote.failName(null);
        manager.commit(List.of(
                new CommitingIndexFile("books", data),
                new CommitingIndexFile("books", segments)
        ));
        waitUntil(() -> metadata.fileMetadata("books", "segments_1").getStatus() == IndexFileStatus.CLEAN);
        manager.close();

        assertEquals(1, metadata.fileMetadata("books", "_0.si").getEpoch());
        assertEquals(1, metadata.fileMetadata("books", "segments_1").getEpoch());
        assertEquals(List.of(
                "books/_data/_0.si",
                "books/_data/_0.si",
                "books/_data/segments_1"
        ), remote.puts);
    }

    @Test
    public void testDirtyMetadataIsReplacedWhenFileNameIsReusedWithDifferentContent() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        RecordingRemoteObjectStore remote = new RecordingRemoteObjectStore();
        ManifestOptions options = new ManifestOptions("bucket");
        ManifestManager manager = new ManifestManager(options, remote, metadata);
        Path data = tempDir.resolve("_0.si");
        Files.write(data, new byte[]{1});

        remote.failName("_0.si");
        manager.commit(List.of(new CommitingIndexFile("books", data)));
        waitUntil(() -> metadata.fileMetadata("books", "_0.si").getStatus() == IndexFileStatus.UPLOADING);

        remote.failName(null);
        Files.write(data, new byte[]{1, 2});
        manager.commit(List.of(new CommitingIndexFile("books", data)));
        waitUntil(() -> metadata.fileMetadata("books", "_0.si").getStatus() == IndexFileStatus.CLEAN);
        manager.close();

        assertEquals(2, metadata.fileMetadata("books", "_0.si").getEpoch());
        assertEquals(2, metadata.fileMetadata("books", "_0.si").getSize());
    }

    @Test
    public void testDeleteIndexShardsRemovesRemoteObjectsAndMetadata() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        RecordingRemoteObjectStore remote = new RecordingRemoteObjectStore();
        ManifestManager manager = new ManifestManager(new ManifestOptions("bucket"), remote, metadata);
        Path data = tempDir.resolve("_0.si");
        Path segments = tempDir.resolve("segments_1");
        Files.write(data, new byte[]{1});
        Files.write(segments, new byte[]{2});

        manager.commit(List.of(
                new CommitingIndexFile("books__shard_0", data),
                new CommitingIndexFile("books__shard_0", segments)
        ));
        waitUntil(() -> metadata.fileMetadata("books__shard_0", "segments_1").getStatus() == IndexFileStatus.CLEAN);

        manager.deleteIndexShards("books", 1);
        manager.close();

        assertEquals(Set.of(
                "books__shard_0/_data/_0.si",
                "books__shard_0/_data/segments_1"
        ), Set.copyOf(remote.deletes));
        assertTrue(metadata.listAll("books__shard_0", List.of(
                IndexFileStatus.DIRTY,
                IndexFileStatus.UPLOADING,
                IndexFileStatus.CLEAN,
                IndexFileStatus.PINNED
        )).isEmpty());
    }

    private void waitUntil(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("condition was not met before timeout");
    }

    private static class RecordingRemoteObjectStore implements RemoteObjectStore {
        private final List<String> puts = new ArrayList<>();
        private final List<String> deletes = new ArrayList<>();
        private String failedName;

        private void failName(String failedName) {
            this.failedName = failedName;
        }

        @Override
        public void put(String key, Path source) throws IOException {
            puts.add(key);
            if (failedName != null && key.endsWith(failedName)) {
                throw new IOException("upload intentionally failed");
            }
        }

        @Override
        public void get(String key, Path target) throws IOException {
            throw new IOException("remote get is not configured in this test");
        }

        @Override
        public void delete(Collection<String> keys) {
            deletes.addAll(keys);
        }
    }
}
