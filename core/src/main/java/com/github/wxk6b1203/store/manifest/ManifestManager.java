package com.github.wxk6b1203.store.manifest;

import com.github.wxk6b1203.metadata.common.CommitingIndexFile;
import com.github.wxk6b1203.metadata.common.IndexFile;
import com.github.wxk6b1203.metadata.common.IndexFileMetadata;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.metadata.provider.ManifestMetadataManager;
import com.github.wxk6b1203.store.directory.Hierarchy;
import com.github.wxk6b1203.store.common.PathUtil;
import com.github.wxk6b1203.store.object.RemoteObjectStore;
import com.github.wxk6b1203.store.object.S3RemoteObjectStore;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ManifestManager implements AutoCloseable {
    private final RemoteObjectStore remoteObjectStore;
    private final ManifestMetadataManager metadataManager;
    private final ExecutorService uploadWorkerPool;

    public ManifestManager(ManifestOptions options, S3Client s3Client, ManifestMetadataManager metadataManager) {
        this(options, s3Client == null ? null : new S3RemoteObjectStore(options.bucket(), s3Client), metadataManager);
    }

    public ManifestManager(ManifestOptions options, RemoteObjectStore remoteObjectStore, ManifestMetadataManager metadataManager) {
        this.remoteObjectStore = remoteObjectStore;
        this.metadataManager = metadataManager;
        this.uploadWorkerPool = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("upload-worker-", 0).factory());
    }

    public List<IndexFileMetadata> listAll(String indexName, List<IndexFileStatus> statuses) {
        return metadataManager.listAll(indexName, statuses);
    }

    public IndexFileMetadata fileMetadata(String indexName, String name) throws NoSuchFileException {
        IndexFileMetadata fileMetadata = metadataManager.fileMetadata(indexName, name);
        if (fileMetadata == null) {
            throw new NoSuchFileException(name);
        }
        return fileMetadata;
    }

    public void commit(Collection<CommitingIndexFile> indexFiles) throws IOException {
        List<PendingUpload> pendingUploads = new ArrayList<>();
        for (CommitingIndexFile indexFile : indexFiles) {
            long size = Files.size(indexFile.filePath());
            long modifiedTime = Files.getLastModifiedTime(indexFile.filePath()).toMillis();
            String fileName = indexFile.filePath().getFileName().toString();
            String objectKey = PathUtil.s3ObjectKey(indexFile.indexName(), fileName);
            IndexFile file = new IndexFile(
                    indexFile.indexName(),
                    fileName,
                    Hierarchy.DATA.path,
                    objectKey,
                    size,
                    0,
                    modifiedTime
            );
            IndexFileMetadata metadata = uploadMetadata(file);
            if (metadata != null) {
                pendingUploads.add(new PendingUpload(indexFile.filePath(), metadata));
            }
        }
        if (!pendingUploads.isEmpty()) {
            uploadWorkerPool.execute(() -> uploadCommit(pendingUploads));
        }
    }

    public void download(IndexFileMetadata metadata, Path target) throws IOException {
        ensureRemoteObjectStore();
        remoteObjectStore.get(metadata.getObjectKey(), target);
    }

    public void deleteIndexShards(String indexName, int numberOfShards) throws IOException {
        List<String> shardNames = new ArrayList<>(numberOfShards);
        for (int shard = 0; shard < numberOfShards; shard++) {
            shardNames.add(physicalIndexName(indexName, shard));
        }
        deleteIndices(shardNames);
    }

    public void deleteIndices(Collection<String> indexNames) throws IOException {
        ensureRemoteObjectStore();
        List<IndexFileMetadata> files = indexNames.stream()
                .flatMap(indexName -> metadataManager.listAll(indexName, allFileStatuses()).stream())
                .toList();
        Set<String> objectKeys = new LinkedHashSet<>();
        for (IndexFileMetadata file : files) {
            objectKeys.add(objectKey(file));
        }
        remoteObjectStore.delete(objectKeys);
        for (String indexName : indexNames) {
            metadataManager.deleteAll(indexName);
        }
    }

    private IndexFileMetadata uploadMetadata(IndexFile file) {
        IndexFileMetadata existing = metadataManager.fileMetadata(file.indexName(), file.name());
        if (existing != null) {
            if (existing.getStatus() == IndexFileStatus.CLEAN || existing.getStatus() == IndexFileStatus.PINNED) {
                return null;
            }
            if (sameUpload(existing, file)) {
                return existing;
            }
        }
        int epoch = metadataManager.commitFile(file);
        IndexFileMetadata metadata = metadataManager.fileMetadata(file.indexName(), file.name());
        if (metadata == null || metadata.getEpoch() != epoch) {
            throw new IllegalStateException("failed to store file metadata: " + file.indexName() + "/" + file.name());
        }
        return metadata;
    }

    private boolean sameUpload(IndexFileMetadata metadata, IndexFile file) {
        return metadata.getSize() == file.size()
                && metadata.getChecksum() == file.checksum()
                && metadata.getModifiedTime() == file.modifiedTime()
                && Objects.equals(metadata.getObjectKey(), file.objectKey());
    }

    private void uploadCommit(List<PendingUpload> pendingUploads) {
        boolean dataFilesUploaded = true;
        for (PendingUpload pendingUpload : pendingUploads) {
            if (!isCommittedSegmentFile(pendingUpload.metadata().getName())) {
                dataFilesUploaded &= upload(pendingUpload.source(), pendingUpload.metadata());
            }
        }
        if (!dataFilesUploaded) {
            return;
        }
        for (PendingUpload pendingUpload : pendingUploads) {
            if (isCommittedSegmentFile(pendingUpload.metadata().getName())) {
                upload(pendingUpload.source(), pendingUpload.metadata());
            }
        }
    }

    private boolean upload(Path source, IndexFileMetadata metadata) {
        try {
            ensureRemoteObjectStore();
            metadataManager.updateFileStatus(
                    metadata.getIndexName(),
                    metadata.getName(),
                    metadata.getEpoch(),
                    IndexFileStatus.UPLOADING
            );
            remoteObjectStore.put(metadata.getObjectKey(), source);
            metadataManager.updateFileStatus(
                    metadata.getIndexName(),
                    metadata.getName(),
                    metadata.getEpoch(),
                    IndexFileStatus.CLEAN
            );
            return true;
        } catch (Exception e) {
            log.error("Failed to upload index file {}/{}", metadata.getIndexName(), metadata.getName(), e);
            return false;
        }
    }

    private void ensureRemoteObjectStore() throws IOException {
        if (remoteObjectStore == null) {
            throw new IOException("remote object store is not configured");
        }
    }

    private List<IndexFileStatus> allFileStatuses() {
        return List.of(
                IndexFileStatus.DIRTY,
                IndexFileStatus.UPLOADING,
                IndexFileStatus.CLEAN,
                IndexFileStatus.PINNED
        );
    }

    private String objectKey(IndexFileMetadata metadata) {
        return metadata.getObjectKey() == null || metadata.getObjectKey().isBlank()
                ? PathUtil.s3ObjectKey(metadata.getIndexName(), metadata.getName())
                : metadata.getObjectKey();
    }

    private String physicalIndexName(String indexName, int shard) {
        return indexName + "__shard_" + shard;
    }

    private boolean isCommittedSegmentFile(String name) {
        return name.startsWith("segments_") && !name.startsWith("pending_segments_");
    }

    private record PendingUpload(Path source, IndexFileMetadata metadata) {
    }

    @Override
    public void close() {
        this.uploadWorkerPool.shutdown();
        try {
            this.uploadWorkerPool.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
