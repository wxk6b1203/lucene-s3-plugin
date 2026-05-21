package com.github.wxk6b1203.metadata.provider;

import com.github.wxk6b1203.metadata.common.IndexFile;
import com.github.wxk6b1203.metadata.common.IndexCommitSnapshot;
import com.github.wxk6b1203.metadata.common.IndexCommitSnapshotPin;
import com.github.wxk6b1203.metadata.common.IndexFileMetadata;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;

import java.util.List;

public abstract class ManifestMetadataManager {
    public abstract int commitFile(IndexFile file);

    public abstract void updateFileStatus(String indexName, String fileName, long epoch, IndexFileStatus status);

    public abstract List<IndexFileMetadata> listAll(String indexName, List<IndexFileStatus> status);

    public abstract IndexFileMetadata fileMetadata(String indexName, String name);

    public abstract long publishSnapshot(String indexName, String segmentFileName, List<IndexFileMetadata> files);

    public abstract IndexCommitSnapshot latestSnapshot(String indexName);

    public abstract IndexCommitSnapshot snapshot(String indexName, long generation);

    public abstract List<IndexCommitSnapshot> listSnapshots(String indexName);

    public abstract void deleteSnapshot(String indexName, long generation);

    public abstract void pinSnapshot(String indexName, long generation, String pinId, long expiresAtMillis);

    public abstract void releaseSnapshotPin(String indexName, String pinId);

    public abstract List<IndexCommitSnapshotPin> snapshotPins(String indexName);

    public abstract void deleteExpiredSnapshotPins(long nowMillis);

    public abstract void deleteByStatus(String indexName, List<IndexFileStatus> statuses);

    public abstract void deleteAll(String indexName);
}
