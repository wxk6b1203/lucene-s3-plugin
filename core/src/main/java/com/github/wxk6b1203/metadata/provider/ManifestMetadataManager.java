package com.github.wxk6b1203.metadata.provider;

import com.github.wxk6b1203.metadata.common.IndexFile;
import com.github.wxk6b1203.metadata.common.IndexFileMetadata;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;

import java.util.List;

public abstract class ManifestMetadataManager {
    public abstract int commitFile(IndexFile file);

    public abstract void updateFileStatus(String indexName, String fileName, long epoch, IndexFileStatus status);

    public abstract List<IndexFileMetadata> listAll(String indexName, List<IndexFileStatus> status);

    public abstract IndexFileMetadata fileMetadata(String indexName, String name);

    public abstract void deleteAll(String indexName);
}
