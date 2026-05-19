package com.github.wxk6b1203.metadata.provider;

import com.github.wxk6b1203.metadata.common.IndexFile;
import com.github.wxk6b1203.metadata.common.IndexFileMetadata;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.metadata.common.IndexMetadata;

import java.util.List;
import java.util.Objects;

public abstract class ManifestMetadataManager {
    public interface Key {
        String INDEX = "index";
        String TYPE = "type";
        String INDEX_NAME = "index_name";
    }

    // Retrieve the index metadata by index name, return null if not found
    // Any implementation should ensure thread-safety
    public abstract IndexMetadata get(String indexName);

    // Store the index metadata, return the mod revision if success, otherwise return -1
    // Any implementation should ensure idempotency
    public abstract long store(IndexMetadata indexMetadata);

    public abstract int commitFile(IndexFile file);

    public abstract void updateFileStatus(String indexName, String fileName, long epoch, IndexFileStatus status);

    public abstract List<IndexFileMetadata> listAll(List<IndexFileStatus> status);

    public List<IndexFileMetadata> listAll(String indexName, List<IndexFileStatus> status) {
        return listAll(status).stream()
                .filter(file -> Objects.equals(indexName, file.getIndexName()))
                .toList();
    }

    public abstract IndexFileMetadata fileMetadata(String indexName, String name);

    public abstract void deleteAll(String indexName);
}
