package com.github.wxk6b1203.metadata.provider;

import com.github.wxk6b1203.metadata.common.IndexFile;
import com.github.wxk6b1203.metadata.common.IndexFileMetadata;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.metadata.common.IndexMetadata;

import java.io.IOException;
import java.util.List;

public abstract class ManifestMetadataManager {
    public abstract void prepareDelete(String indexName, String name) throws IOException;

    public abstract void cleaningUp(String indexName, String name);

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

    public abstract IndexFileMetadata get(String indexName, String fileName);

    public abstract List<IndexFileMetadata> listAllClean();

    public abstract List<IndexFileMetadata> listAll(List<IndexFileStatus> status);

    public abstract IndexFileMetadata fileMetadata(String indexName, String name);

}
