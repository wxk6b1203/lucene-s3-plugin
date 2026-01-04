package com.github.wxk6b1203.metadata.provider;

import com.github.wxk6b1203.metadata.common.IndexMetadata;

public abstract class MetadataProvider {
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
}
