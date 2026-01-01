package com.github.wxk6b1203.metadata.provider;

import com.github.wxk6b1203.metadata.common.IndexMetadata;

public abstract class MetadataProvider {
    public String name() {
        return this.getClass().getSimpleName();
    }

    public abstract IndexMetadata get(String indexName);

    public abstract int store(IndexMetadata indexMetadata);
}
