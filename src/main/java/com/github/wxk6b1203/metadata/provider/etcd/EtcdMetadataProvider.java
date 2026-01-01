package com.github.wxk6b1203.metadata.provider.etcd;

import com.github.wxk6b1203.metadata.common.IndexMetadata;
import com.github.wxk6b1203.metadata.provider.MetadataProvider;

public class EtcdMetadataProvider extends MetadataProvider {
    @Override
    public IndexMetadata get(String indexName) {
        return null;
    }

    @Override
    public int store(IndexMetadata indexMetadata) {
        return 0;
    }
}
