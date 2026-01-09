package com.github.wxk6b1203.metadata.provider.mem;

import com.github.wxk6b1203.metadata.common.IndexMetadata;
import com.github.wxk6b1203.metadata.provider.MetadataProvider;

import java.util.concurrent.ConcurrentHashMap;

public class MemProvider extends MetadataProvider {
    ConcurrentHashMap<String, IndexMetadata> indexes = new ConcurrentHashMap<>();

    @Override
    public IndexMetadata get(String indexName) {
        return indexes.get(indexName);
    }

    @Override
    public synchronized long store(IndexMetadata indexMetadata) {
        IndexMetadata origin = indexes.get(indexMetadata.getName());
        if (origin != null) {
            indexMetadata.setEpoch(origin.getEpoch() + 1);
            indexes.put(indexMetadata.getName(), indexMetadata);
        } else {
            indexMetadata.setEpoch(1L);
            indexes.put(indexMetadata.getName(), indexMetadata);
        }
        return indexMetadata.getEpoch();
    }
}
