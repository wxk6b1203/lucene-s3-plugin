package com.github.wxk6b1203.index;

import com.github.wxk6b1203.cluster.FieldMapping;
import com.github.wxk6b1203.cluster.ShardId;

import java.util.Map;

public record IndexDocumentRequest(
        String indexName,
        ShardId shardId,
        String id,
        Map<String, Object> source,
        Map<String, FieldMapping> mappings
) {
    public IndexDocumentRequest(String indexName, ShardId shardId, String id, Map<String, Object> source) {
        this(indexName, shardId, id, source, Map.of());
    }

    public IndexDocumentRequest {
        mappings = mappings == null ? Map.of() : Map.copyOf(mappings);
    }
}
