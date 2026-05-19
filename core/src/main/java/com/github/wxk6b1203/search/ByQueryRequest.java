package com.github.wxk6b1203.search;

import com.github.wxk6b1203.cluster.FieldMapping;

import java.util.Map;

public record ByQueryRequest(
        String indexName,
        Map<String, Object> query,
        Map<String, Object> document,
        String routing,
        boolean conflictsProceed,
        Map<String, FieldMapping> mappings,
        Long ownerTerm,
        Long allocationEpoch
) {
    public ByQueryRequest(
            String indexName,
            Map<String, Object> query,
            Map<String, Object> document,
            String routing,
            boolean conflictsProceed,
            Map<String, FieldMapping> mappings
    ) {
        this(indexName, query, document, routing, conflictsProceed, mappings, null, null);
    }

    public ByQueryRequest(
            String indexName,
            Map<String, Object> query,
            Map<String, Object> document,
            String routing,
            boolean conflictsProceed
    ) {
        this(indexName, query, document, routing, conflictsProceed, Map.of());
    }

    public ByQueryRequest {
        mappings = mappings == null ? Map.of() : Map.copyOf(mappings);
    }
}
