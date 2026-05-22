package com.github.wxk6b1203.search;

import com.github.wxk6b1203.cluster.FieldMapping;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public record SearchRequest(
        String indexName,
        Map<String, Object> query,
        List<Map<String, Object>> aggregations,
        VectorQuery vector,
        String routing,
        int from,
        int size,
        List<Map<String, Object>> sort,
        List<Object> searchAfter,
        String pitId,
        Map<String, FieldMapping> mappings,
        String readPreference,
        Long remoteSnapshotGeneration
) {
    public SearchRequest(
            String indexName,
            Map<String, Object> query,
            List<Map<String, Object>> aggregations,
            VectorQuery vector,
            String routing,
            int from,
            int size,
            List<Map<String, Object>> sort,
            List<Object> searchAfter,
            String pitId,
            Map<String, FieldMapping> mappings
    ) {
        this(indexName, query, aggregations, vector, routing, from, size, sort, searchAfter, pitId, mappings, "weak", null);
    }

    public SearchRequest(
            String indexName,
            Map<String, Object> query,
            List<Map<String, Object>> aggregations,
            VectorQuery vector,
            String routing,
            int from,
            int size,
            List<Map<String, Object>> sort,
            List<Object> searchAfter,
            Map<String, FieldMapping> mappings
    ) {
        this(indexName, query, aggregations, vector, routing, from, size, sort, searchAfter, null, mappings, "weak", null);
    }

    public SearchRequest(
            String indexName,
            Map<String, Object> query,
            List<Map<String, Object>> aggregations,
            VectorQuery vector,
            String routing,
            int from,
            int size,
            List<Map<String, Object>> sort,
            Map<String, FieldMapping> mappings
    ) {
        this(indexName, query, aggregations, vector, routing, from, size, sort, List.of(), null, mappings, "weak", null);
    }

    public SearchRequest(
            String indexName,
            Map<String, Object> query,
            List<Map<String, Object>> aggregations,
            VectorQuery vector,
            String routing,
            int from,
            int size,
            Map<String, FieldMapping> mappings
    ) {
        this(indexName, query, aggregations, vector, routing, from, size, List.of(), List.of(), null, mappings, "weak", null);
    }

    public SearchRequest(
            String indexName,
            Map<String, Object> query,
            List<Map<String, Object>> aggregations,
            VectorQuery vector,
            String routing,
            int from,
            int size
    ) {
        this(indexName, query, aggregations, vector, routing, from, size, List.of(), List.of(), null, Map.of(), "weak", null);
    }

    public SearchRequest(
            String indexName,
            Map<String, Object> query,
            List<Map<String, Object>> aggregations,
            VectorQuery vector,
            String routing,
            int from,
            int size,
            List<Map<String, Object>> sort,
            List<Object> searchAfter,
            String pitId,
            Map<String, FieldMapping> mappings,
            String readPreference
    ) {
        this(
                indexName,
                query,
                aggregations,
                vector,
                routing,
                from,
                size,
                sort,
                searchAfter,
                pitId,
                mappings,
                readPreference,
                null
        );
    }

    public SearchRequest {
        sort = sort == null ? List.of() : List.copyOf(sort);
        searchAfter = searchAfter == null ? List.of() : Collections.unmodifiableList(new ArrayList<>(searchAfter));
        mappings = mappings == null ? Map.of() : Map.copyOf(mappings);
        readPreference = readPreference == null || readPreference.isBlank() ? "weak" : readPreference;
    }
}
