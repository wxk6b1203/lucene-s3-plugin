package com.github.wxk6b1203.http;

import com.github.wxk6b1203.search.SearchRequest;
import com.github.wxk6b1203.search.SearchResponse;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SearchResponseMergerTest {
    @Test
    @SuppressWarnings("unchecked")
    void termsMinDocCountIsAppliedAfterShardCountsAreMerged() {
        SearchResponse shard0 = response(Map.of(
                "by_category", Map.of(
                        "type", "terms",
                        "field", "category",
                        "size", 10,
                        "min_doc_count", 2,
                        "order", Map.of("_count", "desc"),
                        "buckets", List.of(Map.of("key", "common", "doc_count", 1L))
                )
        ));
        SearchResponse shard1 = response(Map.of(
                "by_category", Map.of(
                        "type", "terms",
                        "field", "category",
                        "size", 10,
                        "min_doc_count", 2,
                        "order", Map.of("_count", "desc"),
                        "buckets", List.of(Map.of("key", "common", "doc_count", 1L))
                )
        ));

        SearchResponse merged = SearchResponseMerger.mergeSearchResponses(
                List.of(shard0, shard1),
                new SearchRequest("books", Map.of("match_all", Map.of()), List.of(), null, null, 0, 10, Map.of()),
                System.nanoTime()
        );

        Map<String, Object> byCategory = (Map<String, Object>) merged.aggregations().get("by_category");
        List<Map<String, Object>> buckets = (List<Map<String, Object>>) byCategory.get("buckets");
        assertEquals(1, buckets.size());
        assertEquals("common", buckets.getFirst().get("key"));
        assertEquals(2L, buckets.getFirst().get("doc_count"));
    }

    private SearchResponse response(Map<String, Object> aggregations) {
        return new SearchResponse(0, 1, 1, 0, List.of(), aggregations, List.of());
    }
}
