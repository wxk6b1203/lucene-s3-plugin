package com.github.wxk6b1203.search;

import java.util.List;
import java.util.Map;

public record SearchResponse(
        long tookMillis,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<SearchHit> hits,
        Map<String, Object> aggregations,
        List<String> shardFailures
) {
}
