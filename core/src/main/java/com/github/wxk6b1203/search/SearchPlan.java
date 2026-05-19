package com.github.wxk6b1203.search;

import java.util.List;

public record SearchPlan(
        String indexName,
        String routing,
        long clusterStateVersion,
        List<SearchShardTarget> targets
) {
}
