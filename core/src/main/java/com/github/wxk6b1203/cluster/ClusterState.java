package com.github.wxk6b1203.cluster;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public record ClusterState(
        String clusterName,
        long version,
        String masterNodeId,
        Map<String, ClusterNode> nodes,
        Map<String, IndexSettings> indices,
        List<ShardRouting> routingTable,
        Map<String, IndexLifecyclePolicy> lifecyclePolicies,
        Instant updatedAt
) {
}
