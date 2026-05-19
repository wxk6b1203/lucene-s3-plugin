package com.github.wxk6b1203.cluster;

import java.time.Instant;
import java.util.Set;

public record ClusterNode(
        String id,
        String name,
        String host,
        int httpPort,
        Set<NodeRole> roles,
        Instant lastSeenAt
) {
}
