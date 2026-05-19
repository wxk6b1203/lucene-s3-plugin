package com.github.wxk6b1203.cluster;

public record WriteRoute(
        ShardId shardId,
        String nodeId,
        String host,
        int httpPort,
        long ownerTerm,
        long allocationEpoch
) {
}
