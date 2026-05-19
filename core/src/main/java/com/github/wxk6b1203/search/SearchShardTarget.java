package com.github.wxk6b1203.search;

import com.github.wxk6b1203.cluster.ShardId;

public record SearchShardTarget(
        ShardId shardId,
        String nodeId,
        String host,
        int httpPort,
        long ownerTerm,
        long allocationEpoch,
        boolean remoteSnapshot
) {
    public SearchShardTarget(
            ShardId shardId,
            String nodeId,
            String host,
            int httpPort,
            long ownerTerm,
            long allocationEpoch
    ) {
        this(shardId, nodeId, host, httpPort, ownerTerm, allocationEpoch, false);
    }
}
