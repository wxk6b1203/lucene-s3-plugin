package com.github.wxk6b1203.cluster;

public record ShardRouting(
        ShardId shardId,
        ShardState state,
        String nodeId,
        long ownerTerm,
        long allocationEpoch
) {
    public boolean searchable() {
        return state == ShardState.STARTED || state == ShardState.RELOCATING;
    }
}
