package com.github.wxk6b1203.cluster;

public interface ShardAllocator {
    ClusterState rebalance(ClusterState state);
}
