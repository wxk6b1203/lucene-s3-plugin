package com.github.wxk6b1203.cluster;

import java.util.List;

public interface ShardRouter {
    ShardId route(String indexName, String routingKey, ClusterState state);

    List<ShardRouting> searchShards(String indexName, ClusterState state);

    ShardRouting searchShard(ShardId shardId, ClusterState state);
}
