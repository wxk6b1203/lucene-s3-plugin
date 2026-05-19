package com.github.wxk6b1203.cluster;

public class WriteRouter {
    private final ShardRouter shardRouter;

    public WriteRouter(ShardRouter shardRouter) {
        this.shardRouter = shardRouter;
    }

    public WriteRoute route(String indexName, String routingKey, ClusterState state) {
        ShardId shardId = shardRouter.route(indexName, routingKey, state);
        ShardRouting owner = state.routingTable().stream()
                .filter(routing -> routing.shardId().equals(shardId))
                .filter(routing -> routing.state() == ShardState.STARTED)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("shard owner is not available: " + shardId.routeKey()));
        ClusterNode node = state.nodes().get(owner.nodeId());
        if (node == null) {
            throw new IllegalStateException("shard owner node is not live: " + owner.nodeId());
        }
        return new WriteRoute(shardId, node.id(), node.host(), node.httpPort(), owner.ownerTerm(), owner.allocationEpoch());
    }
}
