package com.github.wxk6b1203.search;

import com.github.wxk6b1203.cluster.ClusterNode;
import com.github.wxk6b1203.cluster.ClusterState;
import com.github.wxk6b1203.cluster.ShardId;
import com.github.wxk6b1203.cluster.ShardRouter;
import com.github.wxk6b1203.cluster.ShardRouting;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class SearchPlanner {
    private final ShardRouter shardRouter;

    public SearchPlanner(ShardRouter shardRouter) {
        this.shardRouter = shardRouter;
    }

    public SearchPlan plan(SearchRequest request, ClusterState state) {
        if (!state.indices().containsKey(request.indexName())) {
            throw new IllegalArgumentException("index not found: " + request.indexName());
        }
        return new SearchPlan(
                request.indexName(),
                request.routing(),
                state.version(),
                searchTargets(request, state).stream()
                        .toList()
        );
    }

    private List<SearchShardTarget> searchTargets(SearchRequest request, ClusterState state) {
        List<ClusterNode> dataNodes = liveDataNodes(state);
        if (dataNodes.isEmpty()) {
            throw new IllegalStateException("no live data node is available for search");
        }
        Map<String, Integer> load = new HashMap<>();
        dataNodes.forEach(node -> load.put(node.id(), 0));
        return searchShards(request, state).stream()
                .map(routing -> target(routing, request.readPreference(), state, dataNodes, load))
                .toList();
    }

    private SearchShardTarget target(
            ShardRouting routing,
            String readPreference,
            ClusterState state,
            List<ClusterNode> dataNodes,
            Map<String, Integer> load
    ) {
        ClusterNode node = "owner".equalsIgnoreCase(readPreference)
                ? ownerOrThrow(routing, state)
                : ownerOrFallback(routing, state, dataNodes, load);
        load.merge(node.id(), 1, Integer::sum);
        return new SearchShardTarget(
                routing.shardId(),
                node.id(),
                node.host(),
                node.httpPort(),
                routing.ownerTerm(),
                routing.allocationEpoch()
        );
    }

    private ClusterNode ownerOrFallback(
            ShardRouting routing,
            ClusterState state,
            List<ClusterNode> dataNodes,
            Map<String, Integer> load
    ) {
        ClusterNode owner = state.nodes().get(routing.nodeId());
        if (owner != null && owner.roles().contains(com.github.wxk6b1203.cluster.NodeRole.DATA)) {
            return owner;
        }
        return leastLoaded(dataNodes, load);
    }

    private ClusterNode ownerOrThrow(ShardRouting routing, ClusterState state) {
        ClusterNode owner = state.nodes().get(routing.nodeId());
        if (owner == null || !owner.roles().contains(com.github.wxk6b1203.cluster.NodeRole.DATA)) {
            throw new IllegalStateException("shard owner node is not live: " + routing.shardId().routeKey());
        }
        return owner;
    }

    private ClusterNode leastLoaded(List<ClusterNode> dataNodes, Map<String, Integer> load) {
        return dataNodes.stream()
                .min(java.util.Comparator
                        .comparing((ClusterNode node) -> load.getOrDefault(node.id(), 0))
                        .thenComparing(ClusterNode::id))
                .orElseThrow();
    }

    private List<ClusterNode> liveDataNodes(ClusterState state) {
        return state.nodes().values().stream()
                .filter(node -> node.roles().contains(com.github.wxk6b1203.cluster.NodeRole.DATA))
                .sorted(java.util.Comparator.comparing(ClusterNode::id))
                .toList();
    }

    private List<ShardRouting> searchShards(SearchRequest request, ClusterState state) {
        if (request.routing() == null || request.routing().isBlank()) {
            return shardRouter.searchShards(request.indexName(), state);
        }
        ShardId shardId = shardRouter.route(request.indexName(), request.routing(), state);
        return List.of(shardRouter.searchShard(shardId, state));
    }
}
