package com.github.wxk6b1203.cluster;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class BalancedShardAllocator implements ShardAllocator {
    @Override
    public ClusterState rebalance(ClusterState state) {
        List<String> dataNodes = state.nodes().values().stream()
                .filter(node -> node.roles().contains(NodeRole.DATA))
                .map(ClusterNode::id)
                .sorted()
                .toList();
        if (dataNodes.isEmpty()) {
            return withRouting(state, state.routingTable().stream()
                    .map(routing -> assign(routing, ShardState.UNASSIGNED, null, routing.ownerTerm()))
                    .toList());
        }

        Map<String, Integer> load = emptyLoad(dataNodes);
        List<ShardRouting> next = new ArrayList<>(state.routingTable().size());

        for (ShardRouting routing : sortedByShard(state.routingTable())) {
            String node = liveNodeOrLeastLoaded(routing.nodeId(), dataNodes, load);
            load.merge(node, 1, Integer::sum);
            long ownerTerm = routing.nodeId() != null && routing.nodeId().equals(node)
                    ? routing.ownerTerm()
                    : routing.ownerTerm() + 1;
            next.add(assign(routing, ShardState.STARTED, node, ownerTerm));
        }
        return withRouting(state, next);
    }

    private Map<String, Integer> emptyLoad(List<String> dataNodes) {
        Map<String, Integer> load = new HashMap<>();
        for (String node : dataNodes) {
            load.put(node, 0);
        }
        return load;
    }

    private List<ShardRouting> sortedByShard(List<ShardRouting> routingTable) {
        return routingTable.stream()
                .sorted(Comparator
                        .comparing((ShardRouting routing) -> routing.shardId().indexName())
                        .thenComparing(routing -> routing.shardId().shardNumber()))
                .toList();
    }

    private String liveNodeOrLeastLoaded(String current, List<String> dataNodes, Map<String, Integer> load) {
        if (current != null && dataNodes.contains(current)) {
            return current;
        }
        return leastLoaded(dataNodes, load);
    }

    private String leastLoaded(List<String> candidates, Map<String, Integer> load) {
        return candidates.stream()
                .min(Comparator.comparing((String node) -> load.getOrDefault(node, 0)).thenComparing(node -> node))
                .orElseThrow();
    }

    private ShardRouting assign(ShardRouting routing, ShardState state, String nodeId, long ownerTerm) {
        long allocationEpoch = routing.state() == state
                && Objects.equals(routing.nodeId(), nodeId)
                && routing.ownerTerm() == ownerTerm
                ? routing.allocationEpoch()
                : routing.allocationEpoch() + 1;
        return new ShardRouting(
                routing.shardId(),
                state,
                nodeId,
                ownerTerm,
                allocationEpoch
        );
    }

    private ClusterState withRouting(ClusterState state, List<ShardRouting> routingTable) {
        return new ClusterState(
                state.clusterName(),
                state.version(),
                state.masterNodeId(),
                state.nodes(),
                state.indices(),
                routingTable,
                state.lifecyclePolicies(),
                state.updatedAt()
        );
    }
}
