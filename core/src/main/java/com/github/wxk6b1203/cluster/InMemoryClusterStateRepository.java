package com.github.wxk6b1203.cluster;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class InMemoryClusterStateRepository implements ClusterStateRepository {
    private final AtomicReference<ClusterState> state;

    public InMemoryClusterStateRepository(String clusterName, ClusterNode localNode) {
        this.state = new AtomicReference<>(new ClusterState(
                clusterName,
                1,
                localNode.id(),
                Map.of(localNode.id(), localNode),
                Map.of(),
                List.of(),
                Map.of(),
                Instant.now()
        ));
    }

    @Override
    public ClusterState current() {
        return state.get();
    }

    @Override
    public ClusterState update(ClusterStateUpdate update) throws IOException {
        while (true) {
            ClusterState current = state.get();
            ClusterState next = update.apply(current);
            if (sameContent(current, next)) {
                return current;
            }
            if (state.compareAndSet(current, bumpVersion(next, current.version() + 1))) {
                return state.get();
            }
        }
    }

    private boolean sameContent(ClusterState left, ClusterState right) {
        return Objects.equals(left.clusterName(), right.clusterName())
                && Objects.equals(left.masterNodeId(), right.masterNodeId())
                && Objects.equals(left.nodes(), right.nodes())
                && Objects.equals(left.indices(), right.indices())
                && Objects.equals(left.routingTable(), right.routingTable())
                && Objects.equals(left.lifecyclePolicies(), right.lifecyclePolicies());
    }

    private ClusterState bumpVersion(ClusterState state, long version) {
        return new ClusterState(
                state.clusterName(),
                version,
                state.masterNodeId(),
                state.nodes(),
                state.indices(),
                state.routingTable(),
                state.lifecyclePolicies(),
                Instant.now()
        );
    }
}
