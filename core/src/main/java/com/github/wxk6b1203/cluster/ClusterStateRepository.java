package com.github.wxk6b1203.cluster;

import java.io.IOException;

public interface ClusterStateRepository {
    ClusterState current() throws IOException;

    ClusterState update(ClusterStateUpdate update) throws IOException;

    default ClusterState updateMaster(String nodeId) throws IOException {
        return update(state -> new ClusterState(
                state.clusterName(),
                state.version(),
                nodeId,
                state.nodes(),
                state.indices(),
                state.routingTable(),
                state.lifecyclePolicies(),
                state.updatedAt()
        ));
    }

    @FunctionalInterface
    interface ClusterStateUpdate {
        ClusterState apply(ClusterState state);
    }
}
