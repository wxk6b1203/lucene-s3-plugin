package com.github.wxk6b1203.cluster;

import com.github.wxk6b1203.errors.NotMasterException;

import java.io.IOException;

public class MasterOnlyClusterStateRepository implements ClusterStateRepository {
    private final ClusterStateRepository delegate;
    private final ClusterCoordinator coordinator;

    public MasterOnlyClusterStateRepository(ClusterStateRepository delegate, ClusterCoordinator coordinator) {
        this.delegate = delegate;
        this.coordinator = coordinator;
    }

    @Override
    public ClusterState current() throws IOException {
        return delegate.current();
    }

    @Override
    public ClusterState update(ClusterStateUpdate update) throws IOException {
        if (!coordinator.isMaster()) {
            throw new NotMasterException("cluster-state mutation must be handled by the current master node");
        }
        return delegate.update(update);
    }
}
