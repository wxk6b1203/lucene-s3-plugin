package com.github.wxk6b1203.cluster;

import java.io.IOException;

public class NoopClusterCoordinator implements ClusterCoordinator {
    @Override
    public void start() throws IOException {
    }

    @Override
    public boolean isMaster() {
        return true;
    }

    @Override
    public void close() {
    }
}
