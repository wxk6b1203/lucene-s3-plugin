package com.github.wxk6b1203.cluster;

import java.io.IOException;

public interface ClusterCoordinator extends AutoCloseable {
    void start() throws IOException;

    boolean isMaster();

    @Override
    void close();
}
