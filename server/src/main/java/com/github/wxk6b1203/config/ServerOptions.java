package com.github.wxk6b1203.config;

import com.github.wxk6b1203.cluster.NodeRole;

import java.util.Set;

public record ServerOptions(
        int httpPort,
        String clusterName,
        String nodeId,
        String nodeName,
        String host,
        Set<NodeRole> roles,
        String etcdEndpoints,
        String etcdNamespace,
        String dataPath,
        String s3Bucket,
        String s3Region,
        String s3Protocol,
        String s3Endpoint,
        boolean s3ChunkedEncoding,
        String s3AccessKey,
        String s3SecretKey,
        int snapshotRetainLatest,
        int etcdTimeoutSeconds
) {
    public ServerOptions {
        etcdTimeoutSeconds = Math.max(1, etcdTimeoutSeconds);
    }

    public ServerOptions(
            int httpPort,
            String clusterName,
            String nodeId,
            String nodeName,
            String host,
            Set<NodeRole> roles,
            String etcdEndpoints,
            String etcdNamespace,
            String dataPath,
            String s3Bucket,
            String s3Region,
            String s3Protocol,
            String s3Endpoint,
            boolean s3ChunkedEncoding,
            String s3AccessKey,
            String s3SecretKey,
            int snapshotRetainLatest
    ) {
        this(
                httpPort,
                clusterName,
                nodeId,
                nodeName,
                host,
                roles,
                etcdEndpoints,
                etcdNamespace,
                dataPath,
                s3Bucket,
                s3Region,
                s3Protocol,
                s3Endpoint,
                s3ChunkedEncoding,
                s3AccessKey,
                s3SecretKey,
                snapshotRetainLatest,
                10
        );
    }

    public boolean etcdEnabled() {
        return etcdEndpoints != null && !etcdEndpoints.isBlank();
    }

    public boolean s3Enabled() {
        return s3Bucket != null && !s3Bucket.isBlank();
    }
}
