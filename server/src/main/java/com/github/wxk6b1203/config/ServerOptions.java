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
        int snapshotRetainLatest
) {
    public boolean etcdEnabled() {
        return etcdEndpoints != null && !etcdEndpoints.isBlank();
    }

    public boolean s3Enabled() {
        return s3Bucket != null && !s3Bucket.isBlank();
    }
}
