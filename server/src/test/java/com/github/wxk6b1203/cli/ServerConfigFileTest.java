package com.github.wxk6b1203.cli;

import com.github.wxk6b1203.cluster.NodeRole;
import com.github.wxk6b1203.config.ServerOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ServerConfigFileTest {
    @TempDir
    Path tempDir;

    @Test
    void yamlConfigurationProvidesServerOptions() throws Exception {
        Path config = tempDir.resolve("server.yaml");
        Files.writeString(config, """
                server:
                  http:
                    port: 9300
                  cluster:
                    name: yaml-cluster
                  node:
                    id: yaml-node
                    name: yaml-node-name
                    host: 0.0.0.0
                  roles:
                    - MASTER
                    - DATA
                  etcd:
                    endpoints: http://127.0.0.1:2379
                    namespace: yaml/ns
                    timeoutSeconds: 7
                  data:
                    path: data/yaml-node
                  cache:
                    maxBytes: 1048576
                    cleanupIntervalSeconds: 30
                  metrics:
                    port: 9500
                  write:
                    maxRequests: 8
                  bulk:
                    maxItems: 500
                    maxBytes: 10485760
                  index:
                    commit:
                      everyRequest: false
                      intervalMillis: 2500
                      afterDocs: 128
                    refresh:
                      policy: interval
                      intervalMillis: 1500
                  analyzer:
                    pluginPath: plugins/analyzers
                  s3:
                    bucket: yaml-bucket
                    region: us-east-1
                    protocol: http
                    endpoint: http://127.0.0.1:9000
                    chunkedEncoding: true
                    contentMd5: true
                    accessKey: minio
                    secretKey: password
                  snapshot:
                    retainLatest: 3
                """);

        Server command = new Server();
        new CommandLine(command).parseArgs("--conf", config.toString());
        ServerOptions options = command.options();

        assertEquals(9300, options.httpPort());
        assertEquals("yaml-cluster", options.clusterName());
        assertEquals("yaml-node", options.nodeId());
        assertEquals("yaml-node-name", options.nodeName());
        assertEquals("0.0.0.0", options.host());
        assertEquals(Set.of(NodeRole.MASTER, NodeRole.DATA), options.roles());
        assertEquals("http://127.0.0.1:2379", options.etcdEndpoints());
        assertEquals("yaml/ns", options.etcdNamespace());
        assertEquals(7, options.etcdTimeoutSeconds());
        assertEquals("data/yaml-node", options.dataPath());
        assertEquals(1048576, options.cacheMaxBytes());
        assertEquals(30, options.cacheCleanupIntervalSeconds());
        assertEquals(9500, options.metricsPort());
        assertEquals(8, options.maxWriteRequests());
        assertEquals(500, options.maxBulkItems());
        assertEquals(10485760, options.maxBulkBytes());
        assertEquals(false, options.commitEveryRequest());
        assertEquals(2500, options.commitIntervalMillis());
        assertEquals(128, options.commitAfterDocs());
        assertEquals("interval", options.refreshPolicy());
        assertEquals(1500, options.refreshIntervalMillis());
        assertEquals("plugins/analyzers", options.analyzerPluginPath());
        assertEquals("yaml-bucket", options.s3Bucket());
        assertEquals("us-east-1", options.s3Region());
        assertEquals("http", options.s3Protocol());
        assertEquals("http://127.0.0.1:9000", options.s3Endpoint());
        assertEquals(true, options.s3ChunkedEncoding());
        assertEquals(true, options.s3ContentMd5());
        assertEquals("minio", options.s3AccessKey());
        assertEquals("password", options.s3SecretKey());
        assertEquals(3, options.snapshotRetainLatest());
    }

    @Test
    void commandLineOptionsOverrideJsonConfiguration() throws Exception {
        Path config = tempDir.resolve("server.json");
        Files.writeString(config, """
                {
                  "httpPort": 9300,
                  "nodeId": "json-node",
                  "roles": "DATA,COORDINATING",
                  "s3": {
                    "bucket": "json-bucket",
                    "chunkedEncoding": true,
                    "contentMd5": true
                  },
                  "snapshotRetainLatest": 4,
                  "cacheMaxBytes": 2048
                }
                """);

        Server command = new Server();
        new CommandLine(command).parseArgs(
                "--conf", config.toString(),
                "-p", "9400",
                "--s3-bucket", "cli-bucket",
                "--no-s3-chunked-encoding",
                "--no-s3-content-md5",
                "--etcd-timeout", "6",
                "--cache-max-bytes", "4096",
                "--cache-cleanup-interval", "45",
                "--metrics-port", "9600",
                "--commit-every-request=false",
                "--commit-interval", "750",
                "--commit-after-docs", "64",
                "--refresh-policy", "interval",
                "--refresh-interval", "500",
                "--max-write-requests", "3",
                "--max-bulk-items", "50",
                "--max-bulk-bytes", "4096"
        );
        ServerOptions options = command.options();

        assertEquals(9400, options.httpPort());
        assertEquals("json-node", options.nodeId());
        assertEquals(Set.of(NodeRole.DATA, NodeRole.COORDINATING), options.roles());
        assertEquals("cli-bucket", options.s3Bucket());
        assertEquals(false, options.s3ChunkedEncoding());
        assertEquals(false, options.s3ContentMd5());
        assertEquals(4, options.snapshotRetainLatest());
        assertEquals(6, options.etcdTimeoutSeconds());
        assertEquals(4096, options.cacheMaxBytes());
        assertEquals(45, options.cacheCleanupIntervalSeconds());
        assertEquals(9600, options.metricsPort());
        assertEquals(false, options.commitEveryRequest());
        assertEquals(750, options.commitIntervalMillis());
        assertEquals(64, options.commitAfterDocs());
        assertEquals("interval", options.refreshPolicy());
        assertEquals(500, options.refreshIntervalMillis());
        assertEquals(3, options.maxWriteRequests());
        assertEquals(50, options.maxBulkItems());
        assertEquals(4096, options.maxBulkBytes());
    }
}
