package com.github.wxk6b1203.cli;

import com.github.wxk6b1203.cluster.NodeRole;
import com.github.wxk6b1203.config.ServerOptions;
import com.github.wxk6b1203.http.HttpApiServer;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@CommandLine.Command(name = "server", mixinStandardHelpOptions = true, versionProvider = Version.class, description = "Lucene S3 Server")
public class Server implements Callable<Integer> {
    @CommandLine.Option(names = {"-c", "--conf"}, description = "Path to configuration file")
    private String configFile;

    @CommandLine.Option(names = {"-p", "--http-port"}, description = "HTTP API port")
    private int httpPort = 9200;

    @CommandLine.Option(names = {"--cluster-name"}, description = "Cluster name")
    private String clusterName = "lucene-s3";

    @CommandLine.Option(names = {"--node-id"}, description = "Stable node id")
    private String nodeId = UUID.randomUUID().toString();

    @CommandLine.Option(names = {"--node-name"}, description = "Node display name")
    private String nodeName = "local-node";

    @CommandLine.Option(names = {"--host"}, description = "Advertised host")
    private String host = "127.0.0.1";

    @CommandLine.Option(names = {"--roles"}, description = "Comma separated node roles: MASTER,DATA,INGEST,COORDINATING")
    private String roles = "MASTER,DATA,COORDINATING";

    @CommandLine.Option(names = {"--etcd-endpoints"}, description = "ETCD endpoints. When set, enables distributed cluster state.")
    private String etcdEndpoints;

    @CommandLine.Option(names = {"--etcd-namespace"}, description = "ETCD namespace for cluster state")
    private String etcdNamespace = "lucene-s3/cluster";

    @CommandLine.Option(names = {"--data-path"}, description = "Local data/cache path")
    private String dataPath = "data";

    @CommandLine.Option(names = {"--s3-bucket"}, description = "S3 bucket for committed Lucene files")
    private String s3Bucket;

    @CommandLine.Option(names = {"--s3-region"}, description = "S3 region")
    private String s3Region;

    @CommandLine.Option(names = {"--s3-endpoint"}, description = "S3-compatible endpoint")
    private String s3Endpoint;

    @CommandLine.Option(names = {"--s3-access-key"}, description = "S3 access key")
    private String s3AccessKey;

    @CommandLine.Option(names = {"--s3-secret-key"}, description = "S3 secret key")
    private String s3SecretKey;

    @Override
    public Integer call() {
        log.info("Starting Lucene S3 Server with config file: {}", configFile);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        try (HttpApiServer apiServer = new HttpApiServer(options())) {
            apiServer.start().toCompletionStage().toCompletableFuture().join();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutting down Lucene S3 Server...");
                countDownLatch.countDown();
            }));
            countDownLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return 1;
        } catch (Exception e) {
            log.error("Lucene S3 Server failed", e);
            return 1;
        }
        return CommandLine.ExitCode.OK;
    }

    private ServerOptions options() {
        Set<NodeRole> parsedRoles = Arrays.stream(roles.split(","))
                .map(String::trim)
                .filter(role -> !role.isBlank())
                .map(String::toUpperCase)
                .map(NodeRole::valueOf)
                .collect(Collectors.toSet());
        return new ServerOptions(
                httpPort,
                clusterName,
                nodeId,
                nodeName,
                host,
                parsedRoles,
                etcdEndpoints,
                etcdNamespace,
                dataPath,
                s3Bucket,
                s3Region,
                s3Endpoint,
                s3AccessKey,
                s3SecretKey
        );
    }
}
