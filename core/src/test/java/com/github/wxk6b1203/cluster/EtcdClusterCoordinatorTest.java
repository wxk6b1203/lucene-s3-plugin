package com.github.wxk6b1203.cluster;

import com.github.wxk6b1203.cluster.etcd.EtcdClusterCoordinator;
import com.github.wxk6b1203.cluster.etcd.EtcdClusterStateRepository;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.options.DeleteOption;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EtcdClusterCoordinatorTest {
    @Test
    @EnabledIfEnvironmentVariable(named = "ETCD_TEST_ENDPOINTS", matches = ".+")
    public void coordinatesMasterLeaseHeartbeatRoutingAndNodeFailover() throws Exception {
        Client client = Client.builder().endpoints(System.getenv("ETCD_TEST_ENDPOINTS")).build();
        String namespace = "test-cluster/" + UUID.randomUUID();
        EtcdClusterCoordinator node1Coordinator = null;
        EtcdClusterCoordinator node2Coordinator = null;
        try {
            EtcdClusterStateRepository repository = new EtcdClusterStateRepository(
                    EtcdClusterStateRepository.Options.builder()
                            .namespace(namespace)
                            .clusterName("test-cluster")
                            .build(),
                    client
            );
            ClusterNode node1 = node("node-1", 9200, Set.of(NodeRole.MASTER, NodeRole.DATA, NodeRole.COORDINATING));
            ClusterNode node2 = node("node-2", 9201, Set.of(NodeRole.DATA, NodeRole.COORDINATING));
            node1Coordinator = coordinator(namespace, client, repository, node1);
            node2Coordinator = coordinator(namespace, client, repository, node2);

            node1Coordinator.start();
            node2Coordinator.start();

            EtcdClusterCoordinator masterCoordinator = node1Coordinator;
            waitUntil(() -> repository.current().nodes().keySet().containsAll(Set.of("node-1", "node-2")));
            waitUntil(masterCoordinator::isMaster);
            assertEquals("node-1", repository.current().masterNodeId());

            ClusterIndexService indexService = new DefaultClusterIndexService(
                    new MasterOnlyClusterStateRepository(repository, masterCoordinator)
            );
            ClusterState created = indexService.createIndex(new IndexSettings("books", 4, null, Instant.now()));
            assertEquals(4, created.routingTable().size());
            waitUntil(() -> {
                ClusterState state = repository.current();
                return shardsOn(state, "node-1") == 2 && shardsOn(state, "node-2") == 2;
            });
            long versionAfterCreate = repository.current().version();

            node2Coordinator.close();
            node2Coordinator = null;
            waitUntil(() -> !repository.current().nodes().containsKey("node-2"));
            waitUntil(() -> {
                ClusterState state = repository.current();
                return shardsOn(state, "node-1") == 4
                        && state.routingTable().stream().allMatch(routing -> routing.state() == ShardState.STARTED);
            });
            ClusterState failedOver = repository.current();

            assertTrue(failedOver.version() > versionAfterCreate);
            assertEquals(4, shardsOn(failedOver, "node-1"));
            assertEquals(0, shardsOn(failedOver, "node-2"));
        } finally {
            if (node2Coordinator != null) {
                node2Coordinator.close();
            }
            if (node1Coordinator != null) {
                node1Coordinator.close();
            }
            deletePrefix(client, namespace);
            client.close();
        }
    }

    private EtcdClusterCoordinator coordinator(
            String namespace,
            Client client,
            EtcdClusterStateRepository repository,
            ClusterNode node
    ) {
        return new EtcdClusterCoordinator(
                EtcdClusterCoordinator.Options.builder()
                        .namespace(namespace)
                        .heartbeatTtlSeconds(3)
                        .masterTtlSeconds(3)
                        .build(),
                client,
                repository,
                repository,
                node
        );
    }

    private ClusterNode node(String id, int httpPort, Set<NodeRole> roles) {
        return new ClusterNode(id, id, "127.0.0.1", httpPort, roles, Instant.now());
    }

    private long shardsOn(ClusterState state, String nodeId) {
        return state.routingTable().stream()
                .filter(routing -> routing.state() == ShardState.STARTED)
                .filter(routing -> nodeId.equals(routing.nodeId()))
                .count();
    }

    private void waitUntil(CheckedCondition condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(12);
        Exception lastException = null;
        while (System.nanoTime() < deadline) {
            try {
                if (condition.evaluate()) {
                    return;
                }
                lastException = null;
            } catch (Exception e) {
                lastException = e;
            }
            Thread.sleep(100);
        }
        if (lastException != null) {
            throw lastException;
        }
        throw new AssertionError("condition was not met before timeout");
    }

    private void deletePrefix(Client client, String namespace) throws Exception {
        String normalized = namespace;
        while (normalized.startsWith("/")) {
            normalized = normalized.substring(1);
        }
        ByteSequence prefix = ByteSequence.from(("/" + normalized).getBytes(StandardCharsets.UTF_8));
        client.getKVClient()
                .delete(prefix, DeleteOption.builder().isPrefix(true).build())
                .get(5, TimeUnit.SECONDS);
    }

    @FunctionalInterface
    private interface CheckedCondition {
        boolean evaluate() throws Exception;
    }
}
