package com.github.wxk6b1203.cluster.etcd;

import com.github.wxk6b1203.cluster.ClusterNode;
import com.github.wxk6b1203.cluster.ClusterState;
import com.github.wxk6b1203.cluster.NodeRole;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class EtcdClusterCoordinatorUnitTest {
    @Test
    void rebalanceFingerprintIgnoresHeartbeatTimestamp() {
        ClusterState first = state(1, Map.of(
                "node-1", node("node-1", Set.of(NodeRole.MASTER, NodeRole.DATA), Instant.parse("2026-05-23T00:00:00Z")),
                "node-2", node("node-2", Set.of(NodeRole.DATA), Instant.parse("2026-05-23T00:00:00Z"))
        ));
        ClusterState heartbeatOnly = state(1, Map.of(
                "node-1", node("node-1", Set.of(NodeRole.MASTER, NodeRole.DATA), Instant.parse("2026-05-23T00:00:03Z")),
                "node-2", node("node-2", Set.of(NodeRole.DATA), Instant.parse("2026-05-23T00:00:03Z"))
        ));

        assertEquals(
                EtcdClusterCoordinator.rebalanceFingerprint(first),
                EtcdClusterCoordinator.rebalanceFingerprint(heartbeatOnly)
        );
    }

    @Test
    void rebalanceFingerprintChangesWhenStateVersionOrDataNodesChange() {
        ClusterState first = state(1, Map.of(
                "node-1", node("node-1", Set.of(NodeRole.MASTER, NodeRole.DATA), Instant.now())
        ));
        ClusterState versionChanged = state(2, first.nodes());
        ClusterState dataNodesChanged = state(1, Map.of(
                "node-1", node("node-1", Set.of(NodeRole.MASTER, NodeRole.DATA), Instant.now()),
                "node-2", node("node-2", Set.of(NodeRole.DATA), Instant.now())
        ));

        assertNotEquals(
                EtcdClusterCoordinator.rebalanceFingerprint(first),
                EtcdClusterCoordinator.rebalanceFingerprint(versionChanged)
        );
        assertNotEquals(
                EtcdClusterCoordinator.rebalanceFingerprint(first),
                EtcdClusterCoordinator.rebalanceFingerprint(dataNodesChanged)
        );
    }

    private ClusterState state(long version, Map<String, ClusterNode> nodes) {
        return new ClusterState("test", version, "node-1", nodes, Map.of(), List.of(), Map.of(), Instant.now());
    }

    private ClusterNode node(String id, Set<NodeRole> roles, Instant lastSeenAt) {
        return new ClusterNode(id, id, "127.0.0.1", 9200, roles, lastSeenAt);
    }
}
