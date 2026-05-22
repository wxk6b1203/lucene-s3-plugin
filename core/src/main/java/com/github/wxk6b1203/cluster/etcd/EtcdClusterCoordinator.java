package com.github.wxk6b1203.cluster.etcd;

import com.github.wxk6b1203.cluster.BalancedShardAllocator;
import com.github.wxk6b1203.cluster.ClusterCoordinator;
import com.github.wxk6b1203.cluster.ClusterNode;
import com.github.wxk6b1203.cluster.ClusterStateRepository;
import com.github.wxk6b1203.cluster.NodeRole;
import com.github.wxk6b1203.cluster.ShardAllocator;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.PutOption;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class EtcdClusterCoordinator implements ClusterCoordinator {
    private final Client client;
    private final EtcdClusterStateRepository repository;
    private final ClusterStateRepository stateRepository;
    private final ClusterNode localNode;
    private final String namespace;
    private final long heartbeatTtlSeconds;
    private final long masterTtlSeconds;
    private final long operationTimeoutSeconds;
    private final boolean masterEligible;
    private final ShardAllocator shardAllocator;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            Thread.ofVirtual().name("cluster-coordinator-", 0).factory()
    );
    private final AtomicBoolean master = new AtomicBoolean(false);
    private long nodeLeaseId;
    private long masterLeaseId;

    @Data
    @Builder
    public static class Options {
        @Builder.Default
        private String namespace = "lucene-s3/cluster";
        @Builder.Default
        private long heartbeatTtlSeconds = 10;
        @Builder.Default
        private long masterTtlSeconds = 10;
        @Builder.Default
        private long operationTimeoutSeconds = 10;
    }

    public EtcdClusterCoordinator(
            Options options,
            Client client,
            EtcdClusterStateRepository repository,
            ClusterStateRepository stateRepository,
            ClusterNode localNode
    ) {
        this.client = client;
        this.repository = repository;
        this.stateRepository = stateRepository;
        this.localNode = localNode;
        this.namespace = normalize(options.namespace);
        this.heartbeatTtlSeconds = options.heartbeatTtlSeconds;
        this.masterTtlSeconds = options.masterTtlSeconds;
        this.operationTimeoutSeconds = Math.max(1, options.operationTimeoutSeconds);
        this.masterEligible = localNode.roles().contains(NodeRole.MASTER);
        this.shardAllocator = new BalancedShardAllocator();
    }

    @Override
    public void start() throws IOException {
        try {
            this.nodeLeaseId = await(client.getLeaseClient().grant(heartbeatTtlSeconds)).getID();
            if (masterEligible) {
                this.masterLeaseId = await(client.getLeaseClient().grant(masterTtlSeconds)).getID();
            }
            heartbeat();
            if (masterEligible) {
                campaignMaster();
            }
            scheduler.scheduleAtFixedRate(this::safeTick, 1, Math.max(1, heartbeatTtlSeconds / 3), TimeUnit.SECONDS);
        } catch (Exception e) {
            throw ioException("failed to start cluster coordinator", e);
        }
    }

    @Override
    public boolean isMaster() {
        if (!masterEligible || !master.get()) {
            return false;
        }
        try {
            var response = client.getKVClient().get(key("master")).get(1, TimeUnit.SECONDS);
            boolean ownsLeaseKey = !response.getKvs().isEmpty()
                    && localNode.id().equals(response.getKvs().getFirst().getValue().toString(StandardCharsets.UTF_8));
            master.set(ownsLeaseKey);
            return ownsLeaseKey;
        } catch (Exception e) {
            master.set(false);
            return false;
        }
    }

    private void safeTick() {
        try {
            heartbeat();
            if (masterEligible) {
                campaignMaster();
            }
            if (masterEligible && master.get()) {
                stateRepository.update(state -> shardAllocator.rebalance(state));
            }
        } catch (Exception e) {
            log.warn("cluster coordinator tick failed", e);
        }
    }

    private void heartbeat() throws Exception {
        await(client.getLeaseClient().keepAliveOnce(nodeLeaseId));
        repository.putNode(touch(localNode), nodeLeaseId);
    }

    private void campaignMaster() throws Exception {
        try {
            await(client.getLeaseClient().keepAliveOnce(masterLeaseId));
            ByteSequence masterKey = key("master");
            ByteSequence value = ByteSequence.from(localNode.id().getBytes(StandardCharsets.UTF_8));
            PutOption putOption = PutOption.builder().withLeaseId(masterLeaseId).build();
            var response = await(client.getKVClient().txn()
                    .If(new Cmp(masterKey, Cmp.Op.EQUAL, CmpTarget.version(0)))
                    .Then(Op.put(masterKey, value, putOption))
                    .Else(Op.get(masterKey, io.etcd.jetcd.options.GetOption.DEFAULT))
                    .commit());
            boolean won = response.isSucceeded();
            boolean stillMaster = won;
            if (!won && !response.getGetResponses().isEmpty() && !response.getGetResponses().getFirst().getKvs().isEmpty()) {
                String currentMaster = response.getGetResponses().getFirst().getKvs().getFirst().getValue().toString(StandardCharsets.UTF_8);
                stillMaster = localNode.id().equals(currentMaster);
            }
            master.set(stillMaster);
            if (stillMaster) {
                stateRepository.updateMaster(localNode.id());
            }
        } catch (Exception e) {
            master.set(false);
            throw e;
        }
    }

    private ClusterNode touch(ClusterNode node) {
        return new ClusterNode(node.id(), node.name(), node.host(), node.httpPort(), node.roles(), Instant.now());
    }

    private ByteSequence key(String suffix) {
        return ByteSequence.from((namespace + "/" + suffix).getBytes(StandardCharsets.UTF_8));
    }

    private String normalize(String value) {
        String normalized = value == null || value.isBlank() ? "lucene-s3/cluster" : value;
        while (normalized.startsWith("/")) {
            normalized = normalized.substring(1);
        }
        while (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return "/" + normalized;
    }

    private IOException ioException(String message, Exception cause) {
        IOException exception = new IOException(message + ": " + cause.getMessage());
        exception.initCause(cause);
        return exception;
    }

    private <T> T await(CompletableFuture<T> future) throws Exception {
        return future.get(operationTimeoutSeconds, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        try {
            if (nodeLeaseId != 0) {
                client.getLeaseClient().revoke(nodeLeaseId).get(3, TimeUnit.SECONDS);
            }
            if (masterLeaseId != 0) {
                client.getLeaseClient().revoke(masterLeaseId).get(3, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            log.warn("failed to revoke etcd leases", e);
        }
    }
}
