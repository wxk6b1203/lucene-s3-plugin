package com.github.wxk6b1203.http;

import com.github.wxk6b1203.cluster.ClusterCoordinator;
import com.github.wxk6b1203.cluster.ClusterNode;
import com.github.wxk6b1203.cluster.ClusterState;
import com.github.wxk6b1203.cluster.ClusterStateRepository;
import com.github.wxk6b1203.cluster.IndexLifecyclePolicy;
import com.github.wxk6b1203.cluster.LifecyclePhase;
import com.github.wxk6b1203.cluster.ShardId;
import com.github.wxk6b1203.cluster.ShardRouting;
import com.github.wxk6b1203.cluster.ShardState;
import com.github.wxk6b1203.index.LocalShardIndexService;
import com.github.wxk6b1203.metadata.common.IndexCommitSnapshot;
import com.github.wxk6b1203.metadata.common.IndexFileMetadata;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.metadata.provider.ManifestMetadataManager;
import com.github.wxk6b1203.store.manifest.ManifestManager;
import com.github.wxk6b1203.store.manifest.ManifestOptions;
import com.github.wxk6b1203.store.object.RemoteObjectStore;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
final class ClusterMaintenanceService {
    private static final long STUCK_PENDING_UPLOAD_MILLIS = Duration.ofMinutes(1).toMillis();
    private static final List<IndexFileStatus> ALL_UPLOAD_STATUSES = List.of(
            IndexFileStatus.DIRTY,
            IndexFileStatus.UPLOADING,
            IndexFileStatus.CLEAN,
            IndexFileStatus.PINNED
    );
    private static final List<IndexFileStatus> PENDING_UPLOAD_STATUSES = List.of(
            IndexFileStatus.DIRTY,
            IndexFileStatus.UPLOADING
    );

    private final ClusterStateRepository clusterStateRepository;
    private final ClusterCoordinator clusterCoordinator;
    private final ClusterNode localNode;
    private final LocalShardIndexService localShardIndexService;
    private final ManifestMetadataManager manifestMetadataManager;
    private final RemoteObjectStore remoteObjectStore;
    private final IndexDataDeleter indexDataDeleter;
    private final int snapshotRetainLatest;
    private final LocalCacheManager cacheManager;
    private final Duration cacheCleanupInterval;
    private final Set<String> lifecycleDeletesInProgress = ConcurrentHashMap.newKeySet();
    private volatile Instant lastCacheCleanup = Instant.EPOCH;
    private volatile LocalCacheManager.CleanupStats lastCacheCleanupStats = new LocalCacheManager.CleanupStats(0, 0, 0, 0);

    ClusterMaintenanceService(
            ClusterStateRepository clusterStateRepository,
            ClusterCoordinator clusterCoordinator,
            ClusterNode localNode,
            LocalShardIndexService localShardIndexService,
            ManifestMetadataManager manifestMetadataManager,
            RemoteObjectStore remoteObjectStore,
            int snapshotRetainLatest,
            IndexDataDeleter indexDataDeleter,
            LocalCacheManager cacheManager,
            Duration cacheCleanupInterval
    ) {
        this.clusterStateRepository = clusterStateRepository;
        this.clusterCoordinator = clusterCoordinator;
        this.localNode = localNode;
        this.localShardIndexService = localShardIndexService;
        this.manifestMetadataManager = manifestMetadataManager;
        this.remoteObjectStore = remoteObjectStore;
        this.snapshotRetainLatest = snapshotRetainLatest;
        this.indexDataDeleter = indexDataDeleter;
        this.cacheManager = cacheManager;
        this.cacheCleanupInterval = cacheCleanupInterval == null || cacheCleanupInterval.isNegative()
                ? Duration.ofMinutes(1)
                : cacheCleanupInterval;
        this.lastCacheCleanupStats = new LocalCacheManager.CleanupStats(
                0,
                cacheManager == null ? 0 : cacheManager.maxBytes(),
                0,
                0
        );
    }

    void tick() {
        retryOwnedShardUploads();
        runSnapshotGarbageCollection();
        runLifecyclePolicies();
        runLocalCacheCleanup();
    }

    Map<String, Object> uploadStatus(String indexFilter) throws IOException {
        return uploadStatus(clusterStateRepository.current(), indexFilter);
    }

    Map<String, Object> retryPendingUploadsNow(String indexFilter) throws IOException {
        ClusterState state = clusterStateRepository.current();
        retryOwnedShardUploads(state, indexFilter);
        return uploadStatus(clusterStateRepository.current(), indexFilter);
    }

    Map<String, Object> cacheStatus() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("enabled", cacheManager != null && cacheManager.enabled());
        result.put("last_cleanup_time", lastCacheCleanup.equals(Instant.EPOCH) ? null : lastCacheCleanup.toString());
        result.put("total_bytes", lastCacheCleanupStats.totalBytes());
        result.put("max_bytes", lastCacheCleanupStats.maxBytes());
        result.put("last_deleted_files", lastCacheCleanupStats.deletedFiles());
        result.put("last_deleted_bytes", lastCacheCleanupStats.deletedBytes());
        return result;
    }

    void retryOwnedShardUploads() {
        try {
            retryOwnedShardUploads(clusterStateRepository.current(), null);
        } catch (IOException e) {
            log.warn("failed to retry pending shard uploads", e);
        }
    }

    void runSnapshotGarbageCollection() {
        if (!clusterCoordinator.isMaster()) {
            return;
        }
        ClusterState state;
        try {
            state = clusterStateRepository.current();
        } catch (IOException e) {
            log.warn("failed to load cluster state for snapshot garbage collection", e);
            return;
        }
        try (ManifestManager manifestManager = new ManifestManager(
                new ManifestOptions(""),
                remoteObjectStore,
                manifestMetadataManager
        )) {
            state.indices().forEach((indexName, settings) -> {
                for (int shard = 0; shard < settings.numberOfShards(); shard++) {
                    String physicalIndexName = physicalIndexName(indexName, shard);
                    try {
                        manifestManager.garbageCollectSnapshots(physicalIndexName, snapshotRetainLatest);
                    } catch (Exception e) {
                        log.warn("failed to garbage collect snapshots for {}", physicalIndexName, e);
                    }
                }
            });
        }
    }

    private void retryOwnedShardUploads(ClusterState state, String indexFilter) throws IOException {
        List<ShardId> shardIds = state.routingTable().stream()
                .filter(routing -> routing.state() == ShardState.STARTED)
                .filter(routing -> localNode.id().equals(routing.nodeId()))
                .filter(routing -> indexFilter == null || indexFilter.equals(routing.shardId().indexName()))
                .map(ShardRouting::shardId)
                .toList();
        if (!shardIds.isEmpty()) {
            localShardIndexService.retryPendingUploads(shardIds);
        }
    }

    private Map<String, Object> uploadStatus(ClusterState state, String indexFilter) {
        if (indexFilter != null && !state.indices().containsKey(indexFilter)) {
            throw new IllegalArgumentException("index not found: " + indexFilter);
        }
        Instant now = Instant.now();
        List<Map<String, Object>> indices = new ArrayList<>();
        long pendingShards = 0;
        long stuckShards = 0;
        long pendingFiles = 0;
        for (var entry : state.indices().entrySet()) {
            String indexName = entry.getKey();
            if (indexFilter != null && !indexFilter.equals(indexName)) {
                continue;
            }
            List<Map<String, Object>> shards = new ArrayList<>();
            for (int shard = 0; shard < entry.getValue().numberOfShards(); shard++) {
                Map<String, Object> shardStatus = shardUploadStatus(state, indexName, shard, now);
                shards.add(shardStatus);
                boolean pending = Boolean.TRUE.equals(shardStatus.get("pending"));
                boolean stuck = Boolean.TRUE.equals(shardStatus.get("stuck"));
                if (pending) {
                    pendingShards++;
                    pendingFiles += ((Number) shardStatus.get("pending_files")).longValue();
                }
                if (stuck) {
                    stuckShards++;
                }
            }
            indices.add(Map.of(
                    "index", indexName,
                    "shards", shards
            ));
        }
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("indices", indices.size());
        summary.put("pending_shards", pendingShards);
        summary.put("stuck_shards", stuckShards);
        summary.put("pending_files", pendingFiles);
        summary.put("stuck_after_millis", STUCK_PENDING_UPLOAD_MILLIS);
        return Map.of(
                "summary", summary,
                "indices", indices
        );
    }

    private Map<String, Object> shardUploadStatus(ClusterState state, String indexName, int shard, Instant now) {
        String physicalIndexName = physicalIndexName(indexName, shard);
        List<IndexFileMetadata> files = manifestMetadataManager.listAll(physicalIndexName, ALL_UPLOAD_STATUSES);
        List<IndexFileMetadata> pending = files.stream()
                .filter(file -> PENDING_UPLOAD_STATUSES.contains(file.getStatus()))
                .toList();
        Map<String, Integer> statusCounts = new LinkedHashMap<>();
        for (IndexFileStatus status : IndexFileStatus.values()) {
            statusCounts.put(status.name(), 0);
        }
        for (IndexFileMetadata file : files) {
            statusCounts.computeIfPresent(file.getStatus().name(), (ignored, value) -> value + 1);
        }
        long oldestPendingAgeMillis = pending.stream()
                .mapToLong(file -> Math.max(0, now.toEpochMilli() - file.getModifiedTime()))
                .max()
                .orElse(0);
        IndexCommitSnapshot latestSnapshot = manifestMetadataManager.latestSnapshot(physicalIndexName);
        ShardRouting routing = routing(state, indexName, shard);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("index", indexName);
        result.put("shard", shard);
        result.put("physical_index", physicalIndexName);
        result.put("owner_node", routing == null ? null : routing.nodeId());
        result.put("owner_term", routing == null ? null : routing.ownerTerm());
        result.put("allocation_epoch", routing == null ? null : routing.allocationEpoch());
        result.put("status_counts", statusCounts);
        result.put("pending", !pending.isEmpty());
        result.put("pending_files", pending.size());
        result.put("oldest_pending_age_millis", oldestPendingAgeMillis);
        result.put("stuck", !pending.isEmpty() && oldestPendingAgeMillis >= STUCK_PENDING_UPLOAD_MILLIS);
        result.put("remote_snapshot_ready", pending.isEmpty() && latestSnapshot != null);
        result.put("latest_snapshot_generation", latestSnapshot == null ? null : latestSnapshot.getGeneration());
        result.put("latest_snapshot_segment", latestSnapshot == null ? null : latestSnapshot.getSegmentFileName());
        result.put("pending_file_details", pending.stream().map(this::pendingFile).toList());
        return result;
    }

    private Map<String, Object> pendingFile(IndexFileMetadata file) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("name", file.getName());
        result.put("status", file.getStatus().name());
        result.put("epoch", file.getEpoch());
        result.put("size", file.getSize());
        result.put("modified_time", file.getModifiedTime());
        result.put("object_key", file.getObjectKey());
        return result;
    }

    private ShardRouting routing(ClusterState state, String indexName, int shard) {
        ShardId shardId = new ShardId(indexName, shard);
        return state.routingTable().stream()
                .filter(routing -> routing.shardId().equals(shardId))
                .findFirst()
                .orElse(null);
    }

    private void runLifecyclePolicies() {
        if (!clusterCoordinator.isMaster()) {
            return;
        }
        ClusterState state;
        try {
            state = clusterStateRepository.current();
        } catch (IOException e) {
            log.warn("failed to load cluster state for lifecycle execution", e);
            return;
        }
        Instant now = Instant.now();
        state.indices().forEach((indexName, settings) -> {
            String policyName = settings.lifecyclePolicy();
            if (policyName == null || policyName.isBlank()) {
                return;
            }
            IndexLifecyclePolicy policy = state.lifecyclePolicies().get(policyName);
            if (policy == null) {
                return;
            }
            Long deleteAgeMillis = policy.minAgeMillisByPhase().get(LifecyclePhase.DELETE);
            if (deleteAgeMillis == null) {
                return;
            }
            long indexAgeMillis = Math.max(0, Duration.between(settings.createdAt(), now).toMillis());
            if (indexAgeMillis < deleteAgeMillis || !lifecycleDeletesInProgress.add(indexName)) {
                return;
            }
            try {
                indexDataDeleter.deleteIndexAndData(indexName, settings.numberOfShards());
                log.info("deleted index {} by lifecycle policy {}", indexName, policyName);
            } catch (Exception e) {
                log.warn("failed to delete index {} by lifecycle policy {}", indexName, policyName, e);
            } finally {
                lifecycleDeletesInProgress.remove(indexName);
            }
        });
    }

    private void runLocalCacheCleanup() {
        if (cacheManager == null || !cacheManager.enabled()) {
            return;
        }
        Instant now = Instant.now();
        if (Duration.between(lastCacheCleanup, now).compareTo(cacheCleanupInterval) < 0) {
            return;
        }
        lastCacheCleanup = now;
        lastCacheCleanupStats = cacheManager.cleanup();
    }

    private String physicalIndexName(String indexName, int shard) {
        return indexName + "__shard_" + shard;
    }

    interface IndexDataDeleter {
        void deleteIndexAndData(String indexName, int numberOfShards) throws IOException;
    }
}
