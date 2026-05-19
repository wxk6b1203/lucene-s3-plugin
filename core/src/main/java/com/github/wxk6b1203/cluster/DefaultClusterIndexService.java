package com.github.wxk6b1203.cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultClusterIndexService implements ClusterIndexService, IndexLifecycleService {
    private final ClusterStateRepository repository;
    private final ShardAllocator shardAllocator;

    public DefaultClusterIndexService(ClusterStateRepository repository) {
        this(repository, new BalancedShardAllocator());
    }

    public DefaultClusterIndexService(ClusterStateRepository repository, ShardAllocator shardAllocator) {
        this.repository = repository;
        this.shardAllocator = shardAllocator;
    }

    @Override
    public ClusterState createIndex(IndexSettings settings) throws IOException {
        return repository.update(state -> {
            if (state.indices().containsKey(settings.name())) {
                throw new IllegalArgumentException("index already exists: " + settings.name());
            }
            HashMap<String, IndexSettings> indices = new HashMap<>(state.indices());
            indices.put(settings.name(), settings);
            List<ShardRouting> routing = new ArrayList<>(state.routingTable());
            for (int shard = 0; shard < settings.numberOfShards(); shard++) {
                routing.add(new ShardRouting(
                        new ShardId(settings.name(), shard),
                        ShardState.UNASSIGNED,
                        null,
                        1,
                        state.version()
                ));
            }
            return shardAllocator.rebalance(new ClusterState(
                    state.clusterName(),
                    state.version(),
                    state.masterNodeId(),
                    state.nodes(),
                    indices,
                    routing,
                    state.lifecyclePolicies(),
                    state.updatedAt()
            ));
        });
    }

    @Override
    public ClusterState deleteIndex(String indexName) throws IOException {
        return repository.update(state -> {
            HashMap<String, IndexSettings> indices = new HashMap<>(state.indices());
            indices.remove(indexName);
            List<ShardRouting> routing = state.routingTable().stream()
                    .filter(shard -> !shard.shardId().indexName().equals(indexName))
                    .toList();
            return new ClusterState(
                    state.clusterName(),
                    state.version(),
                    state.masterNodeId(),
                    state.nodes(),
                    indices,
                    routing,
                    state.lifecyclePolicies(),
                    state.updatedAt()
            );
        });
    }

    @Override
    public ClusterState putMapping(String indexName, Map<String, FieldMapping> mappings) throws IOException {
        return repository.update(state -> {
            IndexSettings current = state.indices().get(indexName);
            if (current == null) {
                throw new IllegalArgumentException("index not found: " + indexName);
            }
            HashMap<String, FieldMapping> mergedMappings = new HashMap<>(current.mappings());
            mappings.forEach((field, mapping) -> {
                FieldMapping existing = mergedMappings.get(field);
                if (existing != null && !existing.equals(mapping)) {
                    throw new IllegalArgumentException("mapping field already exists with a different definition: " + field);
                }
                mergedMappings.put(field, mapping);
            });
            if (mergedMappings.equals(current.mappings())) {
                return state;
            }
            HashMap<String, IndexSettings> indices = new HashMap<>(state.indices());
            indices.put(indexName, new IndexSettings(
                    current.name(),
                    current.numberOfShards(),
                    current.lifecyclePolicy(),
                    current.createdAt(),
                    mergedMappings
            ));
            return new ClusterState(
                    state.clusterName(),
                    state.version(),
                    state.masterNodeId(),
                    state.nodes(),
                    indices,
                    state.routingTable(),
                    state.lifecyclePolicies(),
                    state.updatedAt()
            );
        });
    }

    @Override
    public ClusterState putPolicy(IndexLifecyclePolicy policy) throws IOException {
        return repository.update(state -> {
            HashMap<String, IndexLifecyclePolicy> policies = new HashMap<>(state.lifecyclePolicies());
            policies.put(policy.name(), policy);
            return new ClusterState(
                    state.clusterName(),
                    state.version(),
                    state.masterNodeId(),
                    state.nodes(),
                    state.indices(),
                    state.routingTable(),
                    policies,
                    state.updatedAt()
            );
        });
    }

    @Override
    public ClusterState attachPolicy(String indexName, String policyName) throws IOException {
        return repository.update(state -> {
            IndexSettings current = state.indices().get(indexName);
            if (current == null) {
                throw new IllegalArgumentException("index not found: " + indexName);
            }
            if (!state.lifecyclePolicies().containsKey(policyName)) {
                throw new IllegalArgumentException("lifecycle policy not found: " + policyName);
            }
            HashMap<String, IndexSettings> indices = new HashMap<>(state.indices());
            indices.put(indexName, new IndexSettings(
                    current.name(),
                    current.numberOfShards(),
                    policyName,
                    current.createdAt(),
                    current.mappings()
            ));
            return new ClusterState(
                    state.clusterName(),
                    state.version(),
                    state.masterNodeId(),
                    state.nodes(),
                    indices,
                    state.routingTable(),
                    state.lifecyclePolicies(),
                    state.updatedAt()
            );
        });
    }
}
