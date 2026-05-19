package com.github.wxk6b1203.cluster;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

public class HashShardRouter implements ShardRouter {
    @Override
    public ShardId route(String indexName, String routingKey, ClusterState state) {
        IndexSettings settings = state.indices().get(indexName);
        if (settings == null) {
            throw new IllegalArgumentException("index not found: " + indexName);
        }
        CRC32 crc32 = new CRC32();
        crc32.update((routingKey == null || routingKey.isBlank() ? indexName : routingKey).getBytes(StandardCharsets.UTF_8));
        int shard = Math.toIntExact(Math.floorMod(crc32.getValue(), settings.numberOfShards()));
        return new ShardId(indexName, shard);
    }

    @Override
    public List<ShardRouting> searchShards(String indexName, ClusterState state) {
        IndexSettings settings = state.indices().get(indexName);
        if (settings == null) {
            throw new IllegalArgumentException("index not found: " + indexName);
        }
        Map<ShardId, List<ShardRouting>> liveSearchableCopies = state.routingTable().stream()
                .filter(routing -> routing.shardId().indexName().equals(indexName))
                .filter(ShardRouting::searchable)
                .collect(java.util.stream.Collectors.groupingBy(ShardRouting::shardId));
        List<ShardRouting> selected = new ArrayList<>(settings.numberOfShards());
        for (int shard = 0; shard < settings.numberOfShards(); shard++) {
            ShardId shardId = new ShardId(indexName, shard);
            selected.add(selectCopy(shardId, liveSearchableCopies.getOrDefault(shardId, List.of())));
        }
        return selected;
    }

    @Override
    public ShardRouting searchShard(ShardId shardId, ClusterState state) {
        if (!state.indices().containsKey(shardId.indexName())) {
            throw new IllegalArgumentException("index not found: " + shardId.indexName());
        }
        List<ShardRouting> copies = state.routingTable().stream()
                .filter(routing -> routing.shardId().equals(shardId))
                .filter(ShardRouting::searchable)
                .toList();
        return selectCopy(shardId, copies);
    }

    private ShardRouting selectCopy(ShardId shardId, List<ShardRouting> copies) {
        return copies.stream()
                .min(Comparator.comparing(ShardRouting::nodeId))
                .orElseThrow(() -> new IllegalStateException(
                        "no searchable shard copy available: " + shardId.routeKey()
                ));
    }
}
