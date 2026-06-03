package com.github.wxk6b1203.http;

import com.github.wxk6b1203.cluster.FieldMapping;
import com.github.wxk6b1203.cluster.ShardId;
import com.github.wxk6b1203.cluster.WriteRoute;

import java.util.LinkedHashMap;
import java.util.Map;

record BulkItemRequest(
        String action,
        String index,
        String id,
        String routing,
        Map<String, Object> source
) {
}

record BulkItemPlan(
        int ordinal,
        BulkItemRequest item,
        String id,
        WriteRoute route,
        Map<String, FieldMapping> mappings
) {
}

record BulkItemResult(int ordinal, BulkItemResponse response) {
}

record BulkShardBatchKey(
        String nodeId,
        String host,
        int httpPort,
        ShardId shardId,
        long ownerTerm,
        long allocationEpoch
) {
}

record BulkItemResponse(
        String action,
        String index,
        String id,
        int status,
        String result,
        Object shardId,
        Map<String, Object> error
) {
    boolean failed() {
        return error != null || status >= 300;
    }

    Map<String, Object> asMap() {
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("_index", index);
        if (id != null) {
            item.put("_id", id);
        }
        item.put("status", status);
        if (result != null) {
            item.put("result", result);
        }
        if (shardId != null) {
            item.put("shardId", shardId);
        }
        if (error != null) {
            item.put("error", error);
        }
        return Map.of(action, item);
    }
}
