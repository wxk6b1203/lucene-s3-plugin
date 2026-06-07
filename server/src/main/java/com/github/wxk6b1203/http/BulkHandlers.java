package com.github.wxk6b1203.http;

import com.github.wxk6b1203.index.IndexDocumentResponse;
import com.github.wxk6b1203.util.JsonUtil;
import io.vertx.ext.web.RoutingContext;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.github.wxk6b1203.http.HttpApiRequestParsing.bodyAsString;
import static com.github.wxk6b1203.http.HttpApiRequestParsing.intObject;
import static com.github.wxk6b1203.http.HttpApiRequestParsing.mapValue;
import static com.github.wxk6b1203.http.HttpApiRequestParsing.objectList;
import static com.github.wxk6b1203.http.HttpApiRequestParsing.stringValue;

final class BulkHandlers {
    private BulkHandlers() {
    }

    static List<BulkItemRequest> bulkItems(RoutingContext context) {
        if (HttpApiProtobuf.isProtobufRequest(context)) {
            return HttpApiProtobuf.bulkItems(context, context.pathParam("index"));
        }
        String body = bodyAsString(context);
        if (body == null || body.isBlank()) {
            return List.of();
        }
        String defaultIndex = context.pathParam("index");
        List<BulkItemRequest> items = new ArrayList<>();
        String[] lines = body.split("\\r?\\n");
        for (int line = 0; line < lines.length; line++) {
            String actionLine = lines[line].trim();
            if (actionLine.isEmpty()) {
                continue;
            }
            Map<String, Object> action = JsonUtil.readValueAsMap(actionLine);
            if (action.size() != 1) {
                throw new IllegalArgumentException("bulk action line must contain exactly one action at line " + (line + 1));
            }
            String actionName = action.keySet().iterator().next();
            if (!actionName.equals("index") && !actionName.equals("create") && !actionName.equals("delete")) {
                throw new IllegalArgumentException("unsupported bulk action: " + actionName);
            }
            Map<String, Object> metadata = mapValue(action.get(actionName));
            String index = stringValue(metadata.get("_index"));
            if (index == null || index.isBlank()) {
                index = defaultIndex;
            }
            if (index == null || index.isBlank()) {
                throw new IllegalArgumentException("bulk action requires _index at line " + (line + 1));
            }
            String id = stringValue(metadata.get("_id"));
            String routing = stringValue(metadata.get("routing"));
            if (routing == null || routing.isBlank()) {
                routing = stringValue(metadata.get("_routing"));
            }
            Map<String, Object> source = Map.of();
            if (!actionName.equals("delete")) {
                if (++line >= lines.length || lines[line].isBlank()) {
                    throw new IllegalArgumentException("bulk action requires source after line " + line);
                }
                source = JsonUtil.readValueAsMap(lines[line]);
            }
            items.add(new BulkItemRequest(actionName, index, id, routing, source));
        }
        return items;
    }

    static BulkItemRequest internalBulkItem(Map<String, Object> value, String defaultIndex) {
        String action = stringValue(value.get("action"));
        if (!"index".equals(action) && !"create".equals(action) && !"delete".equals(action)) {
            throw new IllegalArgumentException("unsupported internal bulk action: " + action);
        }
        String index = stringValue(value.get("index"));
        if (index == null || index.isBlank()) {
            index = defaultIndex;
        }
        if (!defaultIndex.equals(index)) {
            throw new IllegalArgumentException("internal bulk item index does not match target shard: " + index);
        }
        String id = stringValue(value.get("id"));
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("internal bulk item requires id");
        }
        String routing = stringValue(value.get("routing"));
        if (routing == null || routing.isBlank()) {
            routing = stringValue(value.get("_routing"));
        }
        return new BulkItemRequest(action, index, id, routing, mapValue(value.get("source")));
    }

    static Map<String, Object> internalBulkBody(BulkShardBatchKey key, List<BulkItemPlan> plans) {
        return Map.of(
                "owner_term", key.ownerTerm(),
                "allocation_epoch", key.allocationEpoch(),
                "items", plans.stream().map(plan -> {
                    Map<String, Object> item = new LinkedHashMap<>();
                    item.put("action", plan.item().action());
                    item.put("index", plan.item().index());
                    item.put("id", plan.id());
                    if (plan.item().routing() != null && !plan.item().routing().isBlank()) {
                        item.put("routing", plan.item().routing());
                    }
                    item.put("source", plan.item().source());
                    return item;
                }).toList()
        );
    }

    static List<BulkItemResult> remoteBulkBatchResponse(
            BulkShardBatchKey key,
            List<BulkItemPlan> plans,
            int status,
            String body
    ) {
        if (status >= 300) {
            Exception exception = new IllegalStateException("remote shard bulk failed on " + key.nodeId()
                    + ": status=" + status
                    + ", body=" + (body == null ? "" : body));
            return plans.stream()
                    .map(plan -> new BulkItemResult(plan.ordinal(), bulkError(plan.item(), plan.id(), status, exception)))
                    .toList();
        }
        Map<String, Object> response = body == null || body.isBlank() ? Map.of() : JsonUtil.readValueAsMap(body);
        List<Object> items = objectList(response.get("items"));
        List<BulkItemResult> results = new ArrayList<>(plans.size());
        for (int i = 0; i < plans.size(); i++) {
            BulkItemPlan plan = plans.get(i);
            if (i >= items.size()) {
                results.add(new BulkItemResult(
                        plan.ordinal(),
                        bulkError(plan.item(), plan.id(), 502, new IllegalStateException("remote bulk response is missing item"))
                ));
                continue;
            }
            results.add(new BulkItemResult(plan.ordinal(), bulkResponseFromMap(plan, mapValue(items.get(i)))));
        }
        return results;
    }

    static BulkItemResponse bulkResponseFromMap(BulkItemPlan plan, Map<String, Object> response) {
        BulkItemRequest item = plan.item();
        Object actionBody = response.get(item.action());
        if (!(actionBody instanceof Map<?, ?>)) {
            return bulkError(
                    item,
                    plan.id(),
                    502,
                    new IllegalStateException("remote bulk response missing action item: " + item.action())
            );
        }
        Map<String, Object> body = mapValue(actionBody);
        Integer status = intObject(body.get("status"));
        if (status == null) {
            return bulkError(
                    item,
                    plan.id(),
                    502,
                    new IllegalStateException("remote bulk response missing item status: " + item.action())
            );
        }
        Map<String, Object> error = body.get("error") instanceof Map<?, ?> ? mapValue(body.get("error")) : null;
        if (status >= 300 && error == null) {
            error = Map.of(
                    "type", "RemoteBulkItemException",
                    "reason", "remote bulk item failed without error body"
            );
        }
        return new BulkItemResponse(
                item.action(),
                stringValueOrDefault(body.get("_index"), item.index()),
                stringValueOrDefault(body.get("_id"), plan.id()),
                status,
                stringValue(body.get("result")),
                body.get("shardId"),
                error
        );
    }

    static BulkItemResponse bulkSuccess(BulkItemRequest item, IndexDocumentResponse response, int status) {
        return new BulkItemResponse(
                item.action(),
                response.indexName(),
                response.id(),
                status,
                response.result(),
                response.shardId(),
                null
        );
    }

    static BulkItemResponse bulkError(BulkItemRequest item, String id, int status, Exception e) {
        return new BulkItemResponse(
                item.action(),
                item.index(),
                id,
                status,
                null,
                null,
                Map.of(
                        "type", e.getClass().getSimpleName(),
                        "reason", e.getMessage() == null ? "" : e.getMessage()
                )
        );
    }

    private static String stringValueOrDefault(Object value, String defaultValue) {
        String string = stringValue(value);
        return string == null || string.isBlank() ? defaultValue : string;
    }
}
