package com.github.wxk6b1203.http;

import com.github.wxk6b1203.search.SearchHit;
import com.github.wxk6b1203.search.SearchRequest;
import com.github.wxk6b1203.search.SearchResponse;
import com.github.wxk6b1203.search.VectorQuery;
import com.github.wxk6b1203.util.JsonUtil;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.vertx.ext.web.RoutingContext;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

final class HttpApiProtobuf {
    static final String MEDIA_TYPE = "application/x-protobuf";
    private static final Set<String> PROTOBUF_MEDIA_TYPES = Set.of(
            MEDIA_TYPE,
            "application/protobuf",
            "application/vnd.google.protobuf"
    );

    private HttpApiProtobuf() {
    }

    static boolean isProtobufRequest(RoutingContext context) {
        return containsProtobufMediaType(context.request().getHeader("content-type"));
    }

    static boolean wantsProtobuf(RoutingContext context) {
        String accept = context.request().getHeader("accept");
        if (containsProtobufMediaType(accept)) {
            return true;
        }
        if (accept != null && accept.toLowerCase().contains("application/json")) {
            return false;
        }
        return isProtobufRequest(context);
    }

    static Map<String, Object> bodyAsMap(RoutingContext context) {
        if (bodyBytes(context).length == 0) {
            return Map.of();
        }
        try {
            return toMap(Struct.parseFrom(bodyBytes(context)));
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("invalid protobuf request body", e);
        }
    }

    static SearchRequest searchRequest(RoutingContext context, String index, String readPreference) {
        try {
            com.github.wxk6b1203.http.proto.SearchRequest request =
                    com.github.wxk6b1203.http.proto.SearchRequest.parseFrom(bodyBytes(context));
            VectorQuery vector = vectorQuery(request);
            Map<String, Object> query = queryMap(request.getQuery());
            if (request.hasVector() && request.getVector().hasFilter()) {
                query = combineVectorFilter(query, queryMap(request.getVector().getFilter()));
            }
            return new SearchRequest(
                    index,
                    query,
                    aggregations(request.getAggregationsList()),
                    vector,
                    blankToNull(request.getRouting()),
                    request.getFrom(),
                    request.getSize() <= 0 ? (vector == null ? 10 : vector.k()) : request.getSize(),
                    sort(request.getSortList()),
                    request.getSearchAfterList().stream().map(HttpApiProtobuf::fromValue).toList(),
                    blankToNull(request.getPitId()),
                    Map.of(),
                    publicReadPreference(readPreference, request.getReadPreference())
            );
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("invalid protobuf search request", e);
        }
    }

    static List<BulkItemRequest> bulkItems(RoutingContext context, String defaultIndex) {
        try {
            com.github.wxk6b1203.http.proto.BulkRequest request =
                    com.github.wxk6b1203.http.proto.BulkRequest.parseFrom(bodyBytes(context));
            List<BulkItemRequest> items = new ArrayList<>(request.getItemsCount());
            for (com.github.wxk6b1203.http.proto.BulkItem item : request.getItemsList()) {
                String action = actionName(item.getAction());
                if (action == null) {
                    throw new IllegalArgumentException("unsupported bulk protobuf action: " + item.getAction());
                }
                String index = item.getIndex();
                if (index == null || index.isBlank()) {
                    index = defaultIndex;
                }
                if (index == null || index.isBlank()) {
                    throw new IllegalArgumentException("bulk protobuf item requires index");
                }
                Map<String, Object> source = Map.of();
                if (!"delete".equals(action)) {
                    if (!item.hasSource()) {
                        throw new IllegalArgumentException("bulk protobuf item requires source object");
                    }
                    source = toMap(item.getSource());
                }
                items.add(new BulkItemRequest(
                        action,
                        index,
                        blankToNull(item.getId()),
                        blankToNull(item.getRouting()),
                        source
                ));
            }
            return items;
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("invalid protobuf bulk request", e);
        }
    }

    static byte[] writeValueAsBytes(Object value) {
        if (value instanceof SearchResponse response) {
            return searchResponse(response).toByteArray();
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> normalized = normalizeMap(map);
            if (isBulkResponse(normalized)) {
                return bulkResponse(normalized).toByteArray();
            }
            if (isErrorResponse(normalized)) {
                return errorResponse(normalized).toByteArray();
            }
            return toStruct(normalized).toByteArray();
        }
        return toStruct(rootMap(value)).toByteArray();
    }

    static Struct toStruct(Map<String, Object> map) {
        Struct.Builder builder = Struct.newBuilder();
        map.forEach((key, value) -> builder.putFields(key, toValue(value)));
        return builder.build();
    }

    static Map<String, Object> toMap(Struct struct) {
        Map<String, Object> map = new LinkedHashMap<>();
        struct.getFieldsMap().forEach((key, value) -> map.put(key, fromValue(value)));
        return map;
    }

    static Map<String, Object> toMap(com.github.wxk6b1203.http.proto.SearchResponse response) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("tookMillis", response.getTookMillis());
        map.put("totalShards", response.getTotalShards());
        map.put("successfulShards", response.getSuccessfulShards());
        map.put("failedShards", response.getFailedShards());
        map.put("hits", response.getHitsList().stream().map(HttpApiProtobuf::hitMap).toList());
        map.put("aggregations", toMap(response.getAggregations()));
        map.put("shardFailures", new ArrayList<>(response.getShardFailuresList()));
        return map;
    }

    static Map<String, Object> toMap(com.github.wxk6b1203.http.proto.BulkResponse response) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("took", response.getTook());
        map.put("errors", response.getErrors());
        map.put("items", response.getItemsList().stream().map(HttpApiProtobuf::bulkItemMap).toList());
        return map;
    }

    static Map<String, Object> toMap(com.github.wxk6b1203.http.proto.ErrorResponse response) {
        return Map.of(
                "error", response.getError(),
                "type", response.getType(),
                "status", response.getStatus()
        );
    }

    private static com.github.wxk6b1203.http.proto.SearchResponse searchResponse(SearchResponse response) {
        return com.github.wxk6b1203.http.proto.SearchResponse.newBuilder()
                .setTookMillis(response.tookMillis())
                .setTotalShards(response.totalShards())
                .setSuccessfulShards(response.successfulShards())
                .setFailedShards(response.failedShards())
                .addAllHits(response.hits().stream().map(HttpApiProtobuf::searchHit).toList())
                .setAggregations(toStruct(response.aggregations()))
                .addAllShardFailures(response.shardFailures())
                .build();
    }

    private static com.github.wxk6b1203.http.proto.SearchHit searchHit(SearchHit hit) {
        return com.github.wxk6b1203.http.proto.SearchHit.newBuilder()
                .setIndexName(nullToBlank(hit.indexName()))
                .setId(nullToBlank(hit.id()))
                .setScore(hit.score())
                .setSource(toStruct(hit.source()))
                .addAllSortValues(hit.sortValues().stream().map(HttpApiProtobuf::toValue).toList())
                .build();
    }

    @SuppressWarnings("unchecked")
    private static com.github.wxk6b1203.http.proto.BulkResponse bulkResponse(Map<String, Object> response) {
        List<Object> items = response.get("items") instanceof List<?> list ? new ArrayList<>(list) : List.of();
        return com.github.wxk6b1203.http.proto.BulkResponse.newBuilder()
                .setTook(longValue(response.get("took")))
                .setErrors(Boolean.TRUE.equals(response.get("errors")))
                .addAllItems(items.stream()
                        .filter(Map.class::isInstance)
                        .map(item -> bulkItemResponse((Map<String, Object>) item))
                        .toList())
                .build();
    }

    private static com.github.wxk6b1203.http.proto.BulkItemResponse bulkItemResponse(Map<String, Object> item) {
        if (item.isEmpty()) {
            return com.github.wxk6b1203.http.proto.BulkItemResponse.getDefaultInstance();
        }
        String action = item.keySet().iterator().next();
        Map<String, Object> body = item.get(action) instanceof Map<?, ?> map ? normalizeMap(map) : Map.of();
        com.github.wxk6b1203.http.proto.BulkItemResponse.Builder builder =
                com.github.wxk6b1203.http.proto.BulkItemResponse.newBuilder()
                        .setAction(action(action))
                        .setIndex(nullToBlank(stringValue(body.get("_index"))))
                        .setId(nullToBlank(stringValue(body.get("_id"))))
                        .setStatus((int) longValue(body.get("status")))
                        .setResult(nullToBlank(stringValue(body.get("result"))));
        Object shardId = body.get("shardId");
        if (shardId instanceof Map<?, ?> map) {
            builder.setShardId(shardId(normalizeMap(map)));
        }
        Object error = body.get("error");
        if (error instanceof Map<?, ?> map) {
            builder.setError(error(normalizeMap(map)));
        }
        return builder.build();
    }

    private static com.github.wxk6b1203.http.proto.ShardId shardId(Map<String, Object> value) {
        return com.github.wxk6b1203.http.proto.ShardId.newBuilder()
                .setIndexName(nullToBlank(stringValue(value.get("indexName"))))
                .setShardNumber((int) longValue(value.get("shardNumber")))
                .build();
    }

    private static com.github.wxk6b1203.http.proto.Error error(Map<String, Object> value) {
        return com.github.wxk6b1203.http.proto.Error.newBuilder()
                .setType(nullToBlank(stringValue(value.get("type"))))
                .setReason(nullToBlank(stringValue(value.get("reason"))))
                .build();
    }

    private static com.github.wxk6b1203.http.proto.ErrorResponse errorResponse(Map<String, Object> value) {
        return com.github.wxk6b1203.http.proto.ErrorResponse.newBuilder()
                .setError(nullToBlank(stringValue(value.get("error"))))
                .setType(nullToBlank(stringValue(value.get("type"))))
                .setStatus((int) longValue(value.get("status")))
                .build();
    }

    private static Map<String, Object> hitMap(com.github.wxk6b1203.http.proto.SearchHit hit) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("indexName", hit.getIndexName());
        map.put("id", hit.getId());
        map.put("score", hit.getScore());
        map.put("source", toMap(hit.getSource()));
        map.put("sortValues", hit.getSortValuesList().stream().map(HttpApiProtobuf::fromValue).toList());
        return map;
    }

    private static Map<String, Object> bulkItemMap(com.github.wxk6b1203.http.proto.BulkItemResponse item) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("_index", item.getIndex());
        if (!item.getId().isBlank()) {
            body.put("_id", item.getId());
        }
        body.put("status", item.getStatus());
        if (!item.getResult().isBlank()) {
            body.put("result", item.getResult());
        }
        if (item.hasShardId()) {
            body.put("shardId", Map.of(
                    "indexName", item.getShardId().getIndexName(),
                    "shardNumber", item.getShardId().getShardNumber()
            ));
        }
        if (item.hasError()) {
            body.put("error", Map.of(
                    "type", item.getError().getType(),
                    "reason", item.getError().getReason()
            ));
        }
        return Map.of(actionName(item.getAction()), body);
    }

    private static Map<String, Object> queryMap(com.github.wxk6b1203.http.proto.Query query) {
        return switch (query.getKindCase()) {
            case MATCH_ALL, KIND_NOT_SET -> Map.of("match_all", Map.of());
            case IDS -> Map.of("ids", Map.of("values", new ArrayList<>(query.getIds().getValuesList())));
            case TERM -> Map.of("term", Map.of(query.getTerm().getField(), fromValue(query.getTerm().getValue())));
            case TERMS -> Map.of("terms", Map.of(
                    query.getTerms().getField(),
                    query.getTerms().getValuesList().stream().map(HttpApiProtobuf::fromValue).toList()
            ));
            case PREFIX -> Map.of("prefix", Map.of(query.getPrefix().getField(), query.getPrefix().getValue()));
            case EXISTS -> Map.of("exists", Map.of("field", query.getExists().getField()));
            case MATCH -> Map.of("match", Map.of(query.getMatch().getField(), fromValue(query.getMatch().getValue())));
            case RANGE -> Map.of("range", Map.of(query.getRange().getField(), rangeBounds(query.getRange())));
            case BOOL -> Map.of("bool", boolMap(query.getBool()));
            case RAW -> toMap(query.getRaw());
        };
    }

    private static Map<String, Object> rangeBounds(com.github.wxk6b1203.http.proto.RangeQuery range) {
        Map<String, Object> bounds = new LinkedHashMap<>();
        if (range.hasGt()) {
            bounds.put("gt", fromValue(range.getGt()));
        }
        if (range.hasGte()) {
            bounds.put("gte", fromValue(range.getGte()));
        }
        if (range.hasLt()) {
            bounds.put("lt", fromValue(range.getLt()));
        }
        if (range.hasLte()) {
            bounds.put("lte", fromValue(range.getLte()));
        }
        if (!range.getRelation().isBlank()) {
            bounds.put("relation", range.getRelation());
        }
        return bounds;
    }

    private static Map<String, Object> boolMap(com.github.wxk6b1203.http.proto.BoolQuery bool) {
        Map<String, Object> map = new LinkedHashMap<>();
        if (bool.getFilterCount() > 0) {
            map.put("filter", bool.getFilterList().stream().map(HttpApiProtobuf::queryMap).toList());
        }
        if (bool.getMustCount() > 0) {
            map.put("must", bool.getMustList().stream().map(HttpApiProtobuf::queryMap).toList());
        }
        if (bool.getShouldCount() > 0) {
            map.put("should", bool.getShouldList().stream().map(HttpApiProtobuf::queryMap).toList());
        }
        if (bool.getMustNotCount() > 0) {
            map.put("must_not", bool.getMustNotList().stream().map(HttpApiProtobuf::queryMap).toList());
        }
        return map;
    }

    private static Map<String, Object> combineVectorFilter(Map<String, Object> query, Map<String, Object> filter) {
        if (isMatchAll(query)) {
            return filter;
        }
        if (isMatchAll(filter)) {
            return query;
        }
        return Map.of("bool", Map.of("filter", List.of(query, filter)));
    }

    private static boolean isMatchAll(Map<String, Object> query) {
        return query.isEmpty() || query.size() == 1 && query.containsKey("match_all");
    }

    private static String publicReadPreference(String queryPreference, String bodyPreference) {
        String preference = queryPreference == null || queryPreference.isBlank()
                ? blankToNull(bodyPreference)
                : queryPreference;
        if (preference == null || preference.isBlank()) {
            return "weak";
        }
        String normalized = preference.trim().toLowerCase(Locale.ROOT);
        if (!normalized.equals("weak") && !normalized.equals("strong")) {
            throw new IllegalArgumentException("read_preference must be weak or strong");
        }
        return normalized;
    }

    private static VectorQuery vectorQuery(com.github.wxk6b1203.http.proto.SearchRequest request) {
        if (!request.hasVector()) {
            return null;
        }
        com.github.wxk6b1203.http.proto.VectorQuery vector = request.getVector();
        return new VectorQuery(
                vector.getField(),
                vector.getVectorList(),
                vector.getK(),
                vector.getNumCandidates(),
                vector.getMinScore() <= 0 ? null : vector.getMinScore()
        );
    }

    private static List<Map<String, Object>> aggregations(List<com.github.wxk6b1203.http.proto.Aggregation> aggregations) {
        return aggregations.stream().map(HttpApiProtobuf::aggregation).toList();
    }

    private static Map<String, Object> aggregation(com.github.wxk6b1203.http.proto.Aggregation aggregation) {
        Map<String, Object> map = new LinkedHashMap<>();
        if (!aggregation.getName().isBlank()) {
            map.put("name", aggregation.getName());
        }
        switch (aggregation.getKindCase()) {
            case TERMS -> map.put("terms", termsAggregation(aggregation.getTerms()));
            case RANGE -> map.put("range", rangeAggregation(aggregation.getRange()));
            case MIN -> map.put("min", metricAggregation(aggregation.getMin()));
            case MAX -> map.put("max", metricAggregation(aggregation.getMax()));
            case SUM -> map.put("sum", metricAggregation(aggregation.getSum()));
            case AVG -> map.put("avg", metricAggregation(aggregation.getAvg()));
            case VALUE_COUNT -> map.put("value_count", metricAggregation(aggregation.getValueCount()));
            case RAW -> map.putAll(toMap(aggregation.getRaw()));
            case KIND_NOT_SET -> {
            }
        }
        return map;
    }

    private static Map<String, Object> termsAggregation(com.github.wxk6b1203.http.proto.TermsAggregation aggregation) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("field", aggregation.getField());
        if (aggregation.getSize() > 0) {
            map.put("size", aggregation.getSize());
        }
        if (aggregation.getMinDocCount() > 0) {
            map.put("min_doc_count", aggregation.getMinDocCount());
        }
        if (aggregation.hasMissing()) {
            map.put("missing", fromValue(aggregation.getMissing()));
        }
        if (!aggregation.getOrderBy().isBlank()) {
            map.put("order", Map.of(
                    aggregation.getOrderBy(),
                    aggregation.getOrder().isBlank() ? "desc" : aggregation.getOrder()
            ));
        }
        return map;
    }

    private static Map<String, Object> rangeAggregation(com.github.wxk6b1203.http.proto.RangeAggregation aggregation) {
        return Map.of(
                "field", aggregation.getField(),
                "ranges", aggregation.getRangesList().stream().map(HttpApiProtobuf::rangeBucket).toList()
        );
    }

    private static Map<String, Object> rangeBucket(com.github.wxk6b1203.http.proto.RangeBucket bucket) {
        Map<String, Object> map = new LinkedHashMap<>();
        if (!bucket.getKey().isBlank()) {
            map.put("key", bucket.getKey());
        }
        if (bucket.hasFrom()) {
            map.put("from", fromValue(bucket.getFrom()));
        }
        if (bucket.hasTo()) {
            map.put("to", fromValue(bucket.getTo()));
        }
        return map;
    }

    private static Map<String, Object> metricAggregation(com.github.wxk6b1203.http.proto.MetricAggregation aggregation) {
        return Map.of("field", aggregation.getField());
    }

    private static List<Map<String, Object>> sort(List<com.github.wxk6b1203.http.proto.SortField> sort) {
        return sort.stream()
                .map(item -> {
                    Map<String, Object> spec = new LinkedHashMap<>();
                    spec.put(item.getField(), Map.of("order", item.getOrder().isBlank() ? "asc" : item.getOrder()));
                    return spec;
                })
                .toList();
    }

    private static Map<String, Object> rootMap(Object value) {
        Object normalized = normalizeValue(value);
        if (normalized instanceof Map<?, ?> map) {
            return normalizeMap(map);
        }
        throw new IllegalArgumentException("protobuf response root must be an object");
    }

    private static Value toValue(Object value) {
        value = normalizeValue(value);
        Value.Builder builder = Value.newBuilder();
        if (value == null) {
            return builder.setNullValue(NullValue.NULL_VALUE).build();
        }
        if (value instanceof Map<?, ?> map) {
            return builder.setStructValue(toStruct(normalizeMap(map))).build();
        }
        if (value instanceof Iterable<?> iterable) {
            ListValue.Builder list = ListValue.newBuilder();
            for (Object item : iterable) {
                list.addValues(toValue(item));
            }
            return builder.setListValue(list).build();
        }
        if (value.getClass().isArray()) {
            ListValue.Builder list = ListValue.newBuilder();
            int length = java.lang.reflect.Array.getLength(value);
            for (int i = 0; i < length; i++) {
                list.addValues(toValue(java.lang.reflect.Array.get(value, i)));
            }
            return builder.setListValue(list).build();
        }
        if (value instanceof Boolean bool) {
            return builder.setBoolValue(bool).build();
        }
        if (value instanceof Number number) {
            return builder.setNumberValue(number.doubleValue()).build();
        }
        return builder.setStringValue(String.valueOf(value)).build();
    }

    private static Map<String, Object> normalizeMap(Map<?, ?> map) {
        Map<String, Object> normalized = new LinkedHashMap<>();
        map.forEach((key, value) -> normalized.put(String.valueOf(key), normalizeValue(value)));
        return normalized;
    }

    private static Object normalizeValue(Object value) {
        if (value == null
                || value instanceof String
                || value instanceof Boolean
                || value instanceof Number) {
            return value;
        }
        if (value instanceof Map<?, ?> map) {
            return normalizeMap(map);
        }
        if (value instanceof Iterable<?> iterable) {
            List<Object> normalized = new ArrayList<>();
            for (Object item : iterable) {
                normalized.add(normalizeValue(item));
            }
            return normalized;
        }
        if (value.getClass().isArray()) {
            List<Object> normalized = new ArrayList<>();
            int length = java.lang.reflect.Array.getLength(value);
            for (int i = 0; i < length; i++) {
                normalized.add(normalizeValue(java.lang.reflect.Array.get(value, i)));
            }
            return normalized;
        }
        return normalizeValue(JsonUtil.readValue(JsonUtil.writeValueAsBytes(value), Object.class));
    }

    private static Object fromValue(Value value) {
        return switch (value.getKindCase()) {
            case NULL_VALUE, KIND_NOT_SET -> null;
            case NUMBER_VALUE -> value.getNumberValue();
            case STRING_VALUE -> value.getStringValue();
            case BOOL_VALUE -> value.getBoolValue();
            case STRUCT_VALUE -> toMap(value.getStructValue());
            case LIST_VALUE -> {
                List<Object> list = new ArrayList<>(value.getListValue().getValuesCount());
                for (Value item : value.getListValue().getValuesList()) {
                    list.add(fromValue(item));
                }
                yield list;
            }
        };
    }

    private static boolean isBulkResponse(Map<String, Object> value) {
        return value.containsKey("items") && value.containsKey("errors") && value.containsKey("took");
    }

    private static boolean isErrorResponse(Map<String, Object> value) {
        return value.containsKey("error") && value.containsKey("type") && value.containsKey("status");
    }

    private static byte[] bodyBytes(RoutingContext context) {
        if (context.body() == null || context.body().buffer() == null) {
            return new byte[0];
        }
        return context.body().buffer().getBytes();
    }

    private static String actionName(com.github.wxk6b1203.http.proto.BulkAction action) {
        return switch (action) {
            case BULK_ACTION_INDEX -> "index";
            case BULK_ACTION_CREATE -> "create";
            case BULK_ACTION_DELETE -> "delete";
            default -> null;
        };
    }

    private static com.github.wxk6b1203.http.proto.BulkAction action(String action) {
        return switch (action) {
            case "index" -> com.github.wxk6b1203.http.proto.BulkAction.BULK_ACTION_INDEX;
            case "create" -> com.github.wxk6b1203.http.proto.BulkAction.BULK_ACTION_CREATE;
            case "delete" -> com.github.wxk6b1203.http.proto.BulkAction.BULK_ACTION_DELETE;
            default -> com.github.wxk6b1203.http.proto.BulkAction.BULK_ACTION_UNSPECIFIED;
        };
    }

    private static String stringValue(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private static long longValue(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value instanceof String string && !string.isBlank()) {
            try {
                return Long.parseLong(string);
            } catch (NumberFormatException ignored) {
                return 0;
            }
        }
        return 0;
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private static String nullToBlank(String value) {
        return value == null ? "" : value;
    }

    private static boolean containsProtobufMediaType(String header) {
        if (header == null || header.isBlank()) {
            return false;
        }
        String normalized = header.toLowerCase();
        for (String mediaType : PROTOBUF_MEDIA_TYPES) {
            if (normalized.contains(mediaType)) {
                return true;
            }
        }
        return false;
    }
}
