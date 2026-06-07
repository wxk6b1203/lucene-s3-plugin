package com.github.wxk6b1203.http;

import com.github.wxk6b1203.cluster.FieldMapping;
import com.github.wxk6b1203.search.VectorQuery;
import com.github.wxk6b1203.util.JsonUtil;
import io.vertx.ext.web.RoutingContext;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

final class HttpApiRequestParsing {
    private HttpApiRequestParsing() {
    }

    static Map<String, FieldMapping> parseMappings(Map<String, Object> body) {
        Object mappingsValue = body.get("mappings");
        Map<String, Object> mappings = mappingsValue instanceof Map<?, ?>
                ? mapValue(mappingsValue)
                : Map.of();
        Map<String, Object> properties = mappings.get("properties") instanceof Map<?, ?>
                ? mapValue(mappings.get("properties"))
                : mappings;
        if (properties.isEmpty() && body.get("properties") instanceof Map<?, ?>) {
            properties = mapValue(body.get("properties"));
        }
        if (properties.isEmpty()) {
            return Map.of();
        }
        Map<String, FieldMapping> result = new HashMap<>();
        properties.forEach((field, specValue) -> {
            if (specValue instanceof Map<?, ?> spec) {
                Map<String, Object> fieldSpec = mapValue(spec);
                result.put(field, new FieldMapping(
                        stringValue(fieldSpec.get("type")),
                        intObject(fieldSpec.containsKey("dimension") ? fieldSpec.get("dimension") : fieldSpec.get("dims")),
                        stringValue(fieldSpec.get("similarity")),
                        booleanObject(fieldSpec.containsKey("indexed") ? fieldSpec.get("indexed") : fieldSpec.get("index")),
                        booleanObject(fieldSpec.containsKey("stored") ? fieldSpec.get("stored") : fieldSpec.get("store")),
                        booleanObject(fieldSpec.containsKey("doc_values") ? fieldSpec.get("doc_values") : fieldSpec.get("docValues")),
                        booleanObject(fieldSpec.containsKey("multi_valued") ? fieldSpec.get("multi_valued") : fieldSpec.get("multiValued")),
                        stringValue(fieldSpec.get("analyzer")),
                        stringValue(fieldSpec.containsKey("search_analyzer")
                                ? fieldSpec.get("search_analyzer")
                                : fieldSpec.get("searchAnalyzer"))
                ));
            }
        });
        return result;
    }

    static Map<String, Object> bodyAsMap(RoutingContext context) {
        if (HttpApiProtobuf.isProtobufRequest(context)) {
            return HttpApiProtobuf.bodyAsMap(context);
        }
        String body = bodyAsString(context);
        if (body == null || body.isBlank()) {
            return Map.of();
        }
        return JsonUtil.readValueAsMap(body);
    }

    static String bodyAsString(RoutingContext context) {
        return context.body() == null ? null : context.body().asString();
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> mapValue(Object value) {
        return value instanceof Map<?, ?> map ? (Map<String, Object>) map : Map.of();
    }

    @SuppressWarnings("unchecked")
    static List<Map<String, Object>> listOfMaps(Object value) {
        if (value instanceof List<?> list) {
            return (List<Map<String, Object>>) list;
        }
        if (value instanceof Map<?, ?> map) {
            List<Map<String, Object>> aggregations = new ArrayList<>();
            map.forEach((name, spec) -> {
                if (spec instanceof Map<?, ?> specMap) {
                    Map<String, Object> aggregation = new HashMap<>((Map<String, Object>) specMap);
                    aggregation.put("name", String.valueOf(name));
                    aggregations.add(aggregation);
                }
            });
            return aggregations;
        }
        return List.of();
    }

    @SuppressWarnings("unchecked")
    static List<Map<String, Object>> sortList(Object value) {
        if (value instanceof List<?> list) {
            List<Map<String, Object>> sorts = new ArrayList<>();
            for (Object item : list) {
                if (item instanceof Map<?, ?> map) {
                    sorts.add((Map<String, Object>) map);
                } else if (item instanceof String field) {
                    sorts.add(Map.of(field, Map.of()));
                }
            }
            return sorts;
        }
        if (value instanceof Map<?, ?> map) {
            return List.of((Map<String, Object>) map);
        }
        if (value instanceof String field) {
            return List.of(Map.of(field, Map.of()));
        }
        return List.of();
    }

    static List<Object> objectList(Object value) {
        return value instanceof List<?> list ? new ArrayList<>(list) : List.of();
    }

    static List<Float> floatList(Object value) {
        if (!(value instanceof List<?> list)) {
            return List.of();
        }
        return list.stream()
                .filter(Number.class::isInstance)
                .map(Number.class::cast)
                .map(Number::floatValue)
                .toList();
    }

    static Duration keepAlive(RoutingContext context) {
        String value = context.queryParams().get("keep_alive");
        if (value == null || value.isBlank()) {
            value = stringValue(bodyAsMap(context).get("keep_alive"));
        }
        if (value == null || value.isBlank()) {
            return Duration.ofMinutes(1);
        }
        value = value.trim().toLowerCase(Locale.ROOT);
        try {
            if (value.endsWith("ms")) {
                return Duration.ofMillis(Long.parseLong(value.substring(0, value.length() - 2)));
            }
            if (value.endsWith("s")) {
                return Duration.ofSeconds(Long.parseLong(value.substring(0, value.length() - 1)));
            }
            if (value.endsWith("m")) {
                return Duration.ofMinutes(Long.parseLong(value.substring(0, value.length() - 1)));
            }
            if (value.endsWith("h")) {
                return Duration.ofHours(Long.parseLong(value.substring(0, value.length() - 1)));
            }
            if (value.endsWith("d")) {
                return Duration.ofDays(Long.parseLong(value.substring(0, value.length() - 1)));
            }
            return Duration.ofMillis(Long.parseLong(value));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid keep_alive: " + value, e);
        }
    }

    static long durationMillis(Object value) {
        if (value == null) {
            return 0L;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        String text = String.valueOf(value).trim().toLowerCase(Locale.ROOT);
        try {
            if (text.endsWith("ms")) {
                return Long.parseLong(text.substring(0, text.length() - 2));
            }
            if (text.endsWith("s")) {
                return Duration.ofSeconds(Long.parseLong(text.substring(0, text.length() - 1))).toMillis();
            }
            if (text.endsWith("m")) {
                return Duration.ofMinutes(Long.parseLong(text.substring(0, text.length() - 1))).toMillis();
            }
            if (text.endsWith("h")) {
                return Duration.ofHours(Long.parseLong(text.substring(0, text.length() - 1))).toMillis();
            }
            if (text.endsWith("d")) {
                return Duration.ofDays(Long.parseLong(text.substring(0, text.length() - 1))).toMillis();
            }
            return Long.parseLong(text);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid duration: " + value, e);
        }
    }

    static int intValue(Object value, int defaultValue) {
        return value instanceof Number number ? number.intValue() : defaultValue;
    }

    static long longValue(Object value, long defaultValue) {
        return value instanceof Number number ? number.longValue() : defaultValue;
    }

    static Integer intObject(Object value) {
        return value instanceof Number number ? number.intValue() : null;
    }

    static Long longObject(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value instanceof String text && !text.isBlank()) {
            return Long.parseLong(text);
        }
        return null;
    }

    static Float floatObject(Object value) {
        return value instanceof Number number ? number.floatValue() : null;
    }

    static Boolean booleanObject(Object value) {
        return value instanceof Boolean bool ? bool : null;
    }

    static String stringValue(Object value) {
        return value == null ? null : value.toString();
    }

    static VectorQuery vectorFromBody(Map<String, Object> body) {
        if (body.get("vector") instanceof Map<?, ?>) {
            return vectorQuery(mapValue(body.get("vector")));
        }
        return body.get("knn") instanceof Map<?, ?>
                ? vectorQuery(mapValue(body.get("knn")))
                : null;
    }

    static VectorQuery vectorQuery(Map<String, Object> knn) {
        return new VectorQuery(
                stringValue(knn.get("field")),
                floatList(knn.get("query_vector")),
                intValue(knn.get("k"), 10),
                intValue(knn.get("num_candidates"), Math.max(10, intValue(knn.get("k"), 10))),
                floatObject(knn.containsKey("min_score") ? knn.get("min_score") : knn.get("similarity"))
        );
    }

    static Map<String, Object> searchQuery(Map<String, Object> body) {
        Map<String, Object> query = queryObject(body.get("query"));
        Map<String, Object> knn = mapValue(body.get("knn"));
        Map<String, Object> knnFilter = queryObject(knn.get("filter"));
        return combineFilters(query, knnFilter);
    }

    static Map<String, Object> knnSearchFilter(Map<String, Object> body, Map<String, Object> knn) {
        return combineFilters(queryObject(body.get("filter")), queryObject(knn.get("filter")));
    }

    static Map<String, Object> combineFilters(Map<String, Object> left, Map<String, Object> right) {
        if (left.isEmpty()) {
            return right;
        }
        if (right.isEmpty()) {
            return left;
        }
        return Map.of("bool", Map.of("filter", List.of(left, right)));
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> queryObject(Object value) {
        if (value instanceof Map<?, ?> map) {
            return (Map<String, Object>) map;
        }
        if (value instanceof List<?> list && !list.isEmpty()) {
            return Map.of("bool", Map.of("filter", list.stream()
                    .filter(Map.class::isInstance)
                    .map(Map.class::cast)
                    .toList()));
        }
        return Map.of();
    }

    static String pitId(Map<String, Object> body) {
        if (body.get("pit") instanceof Map<?, ?> pit) {
            return stringValue(mapValue(pit).get("id"));
        }
        return stringValue(body.get("pit_id"));
    }
}
