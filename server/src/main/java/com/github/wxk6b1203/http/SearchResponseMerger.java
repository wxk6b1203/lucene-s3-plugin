package com.github.wxk6b1203.http;

import com.github.wxk6b1203.search.SearchHit;
import com.github.wxk6b1203.search.SearchRequest;
import com.github.wxk6b1203.search.SearchResponse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class SearchResponseMerger {
    private SearchResponseMerger() {
    }

    static SearchResponse mergeSearchResponses(List<SearchResponse> responses, SearchRequest request, long started) {
        List<String> failures = responses.stream()
                .flatMap(response -> response.shardFailures().stream())
                .toList();
        List<SearchHit> hits = responses.stream()
                .flatMap(response -> response.hits().stream())
                .sorted((left, right) -> compareHits(left, right, request.sort()))
                .toList();
        int from = Math.min(request.searchAfter().isEmpty() ? Math.max(0, request.from()) : 0, hits.size());
        int to = Math.min(from + Math.max(0, request.size()), hits.size());
        return new SearchResponse(
                elapsedMillis(started),
                responses.stream().mapToInt(SearchResponse::totalShards).sum(),
                responses.stream().mapToInt(SearchResponse::successfulShards).sum(),
                responses.stream().mapToInt(SearchResponse::failedShards).sum(),
                hits.subList(from, to),
                mergeAggregations(responses),
                failures
        );
    }

    private static long elapsedMillis(long started) {
        long elapsedNanos = Math.max(0, System.nanoTime() - started);
        return elapsedNanos == 0 ? 0 : Math.max(1, (elapsedNanos + 999_999) / 1_000_000);
    }

    private static int compareHits(SearchHit left, SearchHit right, List<Map<String, Object>> sort) {
        if (sort == null || sort.isEmpty()) {
            return Float.compare(right.score(), left.score());
        }
        for (int i = 0; i < sort.size(); i++) {
            Object leftValue = i < left.sortValues().size() ? left.sortValues().get(i) : null;
            Object rightValue = i < right.sortValues().size() ? right.sortValues().get(i) : null;
            int compared = compareValues(leftValue, rightValue);
            if (compared != 0) {
                return sortDescending(sort.get(i)) ? -compared : compared;
            }
        }
        int byScore = Float.compare(right.score(), left.score());
        return byScore != 0 ? byScore : left.id().compareTo(right.id());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static int compareValues(Object left, Object right) {
        if (left == null && right == null) {
            return 0;
        }
        if (left == null) {
            return 1;
        }
        if (right == null) {
            return -1;
        }
        if (left instanceof Number leftNumber && right instanceof Number rightNumber) {
            return Double.compare(leftNumber.doubleValue(), rightNumber.doubleValue());
        }
        if (left instanceof Comparable comparable && left.getClass().isInstance(right)) {
            return comparable.compareTo(right);
        }
        return String.valueOf(left).compareTo(String.valueOf(right));
    }

    private static String sortField(Map<String, Object> spec) {
        return spec.keySet().stream().findFirst().orElse("_score");
    }

    private static boolean sortDescending(Map<String, Object> spec) {
        String field = sortField(spec);
        Object value = spec.get(field);
        if (value instanceof Map<?, ?> map) {
            Object order = map.get("order");
            return order == null ? "_score".equals(field) : "desc".equalsIgnoreCase(String.valueOf(order));
        }
        return value == null ? "_score".equals(field) : "desc".equalsIgnoreCase(String.valueOf(value));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> mergeAggregations(List<SearchResponse> responses) {
        Map<String, List<Map<String, Object>>> grouped = new HashMap<>();
        for (SearchResponse response : responses) {
            response.aggregations().forEach((name, value) -> {
                if (value instanceof Map<?, ?> map) {
                    grouped.computeIfAbsent(name, ignored -> new java.util.ArrayList<>()).add((Map<String, Object>) map);
                }
            });
        }
        if (grouped.isEmpty()) {
            return Map.of();
        }
        Map<String, Object> merged = new HashMap<>();
        grouped.forEach((name, shards) -> merged.put(name, mergeAggregation(shards)));
        return merged;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> mergeAggregation(List<Map<String, Object>> shards) {
        String type = stringValue(shards.getFirst().get("type"));
        if ("terms".equals(type)) {
            Map<String, Long> counts = new HashMap<>();
            String field = stringValue(shards.getFirst().get("field"));
            for (Map<String, Object> shard : shards) {
                Object bucketsValue = shard.get("buckets");
                if (bucketsValue instanceof List<?> buckets) {
                    for (Object bucketValue : buckets) {
                        if (bucketValue instanceof Map<?, ?> bucket) {
                            Object key = bucket.get("key");
                            Object docCount = bucket.get("doc_count");
                            if (key != null && docCount instanceof Number number) {
                                counts.merge(String.valueOf(key), number.longValue(), Long::sum);
                            }
                        }
                    }
                }
            }
            List<Map<String, Object>> buckets = counts.entrySet().stream()
                    .sorted((left, right) -> {
                        int byCount = Long.compare(right.getValue(), left.getValue());
                        return byCount != 0 ? byCount : left.getKey().compareTo(right.getKey());
                    })
                    .map(entry -> Map.<String, Object>of("key", entry.getKey(), "doc_count", entry.getValue()))
                    .toList();
            return Map.of("type", "terms", "field", field, "buckets", buckets);
        }
        String field = stringValue(shards.getFirst().get("field"));
        long count = shards.stream()
                .map(shard -> shard.get("count"))
                .filter(Number.class::isInstance)
                .map(Number.class::cast)
                .mapToLong(Number::longValue)
                .sum();
        Object value = switch (type) {
            case "min" -> shards.stream()
                    .map(shard -> shard.get("value"))
                    .filter(Number.class::isInstance)
                    .map(Number.class::cast)
                    .mapToDouble(Number::doubleValue)
                    .min()
                    .stream()
                    .boxed()
                    .findFirst()
                    .orElse(null);
            case "max" -> shards.stream()
                    .map(shard -> shard.get("value"))
                    .filter(Number.class::isInstance)
                    .map(Number.class::cast)
                    .mapToDouble(Number::doubleValue)
                    .max()
                    .stream()
                    .boxed()
                    .findFirst()
                    .orElse(null);
            case "sum" -> shards.stream()
                    .map(shard -> shard.get("value"))
                    .filter(Number.class::isInstance)
                    .map(Number.class::cast)
                    .mapToDouble(Number::doubleValue)
                    .sum();
            case "avg" -> {
                double sum = shards.stream()
                        .map(shard -> shard.get("sum"))
                        .filter(Number.class::isInstance)
                        .map(Number.class::cast)
                        .mapToDouble(Number::doubleValue)
                        .sum();
                yield count == 0 ? null : sum / count;
            }
            case "value_count" -> count;
            default -> null;
        };
        Map<String, Object> result = new HashMap<>();
        result.put("type", type);
        result.put("field", field);
        result.put("value", value);
        result.put("count", count);
        if ("avg".equals(type)) {
            double sum = shards.stream()
                    .map(shard -> shard.get("sum"))
                    .filter(Number.class::isInstance)
                    .map(Number.class::cast)
                    .mapToDouble(Number::doubleValue)
                    .sum();
            result.put("sum", sum);
        }
        return result;
    }

    private static String stringValue(Object value) {
        return value == null ? null : value.toString();
    }
}
