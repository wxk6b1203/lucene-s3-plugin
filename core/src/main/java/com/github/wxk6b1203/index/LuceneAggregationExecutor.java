package com.github.wxk6b1203.index;

import com.github.wxk6b1203.cluster.FieldMapping;
import com.github.wxk6b1203.util.JsonUtil;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class LuceneAggregationExecutor {
    Map<String, Object> aggregate(
            IndexSearcher searcher,
            Query query,
            List<Map<String, Object>> aggregations,
            Map<String, FieldMapping> mappings
    ) throws IOException {
        if (aggregations == null || aggregations.isEmpty()) {
            return Map.of();
        }
        int count = searcher.count(query);
        List<Integer> docIds = new ArrayList<>(count);
        List<Map<String, Object>> sources = new ArrayList<>(count);
        if (count > 0) {
            TopDocs topDocs = searcher.search(query, count);
            var storedFields = searcher.storedFields();
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                docIds.add(scoreDoc.doc);
                sources.add(source(storedFields.document(scoreDoc.doc)));
            }
        }
        Map<String, Object> results = new LinkedHashMap<>();
        for (Map<String, Object> aggregation : aggregations) {
            String name = stringValue(aggregation.get("name"));
            if (name == null || name.isBlank()) {
                continue;
            }
            if (aggregation.get("terms") instanceof Map<?, ?> terms) {
                results.put(name, termsAggregation(searcher, docIds, sources, mapValue(terms), mappings));
            } else if (aggregation.get("range") instanceof Map<?, ?> range) {
                results.put(name, rangeAggregation(searcher, docIds, sources, mapValue(range), mappings));
            } else if (aggregation.get("min") instanceof Map<?, ?> min) {
                results.put(name, metricAggregation(searcher, docIds, sources, mapValue(min), "min", mappings));
            } else if (aggregation.get("max") instanceof Map<?, ?> max) {
                results.put(name, metricAggregation(searcher, docIds, sources, mapValue(max), "max", mappings));
            } else if (aggregation.get("sum") instanceof Map<?, ?> sum) {
                results.put(name, metricAggregation(searcher, docIds, sources, mapValue(sum), "sum", mappings));
            } else if (aggregation.get("avg") instanceof Map<?, ?> avg) {
                results.put(name, metricAggregation(searcher, docIds, sources, mapValue(avg), "avg", mappings));
            } else if (aggregation.get("value_count") instanceof Map<?, ?> valueCount) {
                results.put(name, metricAggregation(searcher, docIds, sources, mapValue(valueCount), "value_count", mappings));
            }
        }
        return results;
    }

    private Map<String, Object> termsAggregation(
            IndexSearcher searcher,
            List<Integer> docIds,
            List<Map<String, Object>> sources,
            Map<String, Object> spec,
            Map<String, FieldMapping> mappings
    ) throws IOException {
        String field = stringValue(spec.get("field"));
        int size = Math.max(1, intValue(spec.get("size"), 10));
        long minDocCount = Math.max(0, longOrDefault(spec.get("min_doc_count"), 1));
        Object missing = spec.get("missing");
        Map<String, Long> counts = new HashMap<>();
        FieldMapping mapping = mappings.get(field);
        for (int i = 0; i < docIds.size(); i++) {
            Object value = aggregationValue(searcher, docIds.get(i), field, mapping, sources.get(i));
            List<Object> values = aggregationValues(value);
            if (values.isEmpty() && missing != null) {
                values = List.of(missing);
            }
            for (Object item : values) {
                counts.merge(String.valueOf(item), 1L, Long::sum);
            }
        }
        List<Map<String, Object>> buckets = counts.entrySet().stream()
                .sorted(termsComparator(spec))
                .map(entry -> Map.<String, Object>of(
                        "key", entry.getKey(),
                        "doc_count", entry.getValue()
                ))
                .toList();
        return Map.of(
                "type", "terms",
                "field", field,
                "size", size,
                "min_doc_count", minDocCount,
                "order", termsOrder(spec),
                "buckets", buckets
        );
    }

    private Comparator<Map.Entry<String, Long>> termsComparator(Map<String, Object> spec) {
        Map<String, Object> order = mapValue(spec.get("order"));
        if (order.isEmpty()) {
            return Comparator
                    .comparing((Map.Entry<String, Long> entry) -> entry.getValue()).reversed()
                    .thenComparing(Map.Entry::getKey);
        }
        String by = order.keySet().iterator().next();
        boolean desc = !"asc".equalsIgnoreCase(String.valueOf(order.get(by)));
        Comparator<Map.Entry<String, Long>> comparator = "_key".equals(by)
                ? Map.Entry.comparingByKey()
                : Comparator.comparing(Map.Entry::getValue);
        if (desc) {
            comparator = comparator.reversed();
        }
        return comparator.thenComparing(Map.Entry::getKey);
    }

    private Map<String, Object> termsOrder(Map<String, Object> spec) {
        Map<String, Object> order = mapValue(spec.get("order"));
        if (order.isEmpty()) {
            return Map.of("_count", "desc");
        }
        return Map.copyOf(order);
    }

    private Map<String, Object> rangeAggregation(
            IndexSearcher searcher,
            List<Integer> docIds,
            List<Map<String, Object>> sources,
            Map<String, Object> spec,
            Map<String, FieldMapping> mappings
    ) throws IOException {
        String field = stringValue(spec.get("field"));
        FieldMapping mapping = mappings.get(field);
        List<RangeBucket> ranges = rangeBuckets(spec, mapping);
        long[] counts = new long[ranges.size()];
        for (int i = 0; i < docIds.size(); i++) {
            boolean[] matched = new boolean[ranges.size()];
            for (Object item : aggregationValues(aggregationValue(searcher, docIds.get(i), field, mapping, sources.get(i)))) {
                Double value = aggregationNumber(item, mapping);
                if (value == null) {
                    continue;
                }
                for (int range = 0; range < ranges.size(); range++) {
                    if (ranges.get(range).contains(value)) {
                        matched[range] = true;
                    }
                }
            }
            for (int range = 0; range < matched.length; range++) {
                if (matched[range]) {
                    counts[range]++;
                }
            }
        }
        List<Map<String, Object>> buckets = new ArrayList<>(ranges.size());
        for (int i = 0; i < ranges.size(); i++) {
            RangeBucket range = ranges.get(i);
            Map<String, Object> bucket = new LinkedHashMap<>();
            bucket.put("key", range.key());
            if (range.from() != null) {
                bucket.put("from", range.fromValue());
            }
            if (range.to() != null) {
                bucket.put("to", range.toValue());
            }
            bucket.put("doc_count", counts[i]);
            buckets.add(bucket);
        }
        return Map.of(
                "type", "range",
                "field", field,
                "buckets", buckets
        );
    }

    private List<RangeBucket> rangeBuckets(Map<String, Object> spec, FieldMapping mapping) {
        String field = stringValue(spec.get("field"));
        List<Object> ranges = objectList(spec.get("ranges"));
        if (ranges.isEmpty()) {
            throw new IllegalArgumentException("range aggregation requires ranges");
        }
        List<RangeBucket> buckets = new ArrayList<>(ranges.size());
        for (Object value : ranges) {
            Map<String, Object> range = mapValue(value);
            boolean hasFrom = range.containsKey("from");
            boolean hasTo = range.containsKey("to");
            Object fromValue = range.get("from");
            Object toValue = range.get("to");
            Double from = aggregationBound(fromValue, hasFrom, field, "from", mapping);
            Double to = aggregationBound(toValue, hasTo, field, "to", mapping);
            String key = stringValue(range.get("key"));
            if (key == null || key.isBlank()) {
                key = (!hasFrom ? "*" : String.valueOf(fromValue))
                        + "-"
                        + (!hasTo ? "*" : String.valueOf(toValue));
            }
            buckets.add(new RangeBucket(key, from, to, fromValue, toValue));
        }
        return buckets;
    }

    private Double aggregationBound(
            Object value,
            boolean present,
            String field,
            String bound,
            FieldMapping mapping
    ) {
        if (!present) {
            return null;
        }
        Double number = aggregationNumber(value, mapping);
        if (number == null) {
            throw new IllegalArgumentException("range aggregation " + bound
                    + " bound must be numeric or date for field: " + field);
        }
        return number;
    }

    private Double aggregationNumber(Object value, FieldMapping mapping) {
        if (value == null) {
            return null;
        }
        if (mapping != null && mapping.date()) {
            Long date = dateValue(value, mapping);
            return date == null ? null : date.doubleValue();
        }
        return doubleValue(value);
    }

    private Map<String, Object> metricAggregation(
            IndexSearcher searcher,
            List<Integer> docIds,
            List<Map<String, Object>> sources,
            Map<String, Object> spec,
            String type,
            Map<String, FieldMapping> mappings
    ) throws IOException {
        String field = stringValue(spec.get("field"));
        FieldMapping mapping = mappings.get(field);
        double sum = 0;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        long count = 0;
        for (int i = 0; i < docIds.size(); i++) {
            for (Object item : aggregationValues(aggregationValue(searcher, docIds.get(i), field, mapping, sources.get(i)))) {
                Double value = doubleValue(item);
                if (value == null) {
                    continue;
                }
                sum += value;
                min = Math.min(min, value);
                max = Math.max(max, value);
                count++;
            }
        }
        Object value = switch (type) {
            case "min" -> count == 0 ? null : min;
            case "max" -> count == 0 ? null : max;
            case "sum" -> sum;
            case "avg" -> count == 0 ? null : sum / count;
            case "value_count" -> count;
            default -> null;
        };
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("type", type);
        result.put("field", field);
        result.put("value", value);
        result.put("count", count);
        if ("avg".equals(type)) {
            result.put("sum", sum);
        }
        return result;
    }

    private Object aggregationValue(
            IndexSearcher searcher,
            int docId,
            String field,
            FieldMapping mapping,
            Map<String, Object> source
    ) throws IOException {
        if (mapping == null || !Boolean.TRUE.equals(mapping.docValues())) {
            return source.get(field);
        }
        Object docValue = docValue(searcher, docId, field, mapping);
        return docValue == null ? source.get(field) : docValue;
    }

    private Object docValue(IndexSearcher searcher, int docId, String field, FieldMapping mapping) throws IOException {
        if (mapping.multiValued()) {
            return null;
        }
        LeafReaderContext leaf = leaf(searcher, docId);
        if (leaf == null) {
            return null;
        }
        int leafDocId = docId - leaf.docBase;
        if (mapping.keyword() || mapping.bool()) {
            SortedDocValues values = DocValues.getSorted(leaf.reader(), field);
            if (!values.advanceExact(leafDocId)) {
                return null;
            }
            return values.lookupOrd(values.ordValue()).utf8ToString();
        }
        if (mapping.longNumber() || mapping.date()) {
            NumericDocValues values = DocValues.getNumeric(leaf.reader(), field);
            return values.advanceExact(leafDocId) ? values.longValue() : null;
        }
        if (mapping.doubleNumber()) {
            NumericDocValues values = DocValues.getNumeric(leaf.reader(), field);
            return values.advanceExact(leafDocId) ? Double.longBitsToDouble(values.longValue()) : null;
        }
        if (mapping.ip() || mapping.binary()) {
            BinaryDocValues values = DocValues.getBinary(leaf.reader(), field);
            if (!values.advanceExact(leafDocId)) {
                return null;
            }
            BytesRef bytes = values.binaryValue();
            byte[] copy = Arrays.copyOfRange(bytes.bytes, bytes.offset, bytes.offset + bytes.length);
            if (mapping.ip()) {
                return InetAddressPoint.decode(copy).getHostAddress();
            }
            return Base64.getEncoder().encodeToString(copy);
        }
        return null;
    }

    private LeafReaderContext leaf(IndexSearcher searcher, int docId) {
        for (LeafReaderContext leaf : searcher.getIndexReader().leaves()) {
            if (docId >= leaf.docBase && docId < leaf.docBase + leaf.reader().maxDoc()) {
                return leaf;
            }
        }
        return null;
    }

    private Map<String, Object> source(Document document) {
        String source = document.get("_source");
        return source == null || source.isBlank() ? Map.of() : JsonUtil.readValueAsMap(source);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> mapValue(Object value) {
        return value instanceof Map<?, ?> map ? (Map<String, Object>) map : Map.of();
    }

    private List<Object> objectList(Object value) {
        if (!(value instanceof List<?> list)) {
            return List.of();
        }
        return list.stream().map(item -> (Object) item).toList();
    }

    private int intValue(Object value, int defaultValue) {
        return value instanceof Number number ? number.intValue() : defaultValue;
    }

    private String stringValue(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private long longOrDefault(Object value, long defaultValue) {
        Long number = longValue(value);
        return number == null ? defaultValue : number;
    }

    private Long longValue(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value instanceof String string && !string.isBlank()) {
            try {
                return Long.valueOf(string.trim());
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        return null;
    }

    private Double doubleValue(Object value) {
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        if (value instanceof String string && !string.isBlank()) {
            try {
                return Double.valueOf(string.trim());
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        return null;
    }

    private Long dateValue(Object value, FieldMapping mapping) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (!(value instanceof String string) || string.isBlank()) {
            return null;
        }
        String trimmed = string.trim();
        try {
            return Long.valueOf(trimmed);
        } catch (NumberFormatException ignored) {
        }
        try {
            Instant instant = Instant.parse(trimmed);
            return mapping != null && mapping.dateNanos()
                    ? Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1_000_000_000L), instant.getNano())
                    : instant.toEpochMilli();
        } catch (DateTimeParseException ignored) {
        } catch (ArithmeticException e) {
            return null;
        }
        try {
            Instant instant = OffsetDateTime.parse(trimmed).toInstant();
            return mapping != null && mapping.dateNanos()
                    ? Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1_000_000_000L), instant.getNano())
                    : instant.toEpochMilli();
        } catch (DateTimeParseException ignored) {
        } catch (ArithmeticException e) {
            return null;
        }
        try {
            Instant instant = LocalDate.parse(trimmed).atStartOfDay(ZoneOffset.UTC).toInstant();
            return mapping != null && mapping.dateNanos()
                    ? Math.multiplyExact(instant.getEpochSecond(), 1_000_000_000L)
                    : instant.toEpochMilli();
        } catch (DateTimeParseException ignored) {
            return null;
        } catch (ArithmeticException e) {
            return null;
        }
    }

    private List<Object> aggregationValues(Object value) {
        if (value == null) {
            return List.of();
        }
        if (value instanceof Iterable<?> iterable) {
            List<Object> values = new ArrayList<>();
            for (Object item : iterable) {
                if (item != null) {
                    values.add(item);
                }
            }
            return values;
        }
        return List.of(value);
    }

    private record RangeBucket(
            String key,
            Double from,
            Double to,
            Object fromValue,
            Object toValue
    ) {
        private boolean contains(double value) {
            return (from == null || value >= from) && (to == null || value < to);
        }
    }
}
