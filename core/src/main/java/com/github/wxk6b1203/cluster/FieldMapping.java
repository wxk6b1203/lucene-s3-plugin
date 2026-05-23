package com.github.wxk6b1203.cluster;

public record FieldMapping(
        String type,
        Integer dimension,
        String similarity,
        Boolean indexed,
        Boolean stored,
        Boolean docValues,
        Boolean multiValued,
        String analyzer,
        String searchAnalyzer
) {
    public FieldMapping(String type, Integer dimension, String similarity, Boolean indexed, Boolean stored) {
        this(type, dimension, similarity, indexed, stored, null, null, null, null);
    }

    public FieldMapping(
            String type,
            Integer dimension,
            String similarity,
            Boolean indexed,
            Boolean stored,
            Boolean docValues
    ) {
        this(type, dimension, similarity, indexed, stored, docValues, null, null, null);
    }

    public FieldMapping(
            String type,
            Integer dimension,
            String similarity,
            Boolean indexed,
            Boolean stored,
            Boolean docValues,
            Boolean multiValued
    ) {
        this(type, dimension, similarity, indexed, stored, docValues, multiValued, null, null);
    }

    public FieldMapping {
        type = normalize(type, "keyword");
        similarity = normalize(similarity, "cosine");
        analyzer = blankToNull(analyzer);
        searchAnalyzer = blankToNull(searchAnalyzer);
        indexed = indexed == null || indexed;
        stored = stored == null || stored;
        docValues = docValues == null ? defaultDocValues(type) : docValues;
        multiValued = multiValued != null && multiValued;
        if ((denseVector() || byteVector()) && (dimension == null || dimension <= 0)) {
            throw new IllegalArgumentException(type + " mapping requires a positive dimension");
        }
        if (!supportedType(type)) {
            throw new IllegalArgumentException("unsupported mapping type: " + type);
        }
        if ((denseVector() || byteVector()) && !supportedSimilarity(similarity)) {
            throw new IllegalArgumentException("unsupported vector similarity: " + similarity);
        }
    }

    public boolean denseVector() {
        return "dense_vector".equals(type);
    }

    public boolean byteVector() {
        return "byte_vector".equals(type);
    }

    public boolean keyword() {
        return "keyword".equals(type);
    }

    public boolean text() {
        return "text".equals(type);
    }

    public boolean longNumber() {
        return "long".equals(type) || "integer".equals(type);
    }

    public boolean doubleNumber() {
        return "double".equals(type) || "float".equals(type);
    }

    public boolean bool() {
        return "boolean".equals(type);
    }

    public boolean date() {
        return "date".equals(type) || "date_nanos".equals(type);
    }

    public boolean dateNanos() {
        return "date_nanos".equals(type);
    }

    public boolean ip() {
        return "ip".equals(type);
    }

    public boolean binary() {
        return "binary".equals(type);
    }

    public boolean geoPoint() {
        return "geo_point".equals(type);
    }

    public boolean longRange() {
        return "long_range".equals(type) || "integer_range".equals(type) || "date_range".equals(type);
    }

    public boolean doubleRange() {
        return "double_range".equals(type) || "float_range".equals(type);
    }

    public boolean ipRange() {
        return "ip_range".equals(type);
    }

    private static String normalize(String value, String defaultValue) {
        return value == null || value.isBlank() ? defaultValue : value.trim().toLowerCase();
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }

    private static boolean supportedType(String type) {
        return "keyword".equals(type)
                || "text".equals(type)
                || "long".equals(type)
                || "integer".equals(type)
                || "double".equals(type)
                || "float".equals(type)
                || "boolean".equals(type)
                || "dense_vector".equals(type)
                || "byte_vector".equals(type)
                || "date".equals(type)
                || "date_nanos".equals(type)
                || "ip".equals(type)
                || "binary".equals(type)
                || "geo_point".equals(type)
                || "long_range".equals(type)
                || "integer_range".equals(type)
                || "date_range".equals(type)
                || "double_range".equals(type)
                || "float_range".equals(type)
                || "ip_range".equals(type);
    }

    private static boolean supportedSimilarity(String similarity) {
        return "cosine".equals(similarity)
                || "dot_product".equals(similarity)
                || "euclidean".equals(similarity)
                || "maximum_inner_product".equals(similarity);
    }

    private static boolean defaultDocValues(String type) {
        return "keyword".equals(type)
                || "long".equals(type)
                || "integer".equals(type)
                || "date".equals(type)
                || "date_nanos".equals(type)
                || "double".equals(type)
                || "float".equals(type)
                || "boolean".equals(type)
                || "ip".equals(type)
                || "binary".equals(type)
                || "geo_point".equals(type);
    }
}
