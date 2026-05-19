package com.github.wxk6b1203.cluster;

public record FieldMapping(
        String type,
        Integer dimension,
        String similarity,
        Boolean indexed,
        Boolean stored,
        Boolean docValues
) {
    public FieldMapping(String type, Integer dimension, String similarity, Boolean indexed, Boolean stored) {
        this(type, dimension, similarity, indexed, stored, null);
    }

    public FieldMapping {
        type = normalize(type, "keyword");
        similarity = normalize(similarity, "cosine");
        indexed = indexed == null || indexed;
        stored = stored == null || stored;
        docValues = docValues == null ? defaultDocValues(type) : docValues;
        if (denseVector() && (dimension == null || dimension <= 0)) {
            throw new IllegalArgumentException("dense_vector mapping requires a positive dimension");
        }
        if (!supportedType(type)) {
            throw new IllegalArgumentException("unsupported mapping type: " + type);
        }
        if (denseVector() && !supportedSimilarity(similarity)) {
            throw new IllegalArgumentException("unsupported vector similarity: " + similarity);
        }
    }

    public boolean denseVector() {
        return "dense_vector".equals(type);
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

    private static String normalize(String value, String defaultValue) {
        return value == null || value.isBlank() ? defaultValue : value.trim().toLowerCase();
    }

    private static boolean supportedType(String type) {
        return "keyword".equals(type)
                || "text".equals(type)
                || "long".equals(type)
                || "integer".equals(type)
                || "double".equals(type)
                || "float".equals(type)
                || "boolean".equals(type)
                || "dense_vector".equals(type);
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
                || "double".equals(type)
                || "float".equals(type)
                || "boolean".equals(type);
    }
}
