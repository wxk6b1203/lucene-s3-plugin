package com.github.wxk6b1203.search;

import java.util.List;

public record VectorQuery(
        String field,
        List<Float> vector,
        int k,
        int numCandidates,
        Float minScore
) {
    public VectorQuery(String field, List<Float> vector, int k, int numCandidates) {
        this(field, vector, k, numCandidates, null);
    }

    public VectorQuery {
        vector = vector == null ? List.of() : List.copyOf(vector);
        k = Math.max(1, k);
        numCandidates = Math.max(k, numCandidates);
    }
}
