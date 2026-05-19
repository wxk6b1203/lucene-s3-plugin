package com.github.wxk6b1203.search;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;

public record SearchHit(
        String indexName,
        String id,
        float score,
        Map<String, Object> source,
        List<Object> sortValues
) {
    public SearchHit(String indexName, String id, float score, Map<String, Object> source) {
        this(indexName, id, score, source, List.of());
    }

    public SearchHit {
        sortValues = sortValues == null ? List.of() : Collections.unmodifiableList(new ArrayList<>(sortValues));
    }
}
