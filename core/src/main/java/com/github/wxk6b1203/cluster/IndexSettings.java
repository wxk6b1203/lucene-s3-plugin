package com.github.wxk6b1203.cluster;

import java.time.Instant;
import java.util.Map;

public record IndexSettings(
        String name,
        int numberOfShards,
        String lifecyclePolicy,
        Instant createdAt,
        Map<String, FieldMapping> mappings
) {
    public IndexSettings(String name, int numberOfShards, String lifecyclePolicy, Instant createdAt) {
        this(name, numberOfShards, lifecyclePolicy, createdAt, Map.of());
    }

    public IndexSettings {
        if (numberOfShards <= 0) {
            throw new IllegalArgumentException("numberOfShards must be positive");
        }
        mappings = mappings == null ? Map.of() : Map.copyOf(mappings);
    }
}
