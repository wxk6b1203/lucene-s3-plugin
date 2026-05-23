package com.github.wxk6b1203.cluster;

import java.time.Instant;
import java.util.Map;

public record IndexSettings(
        String name,
        int numberOfShards,
        String lifecyclePolicy,
        Instant createdAt,
        Map<String, FieldMapping> mappings,
        Boolean deletePending,
        Instant deleteStartedAt
) {
    public IndexSettings(String name, int numberOfShards, String lifecyclePolicy, Instant createdAt) {
        this(name, numberOfShards, lifecyclePolicy, createdAt, Map.of());
    }

    public IndexSettings(String name, int numberOfShards, String lifecyclePolicy, Instant createdAt, Map<String, FieldMapping> mappings) {
        this(name, numberOfShards, lifecyclePolicy, createdAt, mappings, false, null);
    }

    public IndexSettings {
        if (numberOfShards <= 0) {
            throw new IllegalArgumentException("numberOfShards must be positive");
        }
        mappings = mappings == null ? Map.of() : Map.copyOf(mappings);
        deletePending = deletePending != null && deletePending;
        if (!deletePending) {
            deleteStartedAt = null;
        } else if (deleteStartedAt == null) {
            deleteStartedAt = Instant.now();
        }
    }

    public IndexSettings withMappings(Map<String, FieldMapping> mappings) {
        return new IndexSettings(name, numberOfShards, lifecyclePolicy, createdAt, mappings, deletePending, deleteStartedAt);
    }

    public IndexSettings withLifecyclePolicy(String lifecyclePolicy) {
        return new IndexSettings(name, numberOfShards, lifecyclePolicy, createdAt, mappings, deletePending, deleteStartedAt);
    }

    public IndexSettings markDeleting(Instant now) {
        return new IndexSettings(name, numberOfShards, lifecyclePolicy, createdAt, mappings, true, now);
    }
}
