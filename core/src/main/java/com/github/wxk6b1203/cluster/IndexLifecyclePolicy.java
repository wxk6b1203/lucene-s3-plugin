package com.github.wxk6b1203.cluster;

import java.util.Map;

public record IndexLifecyclePolicy(
        String name,
        Map<LifecyclePhase, Long> minAgeMillisByPhase
) {
    public IndexLifecyclePolicy {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("lifecycle policy name is required");
        }
        minAgeMillisByPhase = minAgeMillisByPhase == null ? Map.of() : Map.copyOf(minAgeMillisByPhase);
        minAgeMillisByPhase.forEach((phase, minAgeMillis) -> {
            if (phase == null) {
                throw new IllegalArgumentException("lifecycle phase is required");
            }
            if (minAgeMillis == null || minAgeMillis < 0) {
                throw new IllegalArgumentException("lifecycle phase min_age must be non-negative: " + phase);
            }
        });
    }
}
