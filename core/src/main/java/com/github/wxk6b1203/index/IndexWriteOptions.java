package com.github.wxk6b1203.index;

import java.time.Duration;
import java.util.Locale;

public record IndexWriteOptions(
        boolean commitEveryRequest,
        int commitAfterDocs,
        Duration commitInterval,
        RefreshPolicy refreshPolicy,
        Duration refreshInterval
) {
    private static final Duration DEFAULT_DEFERRED_COMMIT_INTERVAL = Duration.ofSeconds(1);
    private static final Duration DEFAULT_REFRESH_INTERVAL = Duration.ofSeconds(1);

    public IndexWriteOptions {
        commitAfterDocs = Math.max(0, commitAfterDocs);
        commitInterval = positiveOrZero(commitInterval);
        if (!commitEveryRequest && commitAfterDocs == 0 && commitInterval.isZero()) {
            commitInterval = DEFAULT_DEFERRED_COMMIT_INTERVAL;
        }
        refreshPolicy = refreshPolicy == null ? RefreshPolicy.IMMEDIATE : refreshPolicy;
        refreshInterval = positiveOrDefault(refreshInterval, DEFAULT_REFRESH_INTERVAL);
    }

    public static IndexWriteOptions defaults() {
        return new IndexWriteOptions(true, 0, Duration.ZERO, RefreshPolicy.IMMEDIATE, DEFAULT_REFRESH_INTERVAL);
    }

    private static Duration positiveOrZero(Duration duration) {
        if (duration == null || duration.isNegative() || duration.isZero()) {
            return Duration.ZERO;
        }
        return duration;
    }

    private static Duration positiveOrDefault(Duration duration, Duration defaultValue) {
        if (duration == null || duration.isNegative() || duration.isZero()) {
            return defaultValue;
        }
        return duration;
    }

    public enum RefreshPolicy {
        IMMEDIATE,
        INTERVAL;

        public static RefreshPolicy parse(String value) {
            if (value == null || value.isBlank()) {
                return IMMEDIATE;
            }
            return switch (value.trim().toLowerCase(Locale.ROOT)) {
                case "immediate", "true" -> IMMEDIATE;
                case "interval", "false" -> INTERVAL;
                default -> throw new IllegalArgumentException("refresh policy must be immediate or interval");
            };
        }
    }
}
