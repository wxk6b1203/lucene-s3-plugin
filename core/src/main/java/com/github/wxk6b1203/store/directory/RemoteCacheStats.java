package com.github.wxk6b1203.store.directory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public final class RemoteCacheStats {
    private static final AtomicLong HITS = new AtomicLong();
    private static final AtomicLong MISSES = new AtomicLong();
    private static final AtomicLong DOWNLOADS = new AtomicLong();
    private static final AtomicLong CORRUPTIONS = new AtomicLong();

    private RemoteCacheStats() {
    }

    static void hit() {
        HITS.incrementAndGet();
    }

    static void miss() {
        MISSES.incrementAndGet();
    }

    static void download() {
        DOWNLOADS.incrementAndGet();
    }

    static void corruption() {
        CORRUPTIONS.incrementAndGet();
    }

    public static Map<String, Object> snapshot() {
        long hits = HITS.get();
        long misses = MISSES.get();
        long total = hits + misses;
        double hitRate = total == 0 ? 0.0 : (double) hits / total;
        Map<String, Object> stats = new HashMap<>();
        stats.put("hits", hits);
        stats.put("misses", misses);
        stats.put("downloads", DOWNLOADS.get());
        stats.put("corruptions", CORRUPTIONS.get());
        stats.put("hit_rate", hitRate);
        return stats;
    }
}
