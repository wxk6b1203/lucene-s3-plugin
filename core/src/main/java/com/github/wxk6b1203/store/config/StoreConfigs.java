package com.github.wxk6b1203.store.config;

public record StoreConfigs(
        String bucket,
        String cachePath,
        String cacheMaxSizeMb,
        String cacheMaxDiskUsagePercentage
) {
}
