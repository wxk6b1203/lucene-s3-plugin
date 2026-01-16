package com.github.wxk6b1203.store.manifest;

import java.nio.file.Path;
import java.time.Duration;

public record ManifestOptions(
        Path basePath,
        String bucket,
        Duration maxBatchWait,
        int maxDeleteWorkers,
        int maxBatchDeleteSize
) {
}
