package com.github.wxk6b1203.store.manifest;

import java.time.Duration;

public record ManifestOptions(
        String bucket,
        UploadWaitStrategy uploadWaitStrategy,
        Duration uploadWaitTimeout
) {
    public ManifestOptions(String bucket) {
        this(bucket, UploadWaitStrategy.ASYNC, Duration.ofSeconds(30));
    }

    public ManifestOptions {
        uploadWaitStrategy = uploadWaitStrategy == null ? UploadWaitStrategy.ASYNC : uploadWaitStrategy;
        uploadWaitTimeout = uploadWaitTimeout == null || uploadWaitTimeout.isNegative() || uploadWaitTimeout.isZero()
                ? Duration.ofSeconds(30)
                : uploadWaitTimeout;
    }
}
