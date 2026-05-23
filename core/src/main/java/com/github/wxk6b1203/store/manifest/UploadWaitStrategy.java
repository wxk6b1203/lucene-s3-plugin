package com.github.wxk6b1203.store.manifest;

import java.util.Locale;

public enum UploadWaitStrategy {
    ASYNC,
    WAIT_FOR_UPLOAD;

    public static UploadWaitStrategy parse(String value) {
        if (value == null || value.isBlank()) {
            return ASYNC;
        }
        String normalized = value.trim().replace('-', '_').toUpperCase(Locale.ROOT);
        return switch (normalized) {
            case "ASYNC" -> ASYNC;
            case "WAIT", "WAIT_UPLOAD", "WAIT_FOR_UPLOAD", "SYNC" -> WAIT_FOR_UPLOAD;
            default -> throw new IllegalArgumentException("upload wait strategy must be async or wait_for_upload");
        };
    }
}
