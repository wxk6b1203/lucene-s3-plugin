package com.github.wxk6b1203.store.directory;

import java.nio.file.Path;

public record S3DirectoryOptions(
        Path basePath,
        String bucket,
        String indexName,
        boolean enableCaching
) {
}
