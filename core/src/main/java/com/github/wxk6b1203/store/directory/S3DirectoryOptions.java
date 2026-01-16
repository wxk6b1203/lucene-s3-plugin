package com.github.wxk6b1203.store.directory;

import java.nio.file.Path;
import java.time.Duration;

public record S3DirectoryOptions(
        Path basePath,
        String indexName
) {
}
