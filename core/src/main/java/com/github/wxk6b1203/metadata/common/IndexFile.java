package com.github.wxk6b1203.metadata.common;

public record IndexFile(
        String indexName,
        String name,
        long size,
        long checksum
) {
}
