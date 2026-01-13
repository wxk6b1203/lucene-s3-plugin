package com.github.wxk6b1203.metadata.common;

public record IndexFileMetadata(
        String indexName,
        String name,
        long epoch,
        long size,
        long checksum,
        long modifiedTime,
        IndexFileStatus status
) {
}
