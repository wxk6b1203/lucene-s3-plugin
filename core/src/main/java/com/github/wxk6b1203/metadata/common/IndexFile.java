package com.github.wxk6b1203.metadata.common;

public record IndexFile(
        String indexName,
        String name,
        String dataDirectory,
        String objectKey,
        long size,
        long checksum,
        long modifiedTime
) {
    public IndexFile(String indexName, String name, long size, long checksum) {
        this(indexName, name, "_data", indexName + "/_data/" + name, size, checksum, System.currentTimeMillis());
    }
}
