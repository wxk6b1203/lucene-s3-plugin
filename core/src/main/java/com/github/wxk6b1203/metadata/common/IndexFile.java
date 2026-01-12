package com.github.wxk6b1203.metadata.common;

import lombok.NoArgsConstructor;

public record IndexFile(
        String indexName,
        String name,
        long size,
        long checksum
) {
}
