package com.github.wxk6b1203.metadata.common;

import java.nio.file.Path;

public record CommittingIndexFile(
    String indexName,
    Path filePath
) {
}
