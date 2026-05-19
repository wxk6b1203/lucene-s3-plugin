package com.github.wxk6b1203.index;

import com.github.wxk6b1203.cluster.ShardId;

public record IndexDocumentResponse(
        String indexName,
        ShardId shardId,
        String id,
        String result,
        boolean committed
) {
}
