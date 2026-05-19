package com.github.wxk6b1203.cluster;

public record ShardId(
        String indexName,
        int shardNumber
) {
    public String routeKey() {
        return indexName + "[" + shardNumber + "]";
    }
}
