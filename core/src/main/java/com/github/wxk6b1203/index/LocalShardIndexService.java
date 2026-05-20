package com.github.wxk6b1203.index;

import com.github.wxk6b1203.cluster.ShardId;
import com.github.wxk6b1203.search.ByQueryRequest;
import com.github.wxk6b1203.search.ByQueryResponse;
import com.github.wxk6b1203.search.PointInTimeResponse;
import com.github.wxk6b1203.search.SearchRequest;
import com.github.wxk6b1203.search.SearchResponse;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;

public interface LocalShardIndexService extends AutoCloseable {
    IndexDocumentResponse index(IndexDocumentRequest request) throws IOException;

    IndexDocumentResponse delete(IndexDocumentRequest request) throws IOException;

    SearchResponse search(ShardId shardId, SearchRequest request) throws IOException;

    default PointInTimeResponse openPointInTime(ShardId shardId, String indexName, Duration keepAlive) throws IOException {
        return openPointInTime(shardId, indexName, keepAlive, "weak");
    }

    PointInTimeResponse openPointInTime(
            ShardId shardId,
            String indexName,
            Duration keepAlive,
            String readPreference
    ) throws IOException;

    boolean closePointInTime(String pitId) throws IOException;

    ByQueryResponse updateByQuery(ShardId shardId, ByQueryRequest request) throws IOException;

    ByQueryResponse deleteByQuery(ShardId shardId, ByQueryRequest request) throws IOException;

    void deleteIndex(String indexName, int numberOfShards) throws IOException;

    void retryPendingUploads(Collection<ShardId> shardIds) throws IOException;

    @Override
    void close() throws IOException;
}
