package com.github.wxk6b1203.index;

import com.github.wxk6b1203.cluster.ShardId;
import com.github.wxk6b1203.search.ByQueryRequest;
import com.github.wxk6b1203.search.ByQueryResponse;
import com.github.wxk6b1203.search.PointInTimeResponse;
import com.github.wxk6b1203.search.SearchRequest;
import com.github.wxk6b1203.search.SearchResponse;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public interface LocalShardIndexService extends AutoCloseable {
    IndexDocumentResponse index(IndexDocumentRequest request) throws IOException;

    IndexDocumentResponse delete(IndexDocumentRequest request) throws IOException;

    default List<IndexDocumentOperationResult> bulk(Collection<IndexDocumentOperation> operations) throws IOException {
        List<IndexDocumentOperationResult> results = new ArrayList<>(operations.size());
        for (IndexDocumentOperation operation : operations) {
            try {
                IndexDocumentResponse response = operation.delete()
                        ? delete(operation.request())
                        : index(operation.request());
                results.add(IndexDocumentOperationResult.success(response));
            } catch (Exception e) {
                results.add(IndexDocumentOperationResult.failure(e));
            }
        }
        return results;
    }

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

    default int openPointInTimeCount() {
        return 0;
    }

    @Override
    void close() throws IOException;
}
