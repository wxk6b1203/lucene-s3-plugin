package com.github.wxk6b1203.http;

import io.vertx.ext.web.RoutingContext;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;

final class WriteBackpressure {
    private final Semaphore writeRequests;
    private final int maxBulkItems;
    private final long maxBulkBytes;

    WriteBackpressure(int maxWriteRequests, int maxBulkItems, long maxBulkBytes) {
        this.writeRequests = maxWriteRequests <= 0 ? null : new Semaphore(maxWriteRequests);
        this.maxBulkItems = Math.max(0, maxBulkItems);
        this.maxBulkBytes = Math.max(0, maxBulkBytes);
    }

    boolean acquire(RoutingContext context) {
        if (writeRequests == null || writeRequests.tryAcquire()) {
            return true;
        }
        HttpApiResponses.error(
                context,
                429,
                new RejectedExecutionException("too many concurrent write requests")
        );
        return false;
    }

    void release() {
        if (writeRequests != null) {
            writeRequests.release();
        }
    }

    void validateBulkBody(RoutingContext context) {
        if (maxBulkBytes <= 0 || context.body() == null || context.body().buffer() == null) {
            return;
        }
        int bytes = context.body().buffer().length();
        if (bytes > maxBulkBytes) {
            throw new RejectedExecutionException("bulk request body is too large: " + bytes
                    + " > " + maxBulkBytes);
        }
    }

    void validateBulkItemCount(int items) {
        if (maxBulkItems > 0 && items > maxBulkItems) {
            throw new RejectedExecutionException("too many bulk items: " + items + " > " + maxBulkItems);
        }
    }
}
