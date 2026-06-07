package com.github.wxk6b1203.http;

import com.github.wxk6b1203.errors.StorageException;
import com.github.wxk6b1203.errors.NotMasterException;
import com.github.wxk6b1203.util.JsonUtil;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.RoutingContext;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;

import java.io.IOException;
import java.net.ConnectException;
import java.net.http.HttpTimeoutException;
import java.nio.file.NoSuchFileException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;

final class HttpApiResponses {
    private HttpApiResponses() {
    }

    static void json(RoutingContext context, int status, Object value) {
        if (HttpApiProtobuf.wantsProtobuf(context)) {
            context.response()
                    .setStatusCode(status)
                    .putHeader("content-type", HttpApiProtobuf.MEDIA_TYPE)
                    .end(Buffer.buffer(HttpApiProtobuf.writeValueAsBytes(value)));
            return;
        }
        context.response()
                .setStatusCode(status)
                .putHeader("content-type", "application/json")
                .end(new String(JsonUtil.writeValueAsBytes(value)));
    }

    static void error(RoutingContext context, int status, Exception e) {
        Throwable cause = responseCause(e);
        Map<String, Object> body = Map.of(
                "error", cause.getMessage() == null ? cause.getClass().getSimpleName() : cause.getMessage(),
                "type", cause.getClass().getSimpleName(),
                "status", status
        );
        if (HttpApiProtobuf.wantsProtobuf(context)) {
            context.response()
                    .setStatusCode(status)
                    .putHeader("content-type", HttpApiProtobuf.MEDIA_TYPE)
                    .end(Buffer.buffer(HttpApiProtobuf.writeValueAsBytes(body)));
            return;
        }
        context.response()
                .setStatusCode(status)
                .putHeader("content-type", "application/json")
                .end(new String(JsonUtil.writeValueAsBytes(body)));
    }

    static int status(Exception e) {
        Throwable cause = responseCause(e);
        if (cause instanceof NotMasterException) {
            return 503;
        }
        if (cause instanceof RejectedExecutionException) {
            return 429;
        }
        if ("IndexNotFoundException".equals(cause.getClass().getSimpleName()) || messageContains(cause, "no segments")) {
            return 503;
        }
        if (cause instanceof NoSuchFileException || messageContains(cause, "not found") || messageContains(cause, "expired")) {
            return 404;
        }
        if (messageContains(cause, "already exists") || messageContains(cause, "conflict")) {
            return 409;
        }
        if (cause instanceof StorageException || cause instanceof SdkClientException
                || cause instanceof ConnectException || cause instanceof HttpTimeoutException) {
            return 503;
        }
        if (cause instanceof SdkServiceException || messageContains(cause, "remote shard")) {
            return 502;
        }
        if (cause instanceof IllegalStateException) {
            if (messageContains(cause, "no live data node")
                    || messageContains(cause, "node is not live")
                    || messageContains(cause, "requires live shard owner")
                    || messageContains(cause, "current master node is not available")) {
                return 503;
            }
            if (messageContains(cause, "not current shard owner")
                    || messageContains(cause, "shard is not writable")
                    || messageContains(cause, "stale")
                    || messageContains(cause, "write fence")) {
                return 409;
            }
            return 500;
        }
        if (cause instanceof IOException) {
            if (messageContains(cause, "no space left")
                    || messageContains(cause, "too many open files")
                    || messageContains(cause, "resource temporarily unavailable")) {
                return 503;
            }
            return 500;
        }
        if (cause instanceof IllegalArgumentException) {
            return 400;
        }
        return 400;
    }

    static Exception exception(Throwable throwable) {
        Throwable cause = responseCause(throwable);
        return cause instanceof Exception exception ? exception : new RuntimeException(cause);
    }

    private static Throwable responseCause(Throwable throwable) {
        Throwable current = throwable;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static boolean messageContains(Throwable throwable, String token) {
        String message = throwable.getMessage();
        return message != null && message.toLowerCase(Locale.ROOT).contains(token);
    }
}
