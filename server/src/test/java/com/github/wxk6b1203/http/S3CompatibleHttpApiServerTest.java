package com.github.wxk6b1203.http;

import com.github.wxk6b1203.cluster.NodeRole;
import com.github.wxk6b1203.config.ServerOptions;
import com.github.wxk6b1203.util.JsonUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class S3CompatibleHttpApiServerTest {
    @TempDir
    Path tempDir;

    @Test
    @Timeout(90)
    @EnabledIfEnvironmentVariable(named = "S3_TEST_BUCKET", matches = ".+")
    @SuppressWarnings("unchecked")
    void committedShardFilesCanBeUploadedAndReadBackFromS3CompatibleStore() throws Exception {
        int port = freePort();
        String indexName = "s3_it_" + UUID.randomUUID().toString().replace("-", "");
        try (HttpApiServer server = new HttpApiServer(new ServerOptions(
                port,
                "test-cluster",
                "node-1",
                "node-1",
                "127.0.0.1",
                Set.of(NodeRole.MASTER, NodeRole.DATA, NodeRole.COORDINATING),
                null,
                "test/ns",
                tempDir.resolve("node").toString(),
                System.getenv("S3_TEST_BUCKET"),
                System.getenv("S3_TEST_REGION"),
                System.getenv().getOrDefault("S3_TEST_PROTOCOL", "https"),
                System.getenv("S3_TEST_ENDPOINT"),
                Boolean.parseBoolean(System.getenv().getOrDefault("S3_TEST_CHUNKED_ENCODING", "false")),
                null,
                null,
                2
        ))) {
            server.start().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            put(port, "/" + indexName, Map.of(
                    "number_of_shards", 1,
                    "mappings", Map.of("properties", Map.of(
                            "category", Map.of("type", "keyword")
                    ))
            ), 200);
            post(port, "/" + indexName + "/_doc/doc-1", Map.of("category", "remote"), 201);
            waitUntil(() -> hitIds(post(port, "/" + indexName + "/_search?read_preference=strong", Map.of(
                    "query", Map.of("term", Map.of("category", "remote")),
                    "size", 10
            ), 200)).equals(List.of("doc-1")));
        }
    }

    private Map<String, Object> put(int port, String path, Object body, int expectedStatus) throws Exception {
        return request(port, "PUT", path, body, expectedStatus);
    }

    private Map<String, Object> post(int port, String path, Object body, int expectedStatus) throws Exception {
        return request(port, "POST", path, body, expectedStatus);
    }

    private Map<String, Object> request(int port, String method, String path, Object body, int expectedStatus) throws Exception {
        Response response = response(port, method, path, body);
        if (response.status() != expectedStatus) {
            fail(method + " " + path + " returned " + response.status() + ": " + response.body());
        }
        return response.body() == null || response.body().isBlank()
                ? Map.of()
                : JsonUtil.readValueAsMap(response.body());
    }

    private Response response(int port, String method, String path, Object body) throws Exception {
        URL url = URI.create("http://127.0.0.1:" + port + path).toURL();
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod(method);
        connection.setConnectTimeout((int) Duration.ofSeconds(5).toMillis());
        connection.setReadTimeout((int) Duration.ofSeconds(20).toMillis());
        connection.setRequestProperty("content-type", "application/json");
        connection.setRequestProperty("connection", "close");
        connection.setDoInput(true);
        if (body != null) {
            connection.setDoOutput(true);
            try (OutputStream outputStream = connection.getOutputStream()) {
                outputStream.write(JsonUtil.writeValueAsBytes(body));
            }
        }
        int status = connection.getResponseCode();
        String responseBody;
        try (InputStream inputStream = status >= 400 ? connection.getErrorStream() : connection.getInputStream()) {
            responseBody = inputStream == null
                    ? ""
                    : new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        } finally {
            connection.disconnect();
        }
        return new Response(status, responseBody);
    }

    private int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private void waitUntil(CheckedCondition condition) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        AssertionError lastFailure = null;
        while (System.nanoTime() < deadline) {
            try {
                if (condition.evaluate()) {
                    return;
                }
            } catch (AssertionError e) {
                lastFailure = e;
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
        if (lastFailure != null) {
            throw lastFailure;
        }
        fail("condition was not met before timeout");
    }

    @SuppressWarnings("unchecked")
    private List<String> hitIds(Map<String, Object> response) {
        return ((List<Map<String, Object>>) response.get("hits")).stream()
                .map(hit -> String.valueOf(hit.get("id")))
                .toList();
    }

    private interface CheckedCondition {
        boolean evaluate() throws Exception;
    }

    private record Response(int status, String body) {
    }
}
