package com.github.wxk6b1203.http;

import com.github.wxk6b1203.cluster.NodeRole;
import com.github.wxk6b1203.config.ServerOptions;
import com.github.wxk6b1203.util.JsonUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class HttpApiServerStressTest {
    @TempDir
    Path tempDir;

    private HttpApiServer server;
    private int port;
    private int metricsPort;

    @AfterEach
    void tearDown() throws IOException {
        if (server != null) {
            server.close();
        }
    }

    @Test
    @Timeout(180)
    @EnabledIfEnvironmentVariable(named = "LUCENE_S3_STRESS", matches = "true")
    void mixedBulkSearchAndObservabilityStress() throws Exception {
        StressOptions options = StressOptions.fromEnvironment();
        startServer(options);
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .version(HttpClient.Version.HTTP_1_1)
                .build();
        Metrics metrics = new Metrics();
        AtomicInteger indexedDocs = new AtomicInteger();
        Throwable failure = null;
        try {
            putJson(client, "/stress_books", indexBody(options.shards()));
            runWorkload(client, options, metrics, indexedDocs);
            HttpResult health = get(client, "/_cluster/health");
            assertEquals(200, health.status());
            assertFalse(health.body().contains("\"status\":\"red\""));
            waitForStrongVisibility(client, options.docs());
            assertEquals(0, metrics.errors());
            assertEquals(options.docs(), indexedDocs.get());
        } catch (Throwable e) {
            failure = e;
        } finally {
            writeReport(client, options, metrics, indexedDocs.get(), failure);
        }
        if (failure != null) {
            if (failure instanceof Exception exception) {
                throw exception;
            }
            if (failure instanceof Error error) {
                throw error;
            }
            throw new AssertionError(failure);
        }
    }

    private void startServer(StressOptions options) throws Exception {
        port = freePort();
        metricsPort = options.metricsPort() < 0 ? freePort() : options.metricsPort();
        server = new HttpApiServer(new ServerOptions(
                port,
                "stress-cluster",
                "stress-node",
                "stress-node",
                "127.0.0.1",
                Set.of(NodeRole.MASTER, NodeRole.DATA, NodeRole.COORDINATING),
                null,
                "lucene-s3/stress",
                tempDir.resolve("node").toString(),
                null,
                null,
                "https",
                null,
                false,
                null,
                null,
                2,
                10,
                options.cacheMaxBytes(),
                1,
                metricsPort
        ));
        server.start().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    private void runWorkload(
            HttpClient client,
            StressOptions options,
            Metrics metrics,
            AtomicInteger indexedDocs
    ) throws InterruptedException {
        AtomicInteger nextDoc = new AtomicInteger();
        CountDownLatch writersRemaining = new CountDownLatch(options.writeThreads());
        Instant deadline = Instant.now().plusSeconds(options.searchSeconds());
        ExecutorService executor = Executors.newFixedThreadPool(options.writeThreads() + options.searchThreads());
        for (int i = 0; i < options.writeThreads(); i++) {
            int writer = i;
            executor.submit(() -> {
                try {
                    writeLoop(client, options, metrics, nextDoc, indexedDocs, writer);
                } finally {
                    writersRemaining.countDown();
                }
            });
        }
        for (int i = 0; i < options.searchThreads(); i++) {
            int reader = i;
            executor.submit(() -> searchLoop(client, options, metrics, indexedDocs, writersRemaining, deadline, reader));
        }
        executor.shutdown();
        if (!executor.awaitTermination(options.searchSeconds() + 120L, TimeUnit.SECONDS)) {
            executor.shutdownNow();
            throw new AssertionError("stress workload did not finish before timeout");
        }
    }

    private void writeReport(
            HttpClient client,
            StressOptions options,
            Metrics metrics,
            int indexedDocs,
            Throwable failure
    ) throws IOException {
        Map<String, Object> report = metrics.report(options, indexedDocs, port, metricsPort);
        report.put("failure", failure == null ? null : failure.toString());
        addJsonEndpoint(report, "health", () -> get(client, "/_cluster/health"));
        addJsonEndpoint(report, "nodes_stats", () -> get(client, "/_nodes/stats"));
        if (metricsPort > 0) {
            report.put("prometheus_metrics", getAbsolute(client, "http://127.0.0.1:" + metricsPort + "/metrics").body());
        }
        Path reportPath = Path.of("build", "reports", "stress", "http-api-stress.json");
        Files.createDirectories(reportPath.getParent());
        Files.write(reportPath, JsonUtil.writeValueAsBytes(report));
        System.out.println("stress report: " + reportPath.toAbsolutePath());
        System.out.println(new String(JsonUtil.writeValueAsBytes(report), StandardCharsets.UTF_8));
    }

    private void addJsonEndpoint(Map<String, Object> report, String name, EndpointCall call) {
        try {
            HttpResult result = call.get();
            report.put(name, result.status() == 200 ? JsonUtil.readValueAsMap(result.body()) : result.body());
        } catch (Exception e) {
            report.put(name, e.toString());
        }
    }

    private void writeLoop(
            HttpClient client,
            StressOptions options,
            Metrics metrics,
            AtomicInteger nextDoc,
            AtomicInteger indexedDocs,
            int writer
    ) {
        while (true) {
            int start = nextDoc.getAndAdd(options.bulkSize());
            if (start >= options.docs()) {
                return;
            }
            int end = Math.min(options.docs(), start + options.bulkSize());
            String body = bulkBody(start, end, writer, options.categories());
            metrics.record("bulk", () -> {
                HttpResult result = post(client, "/_bulk", body, "application/x-ndjson");
                if (result.status() != 200 || Boolean.TRUE.equals(JsonUtil.readValueAsMap(result.body()).get("errors"))) {
                    throw new IllegalStateException("bulk failed: status=" + result.status() + ", body=" + result.body());
                }
                indexedDocs.addAndGet(end - start);
            });
        }
    }

    private void searchLoop(
            HttpClient client,
            StressOptions options,
            Metrics metrics,
            AtomicInteger indexedDocs,
            CountDownLatch writersRemaining,
            Instant deadline,
            int reader
    ) {
        Random random = new Random(31L + reader);
        while (Instant.now().isBefore(deadline) || writersRemaining.getCount() > 0) {
            int choice = random.nextInt(10);
            if (choice < 4) {
                int category = random.nextInt(Math.max(1, options.categories()));
                metrics.record("weak_search", () -> postJson(client, "/stress_books/_search?read_preference=weak", Map.of(
                        "query", Map.of("term", Map.of("category", "cat-" + category)),
                        "size", 10
                )));
            } else if (choice < 7) {
                metrics.record("strong_search", () -> postJson(client, "/stress_books/_search?read_preference=strong", Map.of(
                        "query", Map.of("match_all", Map.of()),
                        "size", 10
                )));
            } else if (choice < 9) {
                metrics.record("knn_search", () -> postJson(client, "/stress_books/_knn_search", Map.of(
                        "field", "embedding",
                        "query_vector", List.of(1.0, 0.0, 0.5),
                        "k", 10,
                        "num_candidates", 100,
                        "filter", Map.of("range", Map.of("pages", Map.of("gte", Math.max(0, indexedDocs.get() / 4))))
                )));
            } else {
                metrics.record("health", () -> get(client, "/_cluster/health"));
            }
        }
    }

    private void waitForStrongVisibility(HttpClient client, int docs) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            HttpResult result = postJson(client, "/stress_books/_search?read_preference=strong", Map.of(
                    "query", Map.of("match_all", Map.of()),
                    "size", 20
            ));
            if (result.status() == 200 && JsonUtil.readValueAsMap(result.body()).get("hits") instanceof List<?> hits
                    && !hits.isEmpty()) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(200);
        }
        throw new AssertionError("strong search did not observe uploaded snapshot for " + docs + " indexed docs");
    }

    private Map<String, Object> indexBody(int shards) {
        return Map.of(
                "settings", Map.of("number_of_shards", shards, "number_of_replicas", 0),
                "mappings", Map.of("properties", Map.of(
                        "title", Map.of("type", "text"),
                        "category", Map.of("type", "keyword"),
                        "pages", Map.of("type", "long"),
                        "price", Map.of("type", "double"),
                        "published_at", Map.of("type", "long"),
                        "available", Map.of("type", "boolean"),
                        "embedding", Map.of("type", "dense_vector", "dimension", 3, "similarity", "cosine")
                ))
        );
    }

    private String bulkBody(int start, int end, int writer, int categories) {
        StringBuilder body = new StringBuilder((end - start) * 192);
        for (int id = start; id < end; id++) {
            body.append("{\"index\":{\"_index\":\"stress_books\",\"_id\":\"doc-")
                    .append(id)
                    .append("\",\"routing\":\"routing-")
                    .append(id)
                    .append("\"}}\n");
            Map<String, Object> source = new HashMap<>();
            source.put("title", "Lucene stress document " + id + " writer " + writer);
            source.put("category", "cat-" + (id % categories));
            source.put("pages", id % 1000);
            source.put("price", (id % 5000) / 10.0);
            source.put("published_at", 20200101L + id);
            source.put("available", id % 2 == 0);
            source.put("embedding", List.of((id % 10) / 10.0, ((id + 3) % 10) / 10.0, 0.5));
            body.append(new String(JsonUtil.writeValueAsBytes(source), StandardCharsets.UTF_8)).append('\n');
        }
        return body.toString();
    }

    private void putJson(HttpClient client, String path, Object body) {
        HttpResult result = request(client, "PUT", path, new String(JsonUtil.writeValueAsBytes(body), StandardCharsets.UTF_8), "application/json");
        if (result.status() >= 300) {
            throw new IllegalStateException("PUT " + path + " failed: status=" + result.status() + ", body=" + result.body());
        }
    }

    private HttpResult postJson(HttpClient client, String path, Object body) {
        HttpResult result = request(client, "POST", path, new String(JsonUtil.writeValueAsBytes(body), StandardCharsets.UTF_8), "application/json");
        if (result.status() >= 300) {
            throw new IllegalStateException("POST " + path + " failed: status=" + result.status() + ", body=" + result.body());
        }
        return result;
    }

    private HttpResult post(HttpClient client, String path, String body, String contentType) {
        return request(client, "POST", path, body, contentType);
    }

    private HttpResult get(HttpClient client, String path) {
        return request(client, "GET", path, null, "application/json");
    }

    private HttpResult getAbsolute(HttpClient client, String uri) {
        return request(client, "GET", URI.create(uri), null, "text/plain");
    }

    private HttpResult request(HttpClient client, String method, String path, String body, String contentType) {
        return request(client, method, URI.create("http://127.0.0.1:" + port + path), body, contentType);
    }

    private HttpResult request(HttpClient client, String method, URI uri, String body, String contentType) {
        try {
            HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
                    .timeout(Duration.ofSeconds(20))
                    .header("content-type", contentType);
            HttpRequest.BodyPublisher publisher = body == null
                    ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofString(body);
            HttpResponse<String> response = client.send(
                    builder.method(method, publisher).build(),
                    HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)
            );
            return new HttpResult(response.statusCode(), response.body());
        } catch (IOException e) {
            throw new IllegalStateException("HTTP request failed: " + method + " " + uri, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("HTTP request interrupted: " + method + " " + uri, e);
        }
    }

    private int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private record HttpResult(int status, String body) {
    }

    private record StressOptions(
            int docs,
            int bulkSize,
            int writeThreads,
            int searchThreads,
            int searchSeconds,
            int shards,
            int categories,
            long cacheMaxBytes,
            int metricsPort
    ) {
        private static final int DEFAULT_CATEGORIES = 16;

        static StressOptions fromEnvironment() {
            return new StressOptions(
                    intEnv("LUCENE_S3_STRESS_DOCS", 2_000),
                    intEnv("LUCENE_S3_STRESS_BULK_SIZE", 50),
                    intEnv("LUCENE_S3_STRESS_WRITE_THREADS", 4),
                    intEnv("LUCENE_S3_STRESS_SEARCH_THREADS", 4),
                    intEnv("LUCENE_S3_STRESS_SECONDS", 10),
                    intEnv("LUCENE_S3_STRESS_SHARDS", 3),
                    intEnv("LUCENE_S3_STRESS_CATEGORIES", DEFAULT_CATEGORIES),
                    longEnv("LUCENE_S3_STRESS_CACHE_MAX_BYTES", 64L * 1024 * 1024),
                    intEnv("LUCENE_S3_STRESS_METRICS_PORT", -1)
            );
        }

        private static int intEnv(String name, int defaultValue) {
            String value = System.getenv(name);
            return value == null || value.isBlank() ? defaultValue : Integer.parseInt(value);
        }

        private static long longEnv(String name, long defaultValue) {
            String value = System.getenv(name);
            return value == null || value.isBlank() ? defaultValue : Long.parseLong(value);
        }
    }

    private static final class Metrics {
        private final Map<String, Metric> metrics = new java.util.concurrent.ConcurrentHashMap<>();

        void record(String name, ThrowingRunnable runnable) {
            Metric metric = metrics.computeIfAbsent(name, ignored -> new Metric());
            long started = System.nanoTime();
            try {
                runnable.run();
                metric.success.increment();
            } catch (Throwable e) {
                metric.errors.increment();
                if (errorSamples.size() < 20) {
                    errorSamples.add(name + ": " + e);
                }
            } finally {
                metric.latenciesNanos.add(System.nanoTime() - started);
            }
        }

        int errors() {
            return metrics.values().stream().mapToInt(metric -> metric.errors.intValue()).sum();
        }

        Map<String, Object> report(StressOptions options, int indexedDocs, int port, int metricsPort) {
            Map<String, Object> report = new HashMap<>();
            report.put("port", port);
            report.put("metrics_port", metricsPort);
            report.put("options", options);
            report.put("indexed_docs", indexedDocs);
            report.put("errors", errors());
            report.put("error_samples", errorSamples);
            Map<String, Object> metricReports = new HashMap<>();
            metrics.forEach((name, metric) -> metricReports.put(name, metric.report()));
            report.put("metrics", metricReports);
            return report;
        }

        private final ConcurrentLinkedQueue<String> errorSamples = new ConcurrentLinkedQueue<>();
    }

    private static final class Metric {
        private final LongAdder success = new LongAdder();
        private final LongAdder errors = new LongAdder();
        private final ConcurrentLinkedQueue<Long> latenciesNanos = new ConcurrentLinkedQueue<>();

        Map<String, Object> report() {
            List<Long> values = new ArrayList<>(latenciesNanos);
            values.sort(Long::compareTo);
            long count = values.size();
            long totalNanos = values.stream().mapToLong(Long::longValue).sum();
            Map<String, Object> report = new HashMap<>();
            report.put("success", success.longValue());
            report.put("errors", errors.longValue());
            report.put("requests", count);
            report.put("avg_ms", count == 0 ? 0 : TimeUnit.NANOSECONDS.toMillis(totalNanos / count));
            report.put("p50_ms", percentile(values, 50));
            report.put("p95_ms", percentile(values, 95));
            report.put("p99_ms", percentile(values, 99));
            report.put("max_ms", values.isEmpty() ? 0 : TimeUnit.NANOSECONDS.toMillis(values.getLast()));
            return report;
        }

        private long percentile(List<Long> values, int percentile) {
            if (values.isEmpty()) {
                return 0;
            }
            int index = Math.min(values.size() - 1, (int) Math.ceil(values.size() * percentile / 100.0) - 1);
            return TimeUnit.NANOSECONDS.toMillis(values.get(index));
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    @FunctionalInterface
    private interface EndpointCall {
        HttpResult get();
    }
}
