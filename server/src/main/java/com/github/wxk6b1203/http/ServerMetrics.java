package com.github.wxk6b1203.http;

import io.prometheus.metrics.core.datapoints.CounterDataPoint;
import io.prometheus.metrics.core.datapoints.GaugeDataPoint;
import io.prometheus.metrics.core.datapoints.DistributionDataPoint;
import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.core.metrics.Histogram;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;

@Slf4j
final class ServerMetrics implements AutoCloseable {
    private final PrometheusRegistry registry = new PrometheusRegistry();
    private final Counter httpRequests;
    private final Histogram httpRequestDuration;
    private final Gauge activeHttpRequests;
    private final Gauge clusterShards;
    private final Gauge uploadShards;
    private final Gauge pits;
    private final Gauge cacheStats;
    private final Gauge cacheHitRate;
    private final Gauge s3Stats;
    private final HTTPServer metricsServer;

    ServerMetrics(int metricsPort) {
        this.httpRequests = Counter.builder()
                .name("lucene_s3_http_requests_total")
                .help("HTTP requests handled by the Lucene S3 API.")
                .labelNames("method", "route", "status")
                .register(registry);
        this.httpRequestDuration = Histogram.builder()
                .name("lucene_s3_http_request_duration_seconds")
                .help("HTTP request duration in seconds.")
                .labelNames("method", "route", "status")
                .classicUpperBounds(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30)
                .register(registry);
        this.activeHttpRequests = Gauge.builder()
                .name("lucene_s3_http_active_requests")
                .help("Currently active HTTP requests.")
                .register(registry);
        this.clusterShards = Gauge.builder()
                .name("lucene_s3_cluster_shards")
                .help("Shard counts by state.")
                .labelNames("state")
                .register(registry);
        this.uploadShards = Gauge.builder()
                .name("lucene_s3_upload_shards")
                .help("Shard upload status counts.")
                .labelNames("state")
                .register(registry);
        this.pits = Gauge.builder()
                .name("lucene_s3_pits")
                .help("Open PIT counts.")
                .labelNames("scope")
                .register(registry);
        this.cacheStats = Gauge.builder()
                .name("lucene_s3_cache_stats")
                .help("Remote cache stats.")
                .labelNames("kind")
                .register(registry);
        this.cacheHitRate = Gauge.builder()
                .name("lucene_s3_cache_hit_rate")
                .help("Remote cache hit rate.")
                .register(registry);
        this.s3Stats = Gauge.builder()
                .name("lucene_s3_s3_stats")
                .help("S3 operation and error counters exported from this node.")
                .labelNames("kind")
                .register(registry);
        JvmMetrics.builder().register(registry);
        this.metricsServer = startMetricsServer(metricsPort);
    }

    void requestStarted() {
        activeHttpRequests.inc(1);
    }

    void requestFinished(String method, String path, int status, long durationNanos) {
        activeHttpRequests.inc(-1);
        String route = normalizePath(path);
        String statusClass = status <= 0 ? "unknown" : (status / 100) + "xx";
        CounterDataPoint counter = httpRequests.labelValues(method, route, statusClass);
        counter.inc();
        DistributionDataPoint duration = httpRequestDuration.labelValues(method, route, statusClass);
        duration.observe(durationNanos / 1_000_000_000.0);
    }

    void setClusterHealth(long activeShards, long unassignedShards, long pendingUploadShards, long stuckUploadShards) {
        clusterShards.labelValues("active").set(activeShards);
        clusterShards.labelValues("unassigned").set(unassignedShards);
        uploadShards.labelValues("pending").set(pendingUploadShards);
        uploadShards.labelValues("stuck").set(stuckUploadShards);
    }

    void setPitCounts(long coordinatingPits, long localPits) {
        pits.labelValues("coordinating").set(coordinatingPits);
        pits.labelValues("local").set(localPits);
    }

    void setCacheStats(Map<String, Object> stats) {
        setGaugeFromNumber(cacheStats.labelValues("hits"), stats.get("hits"));
        setGaugeFromNumber(cacheStats.labelValues("misses"), stats.get("misses"));
        setGaugeFromNumber(cacheStats.labelValues("downloads"), stats.get("downloads"));
        setGaugeFromNumber(cacheStats.labelValues("corruptions"), stats.get("corruptions"));
        setGaugeFromNumber(cacheHitRate, stats.get("hit_rate"));
    }

    void setS3Stats(Map<String, Object> stats) {
        stats.forEach((kind, value) -> setGaugeFromNumber(s3Stats.labelValues(kind), value));
    }

    int metricsPort() {
        return metricsServer == null ? 0 : metricsServer.getPort();
    }

    @Override
    public void close() {
        if (metricsServer != null) {
            metricsServer.close();
        }
    }

    private HTTPServer startMetricsServer(int metricsPort) {
        if (metricsPort <= 0) {
            return null;
        }
        try {
            HTTPServer server = HTTPServer.builder()
                    .port(metricsPort)
                    .registry(registry)
                    .buildAndStart();
            log.info("Prometheus metrics listening on {}", server.getPort());
            return server;
        } catch (IOException e) {
            throw new IllegalStateException("failed to start Prometheus metrics server on port " + metricsPort, e);
        }
    }

    private void setGaugeFromNumber(GaugeDataPoint gauge, Object value) {
        if (value instanceof Number number) {
            gauge.set(number.doubleValue());
        }
    }

    private String normalizePath(String path) {
        if (path == null || path.isBlank()) {
            return "/";
        }
        String[] parts = path.split("/");
        if (parts.length >= 4 && "_internal".equals(parts[1])) {
            if ("_pit".equals(parts[2])) {
                return "/_internal/_pit/:pit";
            }
            if (parts.length >= 5) {
                return "/_internal/:index/:shard/" + parts[4];
            }
            return "/_internal/" + parts[parts.length - 1];
        }
        if (parts.length >= 4 && "_ilm".equals(parts[1]) && "policy".equals(parts[2])) {
            return "/_ilm/policy/:policy";
        }
        if (parts.length >= 3 && parts[1].startsWith("_")) {
            return path;
        }
        if (parts.length == 2 && parts[1].startsWith("_")) {
            return path;
        }
        if (parts.length == 2) {
            return "/:index";
        }
        if (parts.length >= 3) {
            String suffix = parts[2];
            if ("_doc".equals(suffix) && parts.length >= 4) {
                return "/:index/_doc/:id";
            }
            if ("_ilm".equals(suffix) && parts.length >= 5) {
                return "/:index/_ilm/policy/:policy";
            }
            return "/:index/" + suffix;
        }
        return path;
    }
}
