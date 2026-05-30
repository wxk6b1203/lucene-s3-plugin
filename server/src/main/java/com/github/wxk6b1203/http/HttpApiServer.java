package com.github.wxk6b1203.http;

import com.github.wxk6b1203.cluster.*;
import com.github.wxk6b1203.cluster.etcd.EtcdClusterCoordinator;
import com.github.wxk6b1203.cluster.etcd.EtcdClusterStateRepository;
import com.github.wxk6b1203.config.ServerOptions;
import com.github.wxk6b1203.errors.NotMasterException;
import com.github.wxk6b1203.index.*;
import com.github.wxk6b1203.metadata.common.IndexCommitSnapshot;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.metadata.provider.ManifestMetadataManager;
import com.github.wxk6b1203.metadata.provider.etcd.EtcdManifestMetadataManager;
import com.github.wxk6b1203.metadata.provider.mem.MemMockProvider;
import com.github.wxk6b1203.search.*;
import com.github.wxk6b1203.store.directory.RemoteCacheStats;
import com.github.wxk6b1203.store.manifest.ManifestManager;
import com.github.wxk6b1203.store.manifest.ManifestOptions;
import com.github.wxk6b1203.store.manifest.UploadWaitStrategy;
import com.github.wxk6b1203.store.object.LocalFileRemoteObjectStore;
import com.github.wxk6b1203.store.object.RemoteObjectStore;
import com.github.wxk6b1203.store.object.S3RemoteObjectStore;
import com.github.wxk6b1203.util.JsonUtil;
import io.etcd.jetcd.Client;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.LegacyMd5Plugin;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.github.wxk6b1203.http.HttpApiRequestParsing.*;
import static com.github.wxk6b1203.http.HttpApiResponses.*;
import static com.github.wxk6b1203.http.SearchResponseMerger.mergeSearchResponses;

@Slf4j
public class HttpApiServer implements AutoCloseable {
    private static final String FORWARDED_HEADER = "x-lucene-s3-forwarded";
    private static final String OWNER_TERM_HEADER = "x-lucene-s3-owner-term";
    private static final String ALLOCATION_EPOCH_HEADER = "x-lucene-s3-allocation-epoch";
    private static final String DOCUMENT_ID_HEADER = "x-lucene-s3-document-id";
    private static final String REQUEST_START_NANOS = "lucene-s3.request-start-nanos";
    private static final String STAGE_MARK_NANOS = "lucene-s3.stage-mark-nanos";

    private final Vertx vertx;
    private final int port;
    private final ClusterStateRepository clusterStateRepository;
    private final ClusterIndexService indexService;
    private final IndexLifecycleService lifecycleService;
    private final ShardAllocator shardAllocator;
    private final SearchPlanner searchPlanner;
    private final WriteRouter writeRouter;
    private final LocalShardIndexService localShardIndexService;
    private final ClusterCoordinator clusterCoordinator;
    private final Client etcdClient;
    private final ClusterNode localNode;
    private final HttpClient forwardingClient;
    private final ManifestMetadataManager manifestMetadataManager;
    private final RemoteObjectStore remoteObjectStore;
    private final ClusterMaintenanceService maintenanceService;
    private final ClusterIntrospectionHandlers clusterHandlers;
    private final ServerMetrics serverMetrics;
    private final Duration forwardTimeout;
    private final long writeMaintenanceIntervalMillis;
    private final boolean weakRemoteSnapshotReadsEnabled;
    private final Map<String, CoordinatingPit> pits = new ConcurrentHashMap<>();
    private final Set<String> maintenanceTasksRunning = ConcurrentHashMap.newKeySet();
    private final ExecutorService maintenanceExecutor;
    private Long maintenanceTimerId;
    private Long writeMaintenanceTimerId;
    private S3Client s3Client;
    private HttpServer httpServer;

    public HttpApiServer(int port) {
        this(new ServerOptions(
                port,
                "lucene-s3",
                UUID.randomUUID().toString(),
                "local-node",
                "127.0.0.1",
                Set.of(NodeRole.MASTER, NodeRole.DATA, NodeRole.COORDINATING),
                null,
                "lucene-s3/cluster",
                "data",
                null,
                null,
                "https",
                null,
                false,
                null,
                null,
                2
        ));
    }

    public HttpApiServer(ServerOptions options) {
        this.vertx = Vertx.vertx();
        this.port = options.httpPort();
        this.forwardTimeout = Duration.ofSeconds(options.httpForwardTimeoutSeconds());
        this.maintenanceExecutor = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("cluster-maintenance-", 0).factory()
        );
        Set<NodeRole> roles = ensureCoordinatingRole(options.roles());
        this.localNode = new ClusterNode(
                options.nodeId(),
                options.nodeName(),
                options.host(),
                options.httpPort(),
                roles,
                Instant.now()
        );
        this.forwardingClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        this.serverMetrics = new ServerMetrics(options.metricsPort());
        if (options.etcdEnabled()) {
            this.etcdClient = Client.builder().endpoints(options.etcdEndpoints()).build();
            EtcdClusterStateRepository etcdRepository = new EtcdClusterStateRepository(
                    EtcdClusterStateRepository.Options.builder()
                            .clusterName(options.clusterName())
                            .endpoints(options.etcdEndpoints())
                            .namespace(options.etcdNamespace())
                            .operationTimeoutSeconds(options.etcdTimeoutSeconds())
                            .build(),
                    etcdClient
            );
            this.clusterStateRepository = etcdRepository;
            this.clusterCoordinator = new EtcdClusterCoordinator(
                    EtcdClusterCoordinator.Options.builder()
                            .namespace(options.etcdNamespace())
                            .operationTimeoutSeconds(options.etcdTimeoutSeconds())
                            .build(),
                    etcdClient,
                    etcdRepository,
                    etcdRepository,
                    localNode
            );
        } else {
            this.etcdClient = null;
            this.clusterStateRepository = new InMemoryClusterStateRepository(options.clusterName(), localNode);
            this.clusterCoordinator = new NoopClusterCoordinator();
        }
        this.shardAllocator = new BalancedShardAllocator();
        DefaultClusterIndexService clusterIndexService = new DefaultClusterIndexService(
                new MasterOnlyClusterStateRepository(clusterStateRepository, clusterCoordinator),
                shardAllocator
        );
        this.indexService = clusterIndexService;
        this.lifecycleService = clusterIndexService;
        HashShardRouter shardRouter = new HashShardRouter();
        this.searchPlanner = new SearchPlanner(shardRouter);
        this.writeRouter = new WriteRouter(shardRouter);
        this.manifestMetadataManager = options.etcdEnabled()
                ? new EtcdManifestMetadataManager(EtcdManifestMetadataManager.Options.builder()
                .namespace(options.etcdNamespace() + "/manifest")
                .build(), etcdClient)
                : new MemMockProvider();
        Path dataPath = Path.of(options.dataPath());
        this.remoteObjectStore = remoteObjectStore(options, dataPath);
        IndexWriteOptions writeOptions = new IndexWriteOptions(
                options.commitEveryRequest(),
                options.commitAfterDocs(),
                Duration.ofMillis(options.commitIntervalMillis()),
                IndexWriteOptions.RefreshPolicy.parse(options.refreshPolicy()),
                Duration.ofMillis(options.refreshIntervalMillis())
        );
        this.writeMaintenanceIntervalMillis = writeMaintenanceIntervalMillis(writeOptions);
        this.weakRemoteSnapshotReadsEnabled = writeOptions.commitEveryRequest();
        this.localShardIndexService = new LuceneLocalShardIndexService(
                dataPath,
                options.s3Enabled() ? options.s3Bucket() : "lucene-s3",
                this.manifestMetadataManager,
                this.remoteObjectStore,
                new ManifestOptions(
                        options.s3Enabled() ? options.s3Bucket() : "lucene-s3",
                        UploadWaitStrategy.parse(options.uploadWaitStrategy()),
                        Duration.ofSeconds(options.uploadWaitTimeoutSeconds())
                ),
                options.analyzerPluginPath() == null ? null : Path.of(options.analyzerPluginPath()),
                writeOptions
        );
        this.maintenanceService = new ClusterMaintenanceService(
                clusterStateRepository,
                clusterCoordinator,
                localNode,
                localShardIndexService,
                manifestMetadataManager,
                remoteObjectStore,
                Math.max(1, options.snapshotRetainLatest()),
                this::deleteIndexAndData,
                new LocalCacheManager(dataPath, options.cacheMaxBytes(), Duration.ofHours(1)),
                Duration.ofSeconds(options.cacheCleanupIntervalSeconds())
        );
        this.clusterHandlers = new ClusterIntrospectionHandlers(
                clusterStateRepository,
                maintenanceService,
                localNode,
                localShardIndexService,
                manifestMetadataManager,
                serverMetrics,
                pits::size
        );
    }

    private RemoteObjectStore remoteObjectStore(ServerOptions options, Path dataPath) {
        if (!options.s3Enabled()) {
            return new LocalFileRemoteObjectStore(dataPath.resolve("remote-objects"));
        }
        S3ClientBuilder builder = S3Client.builder();
        if (options.s3ContentMd5()) {
            builder.addPlugin(LegacyMd5Plugin.create());
        }
        if (options.s3Region() != null && !options.s3Region().isBlank()) {
            builder.region(Region.of(options.s3Region()));
        }
        if (options.s3Endpoint() != null && !options.s3Endpoint().isBlank()) {
            builder.endpointOverride(s3Endpoint(options.s3Endpoint(), options.s3Protocol()));
        }
        if (options.s3ChunkedEncoding() && isAliyunOssEndpoint(options.s3Endpoint())) {
            log.warn("S3 chunked encoding is enabled for Aliyun OSS endpoint {}. OSS rejects aws-chunked uploads; use --no-s3-chunked-encoding or set s3.chunkedEncoding=false.",
                    options.s3Endpoint());
        }
        builder.serviceConfiguration(S3Configuration.builder()
                .pathStyleAccessEnabled(false)
                .chunkedEncodingEnabled(options.s3ChunkedEncoding())
                .build());
        if (options.s3AccessKey() != null && !options.s3AccessKey().isBlank()
                && options.s3SecretKey() != null && !options.s3SecretKey().isBlank()) {
            builder.credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(options.s3AccessKey(), options.s3SecretKey())
            ));
        }
        this.s3Client = builder.build();
        return new S3RemoteObjectStore(options.s3Bucket(), s3Client);
    }

    private URI s3Endpoint(String endpoint, String protocol) {
        String value = endpoint.trim();
        URI uri = URI.create(value);
        if (uri.getScheme() != null) {
            return uri;
        }
        String scheme = protocol == null || protocol.isBlank() ? "https" : protocol.trim().toLowerCase(Locale.ROOT);
        if (!scheme.equals("http") && !scheme.equals("https")) {
            throw new IllegalArgumentException("s3 protocol must be http or https");
        }
        return URI.create(scheme + "://" + value);
    }

    private boolean isAliyunOssEndpoint(String endpoint) {
        return endpoint != null && endpoint.toLowerCase(Locale.ROOT).contains("aliyuncs.com");
    }

    static long writeMaintenanceIntervalMillis(IndexWriteOptions options) {
        long interval = Long.MAX_VALUE;
        if (!options.commitEveryRequest() && !options.commitInterval().isZero()) {
            interval = Math.min(interval, options.commitInterval().toMillis());
        }
        if (options.refreshPolicy() == IndexWriteOptions.RefreshPolicy.INTERVAL) {
            interval = Math.min(interval, options.refreshInterval().toMillis());
        }
        return interval == Long.MAX_VALUE ? 0 : Math.max(1, interval);
    }

    private void recordMetrics(RoutingContext context) {
        long started = System.nanoTime();
        context.put(REQUEST_START_NANOS, started);
        context.put(STAGE_MARK_NANOS, started);
        serverMetrics.requestStarted();
        context.response().bodyEndHandler(ignored -> serverMetrics.requestFinished(
                context.request().method().name(),
                context.normalizedPath(),
                context.response().getStatusCode(),
                System.nanoTime() - started
        ));
        context.next();
    }

    private void recordBodyReadStage(RoutingContext context) {
        recordStage(context, "body_read");
        context.next();
    }

    private void recordStage(RoutingContext context, String stage) {
        Long mark = context.get(STAGE_MARK_NANOS);
        long now = System.nanoTime();
        if (mark != null && now >= mark) {
            serverMetrics.requestStageFinished(
                    context.request().method().name(),
                    context.normalizedPath(),
                    stage,
                    now - mark
            );
        }
        context.put(STAGE_MARK_NANOS, now);
    }

    private <T> T measured(RoutingContext context, String stage, MeasuredSupplier<T> supplier) throws Exception {
        try {
            return supplier.get();
        } finally {
            recordStage(context, stage);
        }
    }

    private long requestStartedNanos(RoutingContext context) {
        Long started = context.get(REQUEST_START_NANOS);
        return started == null ? System.nanoTime() : started;
    }

    private void jsonMeasured(RoutingContext context, int status, Object body) {
        try {
            json(context, status, body);
        } finally {
            recordStage(context, "response_write");
        }
    }

    public Future<HttpServer> start() {
        Router router = Router.router(vertx);
        router.route().handler(this::recordMetrics);
        router.route().handler(BodyHandler.create());
        router.route().handler(this::recordBodyReadStage);

        router.post("/_internal/:index/:shard/_search").blockingHandler(this::internalShardSearch, false);
        router.post("/_internal/:index/:shard/_pit").blockingHandler(this::internalShardOpenPit, false);
        router.delete("/_internal/_pit/:pit").blockingHandler(this::internalShardClosePit, false);
        router.post("/_internal/:index/:shard/_bulk").blockingHandler(this::internalShardBulk, false);
        router.post("/_internal/:index/:shard/_delete_by_query").blockingHandler(this::internalShardDeleteByQuery, false);
        router.post("/_internal/:index/:shard/_update_by_query").blockingHandler(this::internalShardUpdateByQuery, false);
        router.get("/_cluster/state").blockingHandler(clusterHandlers::clusterState, false);
        router.get("/_cluster/health").blockingHandler(clusterHandlers::clusterHealth, false);
        router.get("/_nodes").blockingHandler(clusterHandlers::nodes, false);
        router.get("/_nodes/stats").blockingHandler(clusterHandlers::nodeStats, false);
        router.get("/_shards").blockingHandler(clusterHandlers::shards, false);
        router.get("/_indices").blockingHandler(clusterHandlers::indices, false);
        router.get("/_snapshot_status").blockingHandler(clusterHandlers::snapshotStatus, false);
        router.get("/_uploads").blockingHandler(clusterHandlers::uploadStatus, false);
        router.post("/_uploads/_retry").blockingHandler(clusterHandlers::retryUploads, false);
        router.post("/_bulk").blockingHandler(this::bulk, false);
        router.delete("/_pit").blockingHandler(this::closePointInTime, false);
        router.put("/:index").blockingHandler(this::createIndex, false);
        router.delete("/:index").blockingHandler(this::deleteIndex, false);
        router.get("/:index").blockingHandler(this::getIndex, false);
        router.get("/:index/_uploads").blockingHandler(clusterHandlers::uploadStatus, false);
        router.post("/:index/_uploads/_retry").blockingHandler(clusterHandlers::retryUploads, false);
        router.get("/:index/_mapping").blockingHandler(this::getMapping, false);
        router.put("/:index/_mapping").blockingHandler(this::putMapping, false);
        router.post("/:index/_bulk").blockingHandler(this::bulk, false);
        router.post("/:index/_doc").blockingHandler(this::indexDocument, false);
        router.post("/:index/_doc/:id").blockingHandler(this::indexDocument, false);
        router.delete("/:index/_doc/:id").blockingHandler(this::deleteDocument, false);
        router.post("/:index/_search").blockingHandler(this::search, false);
        router.post("/:index/_pit").blockingHandler(this::openPointInTime, false);
        router.post("/:index/_search_plan").blockingHandler(this::searchPlan, false);
        router.post("/:index/_knn_search").blockingHandler(this::knnSearch, false);
        router.get("/:index/_write_route").blockingHandler(this::writeRoute, false);
        router.post("/:index/_update_by_query").blockingHandler(this::updateByQuery, false);
        router.post("/:index/_delete_by_query").blockingHandler(this::deleteByQuery, false);
        router.put("/_ilm/policy/:policy").blockingHandler(this::putLifecyclePolicy, false);
        router.put("/:index/_ilm/policy/:policy").blockingHandler(this::attachLifecyclePolicy, false);

        try {
            clusterCoordinator.start();
        } catch (IOException e) {
            return Future.failedFuture(e);
        }
        return vertx.createHttpServer()
                .requestHandler(router)
                .listen(port)
                .onSuccess(server -> {
                    this.httpServer = server;
                    this.maintenanceTimerId = vertx.setPeriodic(1_000, ignored -> maintenanceTick());
                    if (writeMaintenanceIntervalMillis > 0) {
                        this.writeMaintenanceTimerId = vertx.setPeriodic(
                                writeMaintenanceIntervalMillis,
                                ignored -> writeMaintenanceTick()
                        );
                    }
                    log.info("HTTP API listening on {}", port);
                });
    }

    private void createIndex(RoutingContext context) {
        try {
            if (forwardToMasterIfNeeded(context)) {
                return;
            }
            requireMaster();
            String index = context.pathParam("index");
            Map<String, Object> body = bodyAsMap(context);
            int shards = intValue(indexSetting(body,
                    "number_of_shards",
                    "numberOfShards",
                    "settings.number_of_shards",
                    "settings.numberOfShards",
                    "settings.index.number_of_shards",
                    "settings.index.numberOfShards"
            ), 1);
            int redundantCopies = intValue(indexSetting(body,
                    "number_of_replicas",
                    "numberOfReplicas",
                    "settings.number_of_replicas",
                    "settings.numberOfReplicas",
                    "settings.index.number_of_replicas",
                    "settings.index.numberOfReplicas"
            ), 0);
            if (redundantCopies != 0) {
                throw new IllegalArgumentException(
                        "extra shard copies are not supported; committed shard data is recovered from S3"
                );
            }
            String policy = stringValue(indexSetting(body,
                    "lifecycle_policy",
                    "lifecyclePolicy",
                    "settings.lifecycle_policy",
                    "settings.lifecyclePolicy",
                    "settings.index.lifecycle.name",
                    "settings.index.lifecycleName"
            ));
            json(context, 200, indexService.createIndex(new IndexSettings(
                    index,
                    shards,
                    policy,
                    Instant.now(),
                    parseMappings(body)
            )));
        } catch (Exception e) {
            error(context, status(e), e);
        }
    }

    private Object indexSetting(Map<String, Object> body, String... paths) {
        for (String path : paths) {
            Object value = valueAtPath(body, path);
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private Object valueAtPath(Map<String, Object> values, String path) {
        if (values.containsKey(path)) {
            return values.get(path);
        }
        Object current = values;
        String[] parts = path.split("\\.");
        for (int i = 0; i < parts.length; i++) {
            if (!(current instanceof Map<?, ?> map)) {
                return null;
            }
            String remaining = String.join(".", Arrays.copyOfRange(parts, i, parts.length));
            if (map.containsKey(remaining)) {
                return map.get(remaining);
            }
            current = ((Map<String, Object>) map).get(parts[i]);
        }
        return current;
    }

    private void deleteIndex(RoutingContext context) {
        try {
            if (forwardToMasterIfNeeded(context)) {
                return;
            }
            requireMaster();
            String index = context.pathParam("index");
            IndexSettings settings = clusterStateRepository.current().indices().get(index);
            if (settings == null) {
                throw new IllegalArgumentException("index not found: " + index);
            }
            maintenanceService.deleteIndexDataWithScope(index, settings.numberOfShards());
            json(context, 200, clusterStateRepository.current());
        } catch (Exception e) {
            error(context, status(e), e);
        }
    }

    private void getIndex(RoutingContext context) {
        try {
            String index = context.pathParam("index");
            ClusterState state = clusterStateRepository.current();
            IndexSettings settings = state.indices().get(index);
            if (settings == null) {
                throw new IllegalArgumentException("index not found: " + index);
            }
            json(context, 200, Map.of(index, clusterHandlers.indexStats(state, index, settings, true)));
        } catch (Exception e) {
            error(context, status(e), e);
        }
    }

    private void getMapping(RoutingContext context) {
        try {
            String index = context.pathParam("index");
            json(context, 200, Map.of(
                    index,
                    Map.of("mappings", Map.of("properties", indexSettings(index, clusterStateRepository.current()).mappings()))
            ));
        } catch (Exception e) {
            error(context, status(e), e);
        }
    }

    private void putMapping(RoutingContext context) {
        try {
            if (forwardToMasterIfNeeded(context)) {
                return;
            }
            requireMaster();
            json(context, 200, indexService.putMapping(context.pathParam("index"), parseMappings(bodyAsMap(context))));
        } catch (Exception e) {
            error(context, status(e), e);
        }
    }

    private void indexDocument(RoutingContext context) {
        try {
            String index = context.pathParam("index");
            String id = documentId(context);
            String routing = context.queryParams().get("routing");
            if (routing == null || routing.isBlank()) {
                routing = id;
            }
            ClusterState state = writableClusterState(context);
            if (state == null) {
                return;
            }
            IndexSettings settings = indexSettings(index, state);
            WriteRoute route = writeRouter.route(index, routing, state);
            log.debug("index request node={} uri={} forwarded={} routeNode={} shard={} ownerTerm={} epoch={}",
                    localNode.id(),
                    context.request().uri(),
                    context.request().getHeader(FORWARDED_HEADER),
                    route.nodeId(),
                    route.shardId().routeKey(),
                    route.ownerTerm(),
                    route.allocationEpoch());
            if (!localNode.id().equals(route.nodeId())) {
                if (isForwardedShardWrite(context)) {
                    throw new IllegalStateException("forwarded write request did not reach shard owner node: "
                            + route.shardId().routeKey());
                }
                Map<String, String> headers = new HashMap<>(writeFenceHeaders(route));
                headers.put(DOCUMENT_ID_HEADER, id);
                forward(context, route.nodeId(), route.host(), route.httpPort(), headers);
                return;
            }
            if (context.request().getHeader(FORWARDED_HEADER) != null) {
                validateForwardedWriteFence(context, route);
            }
            validateShardWriteFence(route.shardId(), route.ownerTerm(), route.allocationEpoch());
            boolean createOnly = "create".equalsIgnoreCase(context.queryParams().get("op_type"));
            IndexDocumentResponse response = localShardIndexService.index(new IndexDocumentRequest(
                    index,
                    route.shardId(),
                    id,
                    bodyAsMap(context),
                    settings.mappings(),
                    createOnly
            ));
            json(context, 201, response);
        } catch (Exception e) {
            error(context, status(e), e);
        }
    }

    private void deleteDocument(RoutingContext context) {
        try {
            String index = context.pathParam("index");
            String id = context.pathParam("id");
            if (id == null || id.isBlank()) {
                throw new IllegalArgumentException("delete requires document id");
            }
            String routing = context.queryParams().get("routing");
            if (routing == null || routing.isBlank()) {
                routing = id;
            }
            ClusterState state = writableClusterState(context);
            if (state == null) {
                return;
            }
            IndexSettings settings = indexSettings(index, state);
            WriteRoute route = writeRouter.route(index, routing, state);
            if (!localNode.id().equals(route.nodeId())) {
                if (isForwardedShardWrite(context)) {
                    throw new IllegalStateException("forwarded write request did not reach shard owner node: "
                            + route.shardId().routeKey());
                }
                forward(context, route.nodeId(), route.host(), route.httpPort(), writeFenceHeaders(route));
                return;
            }
            if (context.request().getHeader(FORWARDED_HEADER) != null) {
                validateForwardedWriteFence(context, route);
            }
            validateShardWriteFence(route.shardId(), route.ownerTerm(), route.allocationEpoch());
            json(context, 200, localShardIndexService.delete(new IndexDocumentRequest(
                    index,
                    route.shardId(),
                    id,
                    Map.of(),
                    settings.mappings()
            )));
        } catch (Exception e) {
            error(context, status(e), e);
        }
    }

    private void bulk(RoutingContext context) {
        long started = System.nanoTime();
        try {
            List<BulkItemRequest> items = measured(context, "parse", () -> bulkItems(context));
            ClusterState state = measured(context, "cluster_state", () -> writableClusterState(context));
            if (state == null) {
                return;
            }
            List<Future<List<BulkItemResult>>> futures = measured(context, "plan", () -> executeBulkItems(items, state));
            if (futures.isEmpty()) {
                jsonMeasured(context, 200, Map.of(
                        "took", (System.nanoTime() - started) / 1_000_000,
                        "errors", false,
                        "items", List.of()
                ));
                return;
            }
            Future.all(futures)
                    .onSuccess(ignored -> {
                        recordStage(context, "shard_execute");
                        BulkItemResponse[] orderedResponses = new BulkItemResponse[items.size()];
                        futures.stream()
                                .flatMap(future -> future.result().stream())
                                .forEach(result -> orderedResponses[result.ordinal()] = result.response());
                        List<Map<String, Object>> responses = Arrays.stream(orderedResponses)
                                .map(BulkItemResponse::asMap)
                                .toList();
                        boolean errors = Arrays.stream(orderedResponses)
                                .anyMatch(BulkItemResponse::failed);
                        jsonMeasured(context, 200, Map.of(
                                "took", (System.nanoTime() - started) / 1_000_000,
                                "errors", errors,
                                "items", responses
                        ));
                    })
                    .onFailure(e -> {
                        Exception exception = exception(e);
                        error(context, status(exception), exception);
                    });
        } catch (Exception e) {
            error(context, status(e), e);
        }
    }

    private void search(RoutingContext context) {
        try {
            SearchRequest request = measured(context, "parse", () -> searchRequest(context));
            ClusterState state = measured(context, "cluster_state", clusterStateRepository::current);
            SearchRequest parsedRequest = request;
            Map<String, FieldMapping> mappings = measured(context, "mapping", () -> indexSettings(parsedRequest.indexName(), state).mappings());
            SearchRequest validatingRequest = request;
            measured(context, "validate", () -> {
                validateVectorQuery(validatingRequest.vector(), mappings);
                return null;
            });
            request = withMappings(request, mappings);
            SearchRequest mappedRequest = request;
            CoordinatingPit pit = measured(context, "pit_lookup", () -> coordinatingPit(mappedRequest.pitId()));
            if (pit != null && !pit.indexName().equals(request.indexName())) {
                throw new IllegalArgumentException("point in time does not belong to index: " + request.indexName());
            }
            SearchPlan plan = measured(context, "plan", () -> pit == null
                    ? searchPlan(mappedRequest, state)
                    : new SearchPlan(mappedRequest.indexName(), mappedRequest.routing(), state.version(), pit.targets()));
            executeSearchPlan(plan, request, pit, requestStartedNanos(context))
                    .onSuccess(response -> {
                        recordStage(context, "shard_execute");
                        jsonMeasured(context, 200, response);
                    })
                    .onFailure(e -> {
                        Exception exception = exception(e);
                        error(context, status(exception), exception);
                    });
        } catch (Exception e) {
            error(context, status(e), e);
        }
    }

    private void openPointInTime(RoutingContext context) {
        try {
            String index = context.pathParam("index");
            Map<String, Object> body = bodyAsMap(context);
            ClusterState state = clusterStateRepository.current();
            SearchRequest request = new SearchRequest(
                    index,
                    Map.of("match_all", Map.of()),
                    List.of(),
                    null,
                    null,
                    0,
                    1,
                    List.of(),
                    List.of(),
                    null,
                    Map.of(),
                    readPreference(context, body)
            );
            SearchPlan plan = searchPlan(request, state);
            Duration keepAlive = keepAlive(context);
            List<Future<ShardPit>> futures = plan.targets().stream()
                    .map(target -> executeShardOpenPit(target, keepAlive))
                    .toList();
            Future.all(futures)
                    .map(ignored -> {
                        String id = UUID.randomUUID().toString();
                        Map<String, String> shardPitIds = futures.stream()
                                .map(Future::result)
                                .collect(Collectors.toMap(pit -> pit.target().shardId().routeKey(), ShardPit::pitId));
                        pits.put(id, new CoordinatingPit(index, plan.targets(), shardPitIds, Instant.now().plus(keepAlive)));
                        return new PointInTimeResponse(id);
                    })
                    .onSuccess(response -> json(context, 200, response))
                    .onFailure(e -> {
                        Exception exception = exception(e);
                        error(context, status(exception), exception);
                    });
        } catch (Exception e) {
            error(context, status(e), e);
        }
    }

    private void closePointInTime(RoutingContext context) {
        try {
            String id = stringValue(bodyAsMap(context).get("id"));
            if (id == null || id.isBlank()) {
                id = context.queryParams().get("id");
            }
            if (id == null || id.isBlank()) {
                throw new IllegalArgumentException("point in time id is required");
            }
            CoordinatingPit pit = pits.remove(id);
            if (pit == null) {
                json(context, 404, Map.of("succeeded", false));
                return;
            }
            closeCoordinatingPit(pit)
                    .onSuccess(ignored -> json(context, 200, Map.of("succeeded", true)))
                    .onFailure(e -> {
                        Exception exception = exception(e);
                        error(context, status(exception), exception);
                    });
        } catch (Exception e) {
            error(context, status(e), e);
        }
    }

    private void internalShardSearch(RoutingContext context) {
        try {
            String index = context.pathParam("index");
            int shard = Integer.parseInt(context.pathParam("shard"));
            SearchRequest request = internalSearchRequest(context);
            json(context, 200, localShardIndexService.search(new ShardId(index, shard), request));
        } catch (Exception e) {
            error(context, status(e), e);
        }
    }

    private void internalShardOpenPit(RoutingContext context) {
        try {
            String index = context.pathParam("index");
            int shard = Integer.parseInt(context.pathParam("shard"));
            json(context, 200, localShardIndexService.openPointInTime(
                    new ShardId(index, shard),
                    index,
                    keepAlive(context),
                    internalReadPreference(context)
            ));
        } catch (Exception e) {
            error(context, status(e), e);
        }
    }

    private void internalShardClosePit(RoutingContext context) {
        try {
            json(context, 200, Map.of("succeeded", localShardIndexService.closePointInTime(context.pathParam("pit"))));
        } catch (Exception e) {
            error(context, status(e), e);
        }
    }

    private void internalShardBulk(RoutingContext context) {
        try {
            String index = context.pathParam("index");
            int shard = Integer.parseInt(context.pathParam("shard"));
            ShardId shardId = new ShardId(index, shard);
            Map<String, Object> body = bodyAsMap(context);
            Long ownerTerm = longObject(body.get("owner_term"));
            Long allocationEpoch = longObject(body.get("allocation_epoch"));
            validateShardWriteFence(shardId, ownerTerm, allocationEpoch);
            Map<String, FieldMapping> mappings = indexSettings(index, clusterStateRepository.current()).mappings();
            List<Object> requestItems = objectList(body.get("items"));
            List<BulkItemPlan> plans = new ArrayList<>(requestItems.size());
            WriteRoute route = new WriteRoute(
                    shardId,
                    localNode.id(),
                    localNode.host(),
                    localNode.httpPort(),
                    ownerTerm,
                    allocationEpoch
            );
            for (int ordinal = 0; ordinal < requestItems.size(); ordinal++) {
                Object value = requestItems.get(ordinal);
                BulkItemRequest item = internalBulkItem(mapValue(value), index);
                plans.add(new BulkItemPlan(ordinal, item, item.id(), route, mappings));
            }
            List<BulkItemResponse> responses = executeLocalBulkPlans(plans).stream()
                    .map(BulkItemResult::response)
                    .toList();
            json(context, 200, Map.of(
                    "errors", responses.stream().anyMatch(BulkItemResponse::failed),
                    "items", responses.stream().map(BulkItemResponse::asMap).toList()
            ));
        } catch (Exception e) {
            error(context, status(e), e);
        }
    }

    private BulkItemRequest internalBulkItem(Map<String, Object> value, String defaultIndex) {
        String action = stringValue(value.get("action"));
        if (!"index".equals(action) && !"create".equals(action) && !"delete".equals(action)) {
            throw new IllegalArgumentException("unsupported internal bulk action: " + action);
        }
        String index = stringValue(value.get("index"));
        if (index == null || index.isBlank()) {
            index = defaultIndex;
        }
        if (!defaultIndex.equals(index)) {
            throw new IllegalArgumentException("internal bulk item index does not match target shard: " + index);
        }
        String id = stringValue(value.get("id"));
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("internal bulk item requires id");
        }
        String routing = stringValue(value.get("routing"));
        if (routing == null || routing.isBlank()) {
            routing = stringValue(value.get("_routing"));
        }
        return new BulkItemRequest(action, index, id, routing, mapValue(value.get("source")));
    }

    private void internalShardDeleteByQuery(RoutingContext context) {
        try {
            String index = context.pathParam("index");
            int shard = Integer.parseInt(context.pathParam("shard"));
            ShardId shardId = new ShardId(index, shard);
            ByQueryRequest request = byQueryRequest(context);
            validateShardWriteFence(shardId, request.ownerTerm(), request.allocationEpoch());
            json(context, 200, localShardIndexService.deleteByQuery(
                    shardId,
                    request
            ));
        } catch (Exception e) {
            error(context, status(e), e);
        }
    }

    private void internalShardUpdateByQuery(RoutingContext context) {
        try {
            String index = context.pathParam("index");
            int shard = Integer.parseInt(context.pathParam("shard"));
            ShardId shardId = new ShardId(index, shard);
            ByQueryRequest request = byQueryRequest(context);
            validateShardWriteFence(shardId, request.ownerTerm(), request.allocationEpoch());
            json(context, 200, localShardIndexService.updateByQuery(
                    shardId,
                    request
            ));
        } catch (Exception e) {
            error(context, status(e), e);
        }
    }

    private void searchPlan(RoutingContext context) {
        try {
            SearchRequest request = searchRequest(context);
            json(context, 200, searchPlan(request, clusterStateRepository.current()));
        } catch (Exception e) {
            error(context, status(e), e);
        }
    }

    private Future<SearchResponse> executeSearchPlan(SearchPlan plan, SearchRequest request) {
        return executeSearchPlan(plan, request, System.nanoTime());
    }

    private Future<SearchResponse> executeSearchPlan(SearchPlan plan, SearchRequest request, long started) {
        return executeSearchPlan(plan, request, null, started);
    }

    private Future<SearchResponse> executeSearchPlan(SearchPlan plan, SearchRequest request, CoordinatingPit pit) {
        return executeSearchPlan(plan, request, pit, System.nanoTime());
    }

    private Future<SearchResponse> executeSearchPlan(
            SearchPlan plan,
            SearchRequest request,
            CoordinatingPit pit,
            long started
    ) {
        List<SnapshotPin> snapshotPins = pit == null ? pinRemoteSearchSnapshots(plan) : List.of();
        VectorQuery vector = request.vector();
        int shardSize = request.searchAfter().isEmpty()
                ? Math.max(0, request.from()) + Math.max(0, request.size())
                : Math.max(0, request.size());
        if (vector != null) {
            int shardK = Math.max(Math.max(1, request.size()), vector.numCandidates());
            vector = new VectorQuery(vector.field(), vector.vector(), shardK, vector.numCandidates(), vector.minScore());
            shardSize = shardK;
        }
        SearchRequest shardRequest = new SearchRequest(
                request.indexName(),
                request.query(),
                request.aggregations(),
                vector,
                request.routing(),
                0,
                shardSize,
                request.sort(),
                request.searchAfter(),
                null,
                request.mappings(),
                request.readPreference(),
                request.remoteSnapshotGeneration()
        );
        List<Future<SearchResponse>> futures = plan.targets().stream()
                .map(target -> executeShardSearch(target, withPit(shardRequest, pit, target)))
                .toList();
        if (futures.isEmpty()) {
            releaseSnapshotPins(snapshotPins);
            return Future.succeededFuture(new SearchResponse(elapsedMillis(started), 0, 0, 0, List.of(), Map.of(), List.of()));
        }
        return Future.all(futures)
                .map(ignored -> mergeSearchResponses(futures.stream().map(Future::result).toList(), request, started))
                .andThen(ignored -> releaseSnapshotPins(snapshotPins));
    }

    private long elapsedMillis(long started) {
        long elapsedNanos = Math.max(0, System.nanoTime() - started);
        return elapsedNanos == 0 ? 0 : Math.max(1, (elapsedNanos + 999_999) / 1_000_000);
    }

    private List<SnapshotPin> pinRemoteSearchSnapshots(SearchPlan plan) {
        List<SnapshotPin> pins = new ArrayList<>();
        Instant expiresAt = Instant.now().plus(forwardTimeout.multipliedBy(2).plusSeconds(30));
        for (SearchShardTarget target : plan.targets()) {
            Long generation = target.remoteSnapshotGeneration();
            if (!target.remoteSnapshot() || generation == null || generation < 0) {
                continue;
            }
            String physicalIndexName = physicalIndexName(target.shardId());
            if (manifestMetadataManager.snapshot(physicalIndexName, generation) == null) {
                continue;
            }
            String pinId = "search-" + UUID.randomUUID();
            manifestMetadataManager.pinSnapshot(physicalIndexName, generation, pinId, expiresAt.toEpochMilli());
            pins.add(new SnapshotPin(physicalIndexName, pinId));
        }
        return pins;
    }

    private void releaseSnapshotPins(List<SnapshotPin> pins) {
        for (SnapshotPin pin : pins) {
            try {
                manifestMetadataManager.releaseSnapshotPin(pin.indexName(), pin.pinId());
            } catch (Exception e) {
                log.debug("failed to release search snapshot pin {}/{}", pin.indexName(), pin.pinId(), e);
            }
        }
    }

    private SearchPlan searchPlan(SearchRequest request, ClusterState state) {
        SearchPlan basePlan = searchPlanner.plan(request, state);
        if (request.readPreference().equalsIgnoreCase("owner") || request.readPreference().equalsIgnoreCase("remote")) {
            return basePlan;
        }
        List<ClusterNode> dataNodes = liveDataNodes(state);
        if (dataNodes.isEmpty()) {
            throw new IllegalStateException("no live data node is available for search");
        }
        Map<String, Integer> load = new HashMap<>();
        dataNodes.forEach(node -> load.put(node.id(), 0));
        List<SearchShardTarget> targets = basePlan.targets().stream()
                .map(target -> hybridTarget(target, request.readPreference(), state, dataNodes, load))
                .toList();
        return new SearchPlan(basePlan.indexName(), basePlan.routing(), basePlan.clusterStateVersion(), targets);
    }

    private SearchShardTarget hybridTarget(
            SearchShardTarget base,
            String consistency,
            ClusterState state,
            List<ClusterNode> dataNodes,
            Map<String, Integer> load
    ) {
        ShardRouting routing = routingFor(base.shardId(), state);
        IndexCommitSnapshot snapshot = latestRemoteSnapshot(base.shardId());
        if (consistency.equalsIgnoreCase("strong")
                || (weakRemoteSnapshotReadsEnabled && remoteSnapshotReady(base.shardId(), snapshot))) {
            ClusterNode node = leastLoaded(dataNodes, load);
            load.merge(node.id(), 1, Integer::sum);
            return new SearchShardTarget(
                    base.shardId(),
                    node.id(),
                    node.host(),
                    node.httpPort(),
                    routing.ownerTerm(),
                    routing.allocationEpoch(),
                    true,
                    snapshot == null ? -1L : snapshot.getGeneration()
            );
        }
        ClusterNode owner = state.nodes().get(routing.nodeId());
        if (owner == null || !owner.roles().contains(NodeRole.DATA)) {
            throw new IllegalStateException(
                    "weak read requires live shard owner when remote snapshot is not ready: "
                            + base.shardId().routeKey()
            );
        }
        return new SearchShardTarget(
                base.shardId(),
                owner.id(),
                owner.host(),
                owner.httpPort(),
                routing.ownerTerm(),
                routing.allocationEpoch(),
                false,
                null
        );
    }

    private boolean remoteSnapshotReady(ShardId shardId) {
        return remoteSnapshotReady(shardId, latestRemoteSnapshot(shardId));
    }

    private boolean remoteSnapshotReady(ShardId shardId, IndexCommitSnapshot snapshot) {
        if (snapshot == null) {
            return false;
        }
        String physicalIndexName = physicalIndexName(shardId);
        return manifestMetadataManager.listAll(physicalIndexName, List.of(
                        IndexFileStatus.DIRTY,
                        IndexFileStatus.UPLOADING,
                        IndexFileStatus.CLEAN,
                        IndexFileStatus.PINNED
                ))
                .stream()
                .noneMatch(metadata -> metadata.getStatus() == IndexFileStatus.DIRTY
                        || metadata.getStatus() == IndexFileStatus.UPLOADING);
    }

    private IndexCommitSnapshot latestRemoteSnapshot(ShardId shardId) {
        return manifestMetadataManager.latestSnapshot(physicalIndexName(shardId));
    }

    private ShardRouting routingFor(ShardId shardId, ClusterState state) {
        return state.routingTable().stream()
                .filter(routing -> routing.shardId().equals(shardId))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("shard routing not found: " + shardId.routeKey()));
    }

    private ClusterState writableClusterState(RoutingContext context) throws IOException {
        ClusterState state = clusterStateRepository.current();
        log.debug("writable state node={} version={} master={} hasUnavailable={}",
                localNode.id(), state.version(), state.masterNodeId(), hasUnavailableShardOwner(state));
        if (!hasUnavailableShardOwner(state)) {
            return state;
        }
        if (clusterCoordinator.isMaster()) {
            log.debug("writable state rebalance start node={}", localNode.id());
            return clusterStateRepository.update(shardAllocator::rebalance);
        }
        if (context != null && forwardToMasterIfNeeded(context)) {
            return null;
        }
        throw new NotMasterException("stale shard owner routing must be rerouted by the current master node");
    }

    private boolean hasUnavailableShardOwner(ClusterState state) {
        return state.routingTable().stream()
                .filter(routing -> routing.state() == ShardState.STARTED)
                .map(ShardRouting::nodeId)
                .anyMatch(nodeId -> nodeId == null || !state.nodes().containsKey(nodeId));
    }

    private Map<String, String> writeFenceHeaders(WriteRoute route) {
        return Map.of(
                OWNER_TERM_HEADER, Long.toString(route.ownerTerm()),
                ALLOCATION_EPOCH_HEADER, Long.toString(route.allocationEpoch())
        );
    }

    private boolean isForwardedShardWrite(RoutingContext context) {
        return context.request().getHeader(FORWARDED_HEADER) != null
                && context.request().getHeader(OWNER_TERM_HEADER) != null
                && context.request().getHeader(ALLOCATION_EPOCH_HEADER) != null;
    }

    private String documentId(RoutingContext context) {
        String id = context.pathParam("id");
        if (id != null && !id.isBlank()) {
            return id;
        }
        if (context.request().getHeader(FORWARDED_HEADER) != null) {
            id = context.request().getHeader(DOCUMENT_ID_HEADER);
            if (id == null || id.isBlank()) {
                throw new IllegalArgumentException("missing forwarded document id");
            }
            return id;
        }
        return UUID.randomUUID().toString();
    }

    private List<BulkItemRequest> bulkItems(RoutingContext context) {
        String body = bodyAsString(context);
        if (body == null || body.isBlank()) {
            return List.of();
        }
        String defaultIndex = context.pathParam("index");
        List<BulkItemRequest> items = new ArrayList<>();
        String[] lines = body.split("\\r?\\n");
        for (int line = 0; line < lines.length; line++) {
            String actionLine = lines[line].trim();
            if (actionLine.isEmpty()) {
                continue;
            }
            Map<String, Object> action = JsonUtil.readValueAsMap(actionLine);
            if (action.size() != 1) {
                throw new IllegalArgumentException("bulk action line must contain exactly one action at line " + (line + 1));
            }
            String actionName = action.keySet().iterator().next();
            if (!actionName.equals("index") && !actionName.equals("create") && !actionName.equals("delete")) {
                throw new IllegalArgumentException("unsupported bulk action: " + actionName);
            }
            Map<String, Object> metadata = mapValue(action.get(actionName));
            String index = stringValue(metadata.get("_index"));
            if (index == null || index.isBlank()) {
                index = defaultIndex;
            }
            if (index == null || index.isBlank()) {
                throw new IllegalArgumentException("bulk action requires _index at line " + (line + 1));
            }
            String id = stringValue(metadata.get("_id"));
            String routing = stringValue(metadata.get("routing"));
            if (routing == null || routing.isBlank()) {
                routing = stringValue(metadata.get("_routing"));
            }
            Map<String, Object> source = Map.of();
            if (!actionName.equals("delete")) {
                if (++line >= lines.length || lines[line].isBlank()) {
                    throw new IllegalArgumentException("bulk action requires source after line " + line);
                }
                source = JsonUtil.readValueAsMap(lines[line]);
            }
            items.add(new BulkItemRequest(actionName, index, id, routing, source));
        }
        return items;
    }

    private List<Future<List<BulkItemResult>>> executeBulkItems(List<BulkItemRequest> items, ClusterState state) {
        List<Future<List<BulkItemResult>>> futures = new ArrayList<>();
        Map<BulkShardBatchKey, List<BulkItemPlan>> localBatches = new LinkedHashMap<>();
        Map<BulkShardBatchKey, List<BulkItemPlan>> remoteBatches = new LinkedHashMap<>();
        for (int ordinal = 0; ordinal < items.size(); ordinal++) {
            BulkItemRequest item = items.get(ordinal);
            try {
                BulkItemPlan plan = bulkItemPlan(ordinal, item, state);
                BulkShardBatchKey key = bulkShardBatchKey(plan);
                if (localNode.id().equals(plan.route().nodeId())) {
                    localBatches.computeIfAbsent(key, ignored -> new ArrayList<>()).add(plan);
                    continue;
                }
                remoteBatches.computeIfAbsent(key, ignored -> new ArrayList<>()).add(plan);
            } catch (Exception e) {
                futures.add(Future.succeededFuture(List.of(new BulkItemResult(
                        ordinal,
                        bulkError(item, item.id(), status(e), e)
                ))));
            }
        }
        remoteBatches.forEach((key, plans) -> futures.add(executeRemoteBulkBatch(key, plans)));
        localBatches.forEach((key, plans) -> futures.add(Future.succeededFuture(executeLocalBulkBatch(key, plans))));
        return futures;
    }

    private BulkShardBatchKey bulkShardBatchKey(BulkItemPlan plan) {
        return new BulkShardBatchKey(
                plan.route().nodeId(),
                plan.route().host(),
                plan.route().httpPort(),
                plan.route().shardId(),
                plan.route().ownerTerm(),
                plan.route().allocationEpoch()
        );
    }

    private BulkItemPlan bulkItemPlan(int ordinal, BulkItemRequest item, ClusterState state) {
        String id = item.id();
        if (item.action().equals("delete")) {
            if (id == null || id.isBlank()) {
                throw new IllegalArgumentException("bulk delete requires _id");
            }
        } else if (id == null || id.isBlank()) {
            id = UUID.randomUUID().toString();
        }
        IndexSettings settings = indexSettings(item.index(), state);
        WriteRoute route = writeRouter.route(item.index(), routingOrId(item.routing(), id), state);
        return new BulkItemPlan(ordinal, item, id, route, settings.mappings());
    }

    private List<BulkItemResult> executeLocalBulkBatch(BulkShardBatchKey key, List<BulkItemPlan> plans) {
        try {
            validateShardWriteFence(key.shardId(), key.ownerTerm(), key.allocationEpoch());
            return executeLocalBulkPlans(plans);
        } catch (Exception e) {
            return plans.stream()
                    .map(plan -> new BulkItemResult(plan.ordinal(), bulkError(plan.item(), plan.id(), status(e), e)))
                    .toList();
        }
    }

    private List<BulkItemResult> executeLocalBulkPlans(List<BulkItemPlan> plans) {
        try {
            List<IndexDocumentOperation> operations = plans.stream()
                    .map(plan -> new IndexDocumentOperation(
                            plan.item().action(),
                            new IndexDocumentRequest(
                                    plan.item().index(),
                                    plan.route().shardId(),
                                    plan.id(),
                                    plan.item().source(),
                                    plan.mappings(),
                                    plan.item().action().equals("create")
                            )
                    ))
                    .toList();
            List<IndexDocumentOperationResult> results = localShardIndexService.bulk(operations);
            if (results.size() != plans.size()) {
                throw new IllegalStateException("local bulk result count mismatch: expected "
                        + plans.size() + ", actual " + results.size());
            }
            List<BulkItemResult> bulkResults = new ArrayList<>(plans.size());
            for (int i = 0; i < plans.size(); i++) {
                BulkItemPlan plan = plans.get(i);
                IndexDocumentOperationResult result = results.get(i);
                if (result.failed()) {
                    bulkResults.add(new BulkItemResult(
                            plan.ordinal(),
                            bulkError(plan.item(), plan.id(), status(result.failure()), result.failure())
                    ));
                } else {
                    int successStatus = plan.item().action().equals("delete") ? 200 : 201;
                    bulkResults.add(new BulkItemResult(
                            plan.ordinal(),
                            bulkSuccess(plan.item(), result.response(), successStatus)
                    ));
                }
            }
            return bulkResults;
        } catch (Exception e) {
            return plans.stream()
                    .map(plan -> new BulkItemResult(plan.ordinal(), bulkError(plan.item(), plan.id(), status(e), e)))
                    .toList();
        }
    }

    private Future<List<BulkItemResult>> executeRemoteBulkBatch(BulkShardBatchKey key, List<BulkItemPlan> plans) {
        String uri = "/_internal/" + urlPart(key.shardId().indexName())
                + "/" + key.shardId().shardNumber()
                + "/_bulk";
        return remoteJsonRequest("POST", key.host(), key.httpPort(), uri, internalBulkBody(key, plans))
                .map(response -> remoteBulkBatchResponse(key, plans, response.statusCode(), response.body()))
                .recover(e -> Future.succeededFuture(plans.stream()
                        .map(plan -> new BulkItemResult(plan.ordinal(), bulkError(plan.item(), plan.id(), 502, exception(e))))
                        .toList()));
    }

    private Map<String, Object> internalBulkBody(BulkShardBatchKey key, List<BulkItemPlan> plans) {
        return Map.of(
                "owner_term", key.ownerTerm(),
                "allocation_epoch", key.allocationEpoch(),
                "items", plans.stream().map(plan -> {
                    Map<String, Object> item = new LinkedHashMap<>();
                    item.put("action", plan.item().action());
                    item.put("index", plan.item().index());
                    item.put("id", plan.id());
                    if (plan.item().routing() != null && !plan.item().routing().isBlank()) {
                        item.put("routing", plan.item().routing());
                    }
                    item.put("source", plan.item().source());
                    return item;
                }).toList()
        );
    }

    private List<BulkItemResult> remoteBulkBatchResponse(
            BulkShardBatchKey key,
            List<BulkItemPlan> plans,
            int status,
            String body
    ) {
        if (status >= 300) {
            Exception exception = new IllegalStateException("remote shard bulk failed on " + key.nodeId()
                    + ": status=" + status
                    + ", body=" + (body == null ? "" : body));
            return plans.stream()
                    .map(plan -> new BulkItemResult(plan.ordinal(), bulkError(plan.item(), plan.id(), status, exception)))
                    .toList();
        }
        Map<String, Object> response = body == null || body.isBlank() ? Map.of() : JsonUtil.readValueAsMap(body);
        List<Object> items = objectList(response.get("items"));
        List<BulkItemResult> results = new ArrayList<>(plans.size());
        for (int i = 0; i < plans.size(); i++) {
            BulkItemPlan plan = plans.get(i);
            if (i >= items.size()) {
                results.add(new BulkItemResult(
                        plan.ordinal(),
                        bulkError(plan.item(), plan.id(), 502, new IllegalStateException("remote bulk response is missing item"))
                ));
                continue;
            }
            results.add(new BulkItemResult(plan.ordinal(), bulkResponseFromMap(plan, mapValue(items.get(i)))));
        }
        return results;
    }

    private BulkItemResponse bulkResponseFromMap(BulkItemPlan plan, Map<String, Object> response) {
        BulkItemRequest item = plan.item();
        Object actionBody = response.get(item.action());
        if (!(actionBody instanceof Map<?, ?>)) {
            return bulkError(
                    item,
                    plan.id(),
                    502,
                    new IllegalStateException("remote bulk response missing action item: " + item.action())
            );
        }
        Map<String, Object> body = mapValue(actionBody);
        Integer status = intObject(body.get("status"));
        if (status == null) {
            return bulkError(
                    item,
                    plan.id(),
                    502,
                    new IllegalStateException("remote bulk response missing item status: " + item.action())
            );
        }
        Map<String, Object> error = body.get("error") instanceof Map<?, ?> ? mapValue(body.get("error")) : null;
        if (status >= 300 && error == null) {
            error = Map.of(
                    "type", "RemoteBulkItemException",
                    "reason", "remote bulk item failed without error body"
            );
        }
        return new BulkItemResponse(
                item.action(),
                stringValueOrDefault(body.get("_index"), item.index()),
                stringValueOrDefault(body.get("_id"), plan.id()),
                status,
                stringValue(body.get("result")),
                body.get("shardId"),
                error
        );
    }

    private BulkItemResponse bulkSuccess(BulkItemRequest item, IndexDocumentResponse response, int status) {
        return new BulkItemResponse(
                item.action(),
                response.indexName(),
                response.id(),
                status,
                response.result(),
                response.shardId(),
                null
        );
    }

    private BulkItemResponse bulkError(BulkItemRequest item, String id, int status, Exception e) {
        return new BulkItemResponse(
                item.action(),
                item.index(),
                id,
                status,
                null,
                null,
                Map.of(
                        "type", e.getClass().getSimpleName(),
                        "reason", e.getMessage() == null ? "" : e.getMessage()
                )
        );
    }

    private String stringValueOrDefault(Object value, String defaultValue) {
        String string = stringValue(value);
        return string == null || string.isBlank() ? defaultValue : string;
    }

    private String routingOrId(String routing, String id) {
        return routing == null || routing.isBlank() ? id : routing;
    }

    private String urlPart(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private void validateForwardedWriteFence(RoutingContext context, WriteRoute route) {
        long ownerTerm = requiredLongHeader(context, OWNER_TERM_HEADER);
        long allocationEpoch = requiredLongHeader(context, ALLOCATION_EPOCH_HEADER);
        if (ownerTerm != route.ownerTerm() || allocationEpoch != route.allocationEpoch()) {
            throw new IllegalStateException(
                    "stale forwarded write route for shard " + route.shardId().routeKey()
                            + ": expected ownerTerm=" + ownerTerm
                            + ", allocationEpoch=" + allocationEpoch
                            + ", current ownerTerm=" + route.ownerTerm()
                            + ", allocationEpoch=" + route.allocationEpoch()
            );
        }
    }

    private long requiredLongHeader(RoutingContext context, String headerName) {
        String value = context.request().getHeader(headerName);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("missing write fence header: " + headerName);
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid write fence header " + headerName + ": " + value, e);
        }
    }

    private void validateShardWriteFence(ShardId shardId, Long ownerTerm, Long allocationEpoch) throws IOException {
        if (ownerTerm == null || allocationEpoch == null) {
            throw new IllegalArgumentException("missing shard write fence: " + shardId.routeKey());
        }
        validateShardWriteFence(shardId, ownerTerm.longValue(), allocationEpoch.longValue());
    }

    private void validateShardWriteFence(ShardId shardId, long ownerTerm, long allocationEpoch) throws IOException {
        ShardRouting routing = routingFor(shardId, clusterStateRepository.current());
        if (routing.state() != ShardState.STARTED) {
            throw new IllegalStateException("shard is not writable: " + shardId.routeKey() + " state=" + routing.state());
        }
        if (!localNode.id().equals(routing.nodeId())) {
            throw new IllegalStateException(
                    "local node does not own writable shard " + shardId.routeKey()
                            + ": owner=" + routing.nodeId()
                            + ", local=" + localNode.id()
            );
        }
        if (routing.ownerTerm() != ownerTerm || routing.allocationEpoch() != allocationEpoch) {
            throw new IllegalStateException(
                    "stale shard write fence for " + shardId.routeKey()
                            + ": expected ownerTerm=" + ownerTerm
                            + ", allocationEpoch=" + allocationEpoch
                            + ", current ownerTerm=" + routing.ownerTerm()
                            + ", allocationEpoch=" + routing.allocationEpoch()
            );
        }
    }

    private List<ClusterNode> liveDataNodes(ClusterState state) {
        return state.nodes().values().stream()
                .filter(node -> node.roles().contains(NodeRole.DATA))
                .sorted(java.util.Comparator.comparing(ClusterNode::id))
                .toList();
    }

    private ClusterNode leastLoaded(List<ClusterNode> dataNodes, Map<String, Integer> load) {
        return dataNodes.stream()
                .min(java.util.Comparator
                        .comparing((ClusterNode node) -> load.getOrDefault(node.id(), 0))
                        .thenComparing(ClusterNode::id))
                .orElseThrow();
    }

    private String physicalIndexName(ShardId shardId) {
        return shardId.indexName() + "__shard_" + shardId.shardNumber();
    }

    private SearchRequest withPit(SearchRequest request, CoordinatingPit pit, SearchShardTarget target) {
        if (pit == null) {
            return request;
        }
        return new SearchRequest(
                request.indexName(),
                request.query(),
                request.aggregations(),
                request.vector(),
                request.routing(),
                request.from(),
                request.size(),
                request.sort(),
                request.searchAfter(),
                pit.shardPitIds().get(target.shardId().routeKey()),
                request.mappings(),
                request.readPreference(),
                request.remoteSnapshotGeneration()
        );
    }

    private Future<SearchResponse> executeShardSearch(SearchShardTarget target, SearchRequest request) {
        if (target.remoteSnapshot()) {
            request = withRemoteSnapshot(request, target.remoteSnapshotGeneration());
        }
        if (localNode.id().equals(target.nodeId())) {
            try {
                return Future.succeededFuture(localShardIndexService.search(target.shardId(), request));
            } catch (Exception e) {
                return Future.failedFuture(e);
            }
        }
        String uri = "/_internal/" + urlPart(target.shardId().indexName()) + "/" + target.shardId().shardNumber() + "/_search";
        return remoteJsonRequest("POST", target.host(), target.httpPort(), uri, request)
                .map(response -> {
                    if (response.statusCode() >= 300) {
                        throw new IllegalStateException("remote shard search failed on " + target.nodeId()
                                + ": status=" + response.statusCode()
                                + ", body=" + response.body());
                    }
                    return JsonUtil.readValue(response.body(), SearchResponse.class);
                });
    }

    private SearchRequest withReadPreference(SearchRequest request, String readPreference) {
        return new SearchRequest(
                request.indexName(),
                request.query(),
                request.aggregations(),
                request.vector(),
                request.routing(),
                request.from(),
                request.size(),
                request.sort(),
                request.searchAfter(),
                request.pitId(),
                request.mappings(),
                readPreference,
                request.remoteSnapshotGeneration()
        );
    }

    private SearchRequest withRemoteSnapshot(SearchRequest request, Long remoteSnapshotGeneration) {
        return new SearchRequest(
                request.indexName(),
                request.query(),
                request.aggregations(),
                request.vector(),
                request.routing(),
                request.from(),
                request.size(),
                request.sort(),
                request.searchAfter(),
                request.pitId(),
                request.mappings(),
                "remote",
                remoteSnapshotGeneration
        );
    }

    private Future<ShardPit> executeShardOpenPit(SearchShardTarget target, Duration keepAlive) {
        String readPreference = target.remoteSnapshot() ? "remote" : "weak";
        if (localNode.id().equals(target.nodeId())) {
            try {
                PointInTimeResponse response = localShardIndexService.openPointInTime(
                        target.shardId(),
                        target.shardId().indexName(),
                        keepAlive,
                        readPreference
                );
                return Future.succeededFuture(new ShardPit(target, response.id()));
            } catch (Exception e) {
                return Future.failedFuture(e);
            }
        }
        String uri = "/_internal/" + urlPart(target.shardId().indexName()) + "/" + target.shardId().shardNumber()
                + "/_pit?keep_alive=" + keepAlive.toMillis() + "ms"
                + "&read_preference=" + readPreference;
        return remoteJsonRequest("POST", target.host(), target.httpPort(), uri, null)
                .map(response -> {
                    if (response.statusCode() >= 300) {
                        throw new IllegalStateException("remote shard pit open failed on " + target.nodeId()
                                + ": status=" + response.statusCode()
                                + ", body=" + response.body());
                    }
                    return new ShardPit(target, JsonUtil.readValue(response.body(), PointInTimeResponse.class).id());
                });
    }

    private Future<Boolean> executeShardClosePit(SearchShardTarget target, String pitId) {
        if (pitId == null || pitId.isBlank()) {
            return Future.succeededFuture(false);
        }
        if (localNode.id().equals(target.nodeId())) {
            try {
                return Future.succeededFuture(localShardIndexService.closePointInTime(pitId));
            } catch (Exception e) {
                return Future.failedFuture(e);
            }
        }
        return remoteJsonRequest("DELETE", target.host(), target.httpPort(), "/_internal/_pit/" + pitId, null)
                .map(response -> {
                    if (response.statusCode() >= 300) {
                        throw new IllegalStateException("remote shard pit close failed on " + target.nodeId()
                                + ": status=" + response.statusCode()
                                + ", body=" + response.body());
                    }
                    return true;
                });
    }

    private Future<Void> closeCoordinatingPit(CoordinatingPit pit) {
        List<Future<Boolean>> futures = pit.targets().stream()
                .map(target -> executeShardClosePit(target, pit.shardPitIds().get(target.shardId().routeKey())))
                .toList();
        if (futures.isEmpty()) {
            return Future.succeededFuture();
        }
        return Future.all(futures).mapEmpty();
    }

    private Future<ByQueryResponse> executeDeleteByQueryPlan(SearchPlan plan, ByQueryRequest request) {
        List<Future<ByQueryResponse>> futures = plan.targets().stream()
                .map(target -> executeShardDeleteByQuery(target, request))
                .toList();
        if (futures.isEmpty()) {
            return Future.succeededFuture(new ByQueryResponse(null, "delete_by_query", "deleted=0"));
        }
        return Future.all(futures)
                .map(ignored -> {
                    long deleted = futures.stream()
                            .map(Future::result)
                            .map(ByQueryResponse::status)
                            .mapToLong(this::deletedCount)
                            .sum();
                    return new ByQueryResponse(null, "delete_by_query", "deleted=" + deleted);
                });
    }

    private Future<ByQueryResponse> executeUpdateByQueryPlan(SearchPlan plan, ByQueryRequest request) {
        List<Future<ByQueryResponse>> futures = plan.targets().stream()
                .map(target -> executeShardUpdateByQuery(target, request))
                .toList();
        if (futures.isEmpty()) {
            return Future.succeededFuture(new ByQueryResponse(null, "update_by_query", "updated=0"));
        }
        return Future.all(futures)
                .map(ignored -> {
                    long updated = futures.stream()
                            .map(Future::result)
                            .map(ByQueryResponse::status)
                            .mapToLong(status -> count(status, "updated="))
                            .sum();
                    return new ByQueryResponse(null, "update_by_query", "updated=" + updated);
                });
    }

    private Future<ByQueryResponse> executeShardUpdateByQuery(SearchShardTarget target, ByQueryRequest request) {
        ByQueryRequest fencedRequest = withFence(request, target.ownerTerm(), target.allocationEpoch());
        if (localNode.id().equals(target.nodeId())) {
            try {
                validateShardWriteFence(target.shardId(), fencedRequest.ownerTerm(), fencedRequest.allocationEpoch());
                return Future.succeededFuture(localShardIndexService.updateByQuery(target.shardId(), fencedRequest));
            } catch (Exception e) {
                return Future.failedFuture(e);
            }
        }
        String uri = "/_internal/" + urlPart(target.shardId().indexName()) + "/" + target.shardId().shardNumber() + "/_update_by_query";
        return remoteJsonRequest("POST", target.host(), target.httpPort(), uri, byQueryBody(fencedRequest))
                .map(response -> {
                    if (response.statusCode() >= 300) {
                        throw new IllegalStateException("remote shard update_by_query failed on " + target.nodeId()
                                + ": status=" + response.statusCode()
                                + ", body=" + response.body());
                    }
                    return JsonUtil.readValue(response.body(), ByQueryResponse.class);
                });
    }


    private Future<ByQueryResponse> executeShardDeleteByQuery(SearchShardTarget target, ByQueryRequest request) {
        ByQueryRequest fencedRequest = withFence(request, target.ownerTerm(), target.allocationEpoch());
        if (localNode.id().equals(target.nodeId())) {
            try {
                validateShardWriteFence(target.shardId(), fencedRequest.ownerTerm(), fencedRequest.allocationEpoch());
                return Future.succeededFuture(localShardIndexService.deleteByQuery(target.shardId(), fencedRequest));
            } catch (Exception e) {
                return Future.failedFuture(e);
            }
        }
        String uri = "/_internal/" + urlPart(target.shardId().indexName()) + "/" + target.shardId().shardNumber() + "/_delete_by_query";
        return remoteJsonRequest("POST", target.host(), target.httpPort(), uri, byQueryBody(fencedRequest))
                .map(response -> {
                    if (response.statusCode() >= 300) {
                        throw new IllegalStateException("remote shard delete_by_query failed on " + target.nodeId()
                                + ": status=" + response.statusCode()
                                + ", body=" + response.body());
                    }
                    return JsonUtil.readValue(response.body(), ByQueryResponse.class);
                });
    }

    private Map<String, Object> byQueryBody(ByQueryRequest request) {
        Map<String, Object> body = new HashMap<>();
        body.put("query", request.query());
        body.put("doc", request.document());
        body.put("conflicts_proceed", request.conflictsProceed());
        body.put("mappings", request.mappings());
        if (request.ownerTerm() != null) {
            body.put("owner_term", request.ownerTerm());
        }
        if (request.allocationEpoch() != null) {
            body.put("allocation_epoch", request.allocationEpoch());
        }
        if (request.routing() != null) {
            body.put("routing", request.routing());
        }
        return body;
    }

    private long deletedCount(String status) {
        return count(status, "deleted=");
    }

    private long count(String status, String prefix) {
        if (status == null || !status.startsWith(prefix)) {
            return 0;
        }
        return Long.parseLong(status.substring(prefix.length()));
    }

    private void knnSearch(RoutingContext context) {
        try {
            SearchRequest request = knnSearchRequest(context);
            ClusterState state = clusterStateRepository.current();
            Map<String, FieldMapping> mappings = indexSettings(request.indexName(), state).mappings();
            validateVectorQuery(request.vector(), mappings);
            request = withMappings(request, mappings);
            SearchPlan plan = searchPlan(request, state);
            executeSearchPlan(plan, request, requestStartedNanos(context))
                    .onSuccess(response -> json(context, 200, response))
                    .onFailure(e -> {
                        Exception exception = exception(e);
                        error(context, status(exception), exception);
                    });
        } catch (Exception e) {
            error(context, status(e), e);
        }
    }

    private void updateByQuery(RoutingContext context) {
        try {
            ByQueryRequest request = byQueryRequest(context);
            ClusterState state = writableClusterState(context);
            if (state == null) {
                return;
            }
            request = withMappings(request, indexSettings(request.indexName(), state).mappings());
            SearchRequest searchRequest = new SearchRequest(
                    request.indexName(),
                    request.query(),
                    List.of(),
                    null,
                    request.routing(),
                    0,
                    10,
                    List.of(),
                    List.of(),
                    null,
                    request.mappings(),
                    "owner"
            );
            SearchPlan plan = searchPlanner.plan(searchRequest, state);
            executeUpdateByQueryPlan(plan, request)
                    .onSuccess(response -> json(context, 200, response))
                    .onFailure(e -> {
                        Exception exception = exception(e);
                        error(context, status(exception), exception);
                    });
        } catch (Exception e) {
            error(context, status(e), e);
        }
    }

    private void writeRoute(RoutingContext context) {
        try {
            String index = context.pathParam("index");
            String routing = context.queryParams().get("routing");
            ClusterState state = writableClusterState(context);
            if (state == null) {
                return;
            }
            WriteRoute route = writeRouter.route(index, routing, state);
            json(context, 200, Map.of(
                    "shardId", route.shardId(),
                    "nodeId", route.nodeId(),
                    "host", route.host(),
                    "httpPort", route.httpPort(),
                    "ownerTerm", route.ownerTerm(),
                    "allocationEpoch", route.allocationEpoch(),
                    "local", localNode.id().equals(route.nodeId())
            ));
        } catch (Exception e) {
            error(context, status(e), e);
        }
    }

    private void deleteByQuery(RoutingContext context) {
        try {
            ByQueryRequest request = byQueryRequest(context);
            ClusterState state = writableClusterState(context);
            if (state == null) {
                return;
            }
            request = withMappings(request, indexSettings(request.indexName(), state).mappings());
            SearchRequest searchRequest = new SearchRequest(
                    request.indexName(),
                    request.query(),
                    List.of(),
                    null,
                    request.routing(),
                    0,
                    10,
                    List.of(),
                    List.of(),
                    null,
                    request.mappings(),
                    "owner"
            );
            SearchPlan plan = searchPlanner.plan(searchRequest, state);
            executeDeleteByQueryPlan(plan, request)
                    .onSuccess(response -> json(context, 200, response))
                    .onFailure(e -> {
                        Exception exception = exception(e);
                        error(context, status(exception), exception);
                    });
        } catch (Exception e) {
            error(context, status(e), e);
        }
    }

    private void putLifecyclePolicy(RoutingContext context) {
        try {
            if (forwardToMasterIfNeeded(context)) {
                return;
            }
            requireMaster();
            String policy = context.pathParam("policy");
            json(context, 200, lifecycleService.putPolicy(lifecyclePolicy(policy, bodyAsMap(context))));
        } catch (Exception e) {
            error(context, status(e), e);
        }
    }

    private void attachLifecyclePolicy(RoutingContext context) {
        try {
            if (forwardToMasterIfNeeded(context)) {
                return;
            }
            requireMaster();
            json(context, 200, lifecycleService.attachPolicy(context.pathParam("index"), context.pathParam("policy")));
        } catch (Exception e) {
            error(context, status(e), e);
        }
    }

    private ByQueryRequest byQueryRequest(RoutingContext context) {
        Map<String, Object> body = bodyAsMap(context);
        return new ByQueryRequest(
                context.pathParam("index"),
                mapValue(body.get("query")),
                mapValue(body.get("doc")),
                stringValue(body.get("routing")),
                Boolean.TRUE.equals(body.get("conflicts_proceed")),
                parseMappings(body),
                longObject(body.get("owner_term")),
                longObject(body.get("allocation_epoch"))
        );
    }

    private IndexLifecyclePolicy lifecyclePolicy(String name, Map<String, Object> body) {
        Map<String, Object> policy = mapValue(body.getOrDefault("policy", body));
        Map<String, Object> phases = mapValue(policy.get("phases"));
        if (phases.isEmpty()) {
            phases = policy;
        }
        Map<LifecyclePhase, Long> minAgeMillisByPhase = new HashMap<>();
        phases.forEach((phaseName, phaseValue) -> {
            LifecyclePhase phase = LifecyclePhase.valueOf(phaseName.trim().toUpperCase(Locale.ROOT));
            Map<String, Object> spec = mapValue(phaseValue);
            Object minAge = spec.isEmpty() ? phaseValue : spec.get("min_age");
            minAgeMillisByPhase.put(phase, durationMillis(minAge));
        });
        return new IndexLifecyclePolicy(name, minAgeMillisByPhase);
    }

    private SearchRequest searchRequest(RoutingContext context) {
        String index = context.pathParam("index");
        Map<String, Object> body = bodyAsMap(context);
        VectorQuery vector = vectorFromBody(body);
        return new SearchRequest(
                index,
                searchQuery(body),
                listOfMaps(body.get("aggs")),
                vector,
                stringValue(body.get("routing")),
                intValue(body.get("from"), 0),
                intValue(body.get("size"), vector == null ? 10 : vector.k()),
                sortList(body.get("sort")),
                objectList(body.get("search_after")),
                pitId(body),
                parseMappings(body),
                readPreference(context, body)
        );
    }

    private SearchRequest internalSearchRequest(RoutingContext context) {
        String body = bodyAsString(context);
        if (body == null || body.isBlank()) {
            return searchRequest(context);
        }
        return JsonUtil.readValue(body, SearchRequest.class);
    }

    private SearchRequest knnSearchRequest(RoutingContext context) {
        String index = context.pathParam("index");
        Map<String, Object> body = bodyAsMap(context);
        Map<String, Object> knn = mapValue(body.get("knn"));
        VectorQuery vector = vectorQuery(knn.isEmpty() ? body : knn);
        return new SearchRequest(
                index,
                knnSearchFilter(body, knn),
                List.of(),
                vector,
                stringValue(body.get("routing")),
                0,
                vector.k(),
                List.of(),
                List.of(),
                pitId(body),
                parseMappings(body),
                readPreference(context, body)
        );
    }

    private ByQueryRequest withMappings(ByQueryRequest request, Map<String, FieldMapping> mappings) {
        return new ByQueryRequest(
                request.indexName(),
                request.query(),
                request.document(),
                request.routing(),
                request.conflictsProceed(),
                mappings,
                request.ownerTerm(),
                request.allocationEpoch()
        );
    }

    private ByQueryRequest withFence(ByQueryRequest request, long ownerTerm, long allocationEpoch) {
        return new ByQueryRequest(
                request.indexName(),
                request.query(),
                request.document(),
                request.routing(),
                request.conflictsProceed(),
                request.mappings(),
                ownerTerm,
                allocationEpoch
        );
    }

    private SearchRequest withMappings(SearchRequest request, Map<String, FieldMapping> mappings) {
        return new SearchRequest(
                request.indexName(),
                request.query(),
                request.aggregations(),
                request.vector(),
                request.routing(),
                request.from(),
                request.size(),
                request.sort(),
                request.searchAfter(),
                request.pitId(),
                mappings,
                request.readPreference()
        );
    }

    private String readPreference(RoutingContext context, Map<String, Object> body) {
        String preference = context.queryParams().get("read_preference");
        if (preference == null || preference.isBlank()) {
            preference = stringValue(body.get("read_preference"));
        }
        if (preference == null || preference.isBlank()) {
            preference = stringValue(body.get("read_from"));
        }
        if (preference == null || preference.isBlank()) {
            return "weak";
        }
        String normalized = preference.trim().toLowerCase(Locale.ROOT);
        if (!normalized.equals("weak") && !normalized.equals("strong")) {
            throw new IllegalArgumentException("read_preference must be weak or strong");
        }
        return normalized;
    }

    private String internalReadPreference(RoutingContext context) {
        String preference = context.queryParams().get("read_preference");
        if (preference == null || preference.isBlank()) {
            return "weak";
        }
        String normalized = preference.trim().toLowerCase(Locale.ROOT);
        if (!normalized.equals("weak")
                && !normalized.equals("strong")
                && !normalized.equals("owner")
                && !normalized.equals("remote")) {
            throw new IllegalArgumentException("internal read_preference must be weak, strong, owner, or remote");
        }
        return normalized;
    }

    private IndexSettings indexSettings(String index, ClusterState state) {
        IndexSettings settings = state.indices().get(index);
        if (settings == null) {
            throw new IllegalArgumentException("index not found: " + index);
        }
        if (settings.deletePending()) {
            throw new IllegalArgumentException("index is deleting: " + index);
        }
        return settings;
    }

    private void validateVectorQuery(VectorQuery vector, Map<String, FieldMapping> mappings) {
        if (vector == null) {
            return;
        }
        FieldMapping mapping = mappings.get(vector.field());
        if (mapping == null) {
            throw new IllegalArgumentException("knn field is not mapped: " + vector.field());
        }
        if (!mapping.denseVector() && !mapping.byteVector()) {
            throw new IllegalArgumentException("knn field is not a vector field: " + vector.field());
        }
        if (!Boolean.TRUE.equals(mapping.indexed())) {
            throw new IllegalArgumentException("knn field is not indexed: " + vector.field());
        }
        if (vector.vector() == null || vector.vector().size() != mapping.dimension()) {
            throw new IllegalArgumentException("knn query_vector dimension mismatch: " + vector.field());
        }
    }

    private CoordinatingPit coordinatingPit(String pitId) {
        if (pitId == null || pitId.isBlank()) {
            return null;
        }
        CoordinatingPit pit = pits.get(pitId);
        if (pit == null) {
            throw new IllegalArgumentException("point in time not found: " + pitId);
        }
        if (Instant.now().isAfter(pit.expiresAt())) {
            if (pits.remove(pitId, pit)) {
                closeCoordinatingPit(pit)
                        .onFailure(e -> log.warn("failed to close expired point in time {}", pitId, e));
            }
            throw new IllegalArgumentException("point in time expired: " + pitId);
        }
        return pit;
    }

    private void maintenanceTick() {
        submitMaintenanceTask("pit-cleanup", this::cleanupExpiredPits);
        submitMaintenanceTask("idle-resource-cleanup", this::cleanupIdleResources);
        for (ClusterMaintenanceService.MaintenanceTask task : ClusterMaintenanceService.MaintenanceTask.values()) {
            if (task == ClusterMaintenanceService.MaintenanceTask.WRITE_MAINTENANCE) {
                continue;
            }
            submitMaintenanceTask(task.name(), () -> maintenanceService.run(task));
        }
        submitMaintenanceTask("metrics-refresh", this::refreshRuntimeMetrics);
    }

    private void writeMaintenanceTick() {
        submitMaintenanceTask(
                ClusterMaintenanceService.MaintenanceTask.WRITE_MAINTENANCE.name(),
                () -> maintenanceService.run(ClusterMaintenanceService.MaintenanceTask.WRITE_MAINTENANCE)
        );
    }

    private void submitMaintenanceTask(String name, Runnable task) {
        if (!maintenanceTasksRunning.add(name)) {
            return;
        }
        try {
            maintenanceExecutor.execute(() -> runMaintenanceTask(name, task));
        } catch (RejectedExecutionException e) {
            maintenanceTasksRunning.remove(name);
            log.warn("cluster maintenance executor rejected task {}", name, e);
        }
    }

    private void runMaintenanceTask(String name, Runnable task) {
        try {
            task.run();
        } catch (Exception e) {
            log.warn("cluster maintenance task {} failed", name, e);
        } finally {
            maintenanceTasksRunning.remove(name);
        }
    }

    private void refreshRuntimeMetrics() {
        try {
            ClusterState state = clusterStateRepository.current();
            Map<String, Object> uploads = maintenanceService.uploadStatus(null);
            Map<String, Object> summary = mapValue(uploads.get("summary"));
            long activeShards = state.routingTable().stream()
                    .filter(routing -> routing.state() == ShardState.STARTED)
                    .count();
            long unassignedShards = state.routingTable().stream()
                    .filter(routing -> routing.state() != ShardState.STARTED
                            || !state.nodes().containsKey(routing.nodeId()))
                    .count();
            serverMetrics.setClusterHealth(
                    activeShards,
                    unassignedShards,
                    longValue(summary.get("pending_shards"), 0),
                    longValue(summary.get("stuck_shards"), 0)
            );
            serverMetrics.setPitCounts(pits.size(), localShardIndexService.openPointInTimeCount());
            serverMetrics.setCacheStats(RemoteCacheStats.snapshot());
            serverMetrics.setS3Stats(S3RemoteObjectStore.statsSnapshot());
        } catch (Exception e) {
            log.debug("failed to refresh Prometheus metrics", e);
        }
    }

    private void cleanupExpiredPits() {
        Instant now = Instant.now();
        pits.forEach((pitId, pit) -> {
            if (!now.isBefore(pit.expiresAt()) && pits.remove(pitId, pit)) {
                closeCoordinatingPit(pit)
                        .onFailure(e -> log.warn("failed to close expired point in time {}", pitId, e));
            }
        });
    }

    private void cleanupIdleResources() {
        try {
            localShardIndexService.cleanupIdleResources();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void runSnapshotGarbageCollection() {
        maintenanceService.runSnapshotGarbageCollection();
    }

    private ClusterState deleteIndexAndData(String indexName, int numberOfShards) throws IOException {
        indexService.markIndexDeleting(indexName);
        localShardIndexService.deleteIndex(indexName, numberOfShards);
        try (ManifestManager manifestManager = new ManifestManager(
                new ManifestOptions(""),
                remoteObjectStore,
                manifestMetadataManager
        )) {
            manifestManager.deleteIndexShards(indexName, numberOfShards);
        }
        return indexService.deleteIndex(indexName);
    }

    private void requireMaster() {
        if (!clusterCoordinator.isMaster()) {
            throw new NotMasterException("cluster-state mutation must be handled by the current master node");
        }
    }

    private boolean forwardToMasterIfNeeded(RoutingContext context) throws IOException {
        if (clusterCoordinator.isMaster()) {
            return false;
        }
        if (context.request().getHeader(FORWARDED_HEADER) != null) {
            throw new NotMasterException("forwarded cluster-state mutation did not reach the current master node");
        }
        ClusterState state = clusterStateRepository.current();
        ClusterNode masterNode = state.nodes().get(state.masterNodeId());
        if (masterNode == null) {
            throw new NotMasterException("current master node is not available");
        }
        if (localNode.id().equals(masterNode.id())) {
            throw new NotMasterException("local node is recorded as master but does not own the master lease");
        }
        forward(context, masterNode.id(), masterNode.host(), masterNode.httpPort());
        return true;
    }

    private Future<RemoteHttpResponse> remoteJsonRequest(
            String method,
            String host,
            int port,
            String uri,
            Object body
    ) {
        long started = System.nanoTime();
        Context requestContext = Vertx.currentContext();
        Promise<RemoteHttpResponse> promise = Promise.promise();
        HttpRequest.BodyPublisher publisher = body == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofByteArray(JsonUtil.writeValueAsBytes(body));
        HttpRequest.Builder request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + host + ":" + port + uri))
                .version(HttpClient.Version.HTTP_1_1)
                .timeout(forwardTimeout)
                .method(method, publisher);
        if (body != null) {
            request.header("content-type", "application/json");
        }
        forwardingClient.sendAsync(request.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
                .orTimeout(forwardTimeout.toMillis(), TimeUnit.MILLISECONDS)
                .whenComplete((response, throwable) -> runOnRequestContext(requestContext, () -> {
                    if (throwable != null) {
                        Throwable cause = throwable.getCause() == null ? throwable : throwable.getCause();
                        serverMetrics.internalHttpFinished(method, uri, 0, System.nanoTime() - started);
                        promise.fail(cause);
                        return;
                    }
                    serverMetrics.internalHttpFinished(method, uri, response.statusCode(), System.nanoTime() - started);
                    promise.complete(new RemoteHttpResponse(response.statusCode(), response.body()));
                }));
        return promise.future();
    }

    private void forward(RoutingContext context, String targetNodeId, String targetHost, int targetPort) {
        forward(context, targetNodeId, targetHost, targetPort, Map.of());
    }

    private void forward(
            RoutingContext context,
            String targetNodeId,
            String targetHost,
            int targetPort,
            Map<String, String> extraHeaders
    ) {
        long started = System.nanoTime();
        Context requestContext = Vertx.currentContext();
        String body = context.body() == null ? null : context.body().asString();
        HttpRequest.BodyPublisher publisher = body == null || body.isEmpty()
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8);
        HttpRequest.Builder request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + targetHost + ":" + targetPort + context.request().uri()))
                .version(HttpClient.Version.HTTP_1_1)
                .timeout(forwardTimeout)
                .method(context.request().method().name(), publisher)
                .header(FORWARDED_HEADER, localNode.id());
        context.request().headers().forEach(header -> {
            if (!skipForwardHeader(header.getKey())) {
                request.header(header.getKey(), header.getValue());
            }
        });
        extraHeaders.forEach(request::header);
        log.debug("forward request node={} target={} address={}:{} uri={} extraHeaders={}",
                localNode.id(), targetNodeId, targetHost, targetPort, context.request().uri(), extraHeaders.keySet());
        forwardingClient.sendAsync(request.build(), HttpResponse.BodyHandlers.ofByteArray())
                .orTimeout(forwardTimeout.toMillis(), TimeUnit.MILLISECONDS)
                .whenComplete((response, throwable) -> runOnRequestContext(requestContext, () -> {
                    if (throwable != null) {
                        Throwable cause = throwable.getCause() == null ? throwable : throwable.getCause();
                        serverMetrics.internalHttpFinished(
                                context.request().method().name(),
                                context.normalizedPath(),
                                0,
                                System.nanoTime() - started
                        );
                        Exception exception = cause instanceof Exception e ? e : new RuntimeException(cause);
                        log.warn("forward failed node={} target={} address={}:{} uri={}",
                                localNode.id(), targetNodeId, targetHost, targetPort, context.request().uri(), exception);
                        error(context, 502, new IOException(
                                "failed to forward request to node " + targetNodeId + " at "
                                        + targetHost + ":" + targetPort + ": " + exception.getMessage(),
                                exception
                        ));
                        return;
                    }
                    serverMetrics.internalHttpFinished(
                            context.request().method().name(),
                            context.normalizedPath(),
                            response.statusCode(),
                            System.nanoTime() - started
                    );
                    log.debug("forward response node={} target={} uri={} status={}",
                            localNode.id(), targetNodeId, context.request().uri(), response.statusCode());
                    context.response().setStatusCode(response.statusCode());
                    response.headers().map().forEach((name, values) -> {
                        if (!skipForwardHeader(name)) {
                            for (String value : values) {
                                context.response().putHeader(name, value);
                            }
                        }
                    });
                    byte[] responseBody = response.body();
                    if (responseBody == null || responseBody.length == 0) {
                        context.response().end();
                    } else {
                        context.response().end(Buffer.buffer(responseBody));
                    }
                }));
    }

    private void runOnRequestContext(Context requestContext, Runnable task) {
        if (requestContext == null) {
            vertx.runOnContext(ignored -> task.run());
            return;
        }
        requestContext.runOnContext(ignored -> task.run());
    }

    private boolean skipForwardHeader(String headerName) {
        String lower = headerName.toLowerCase(Locale.ROOT);
        return lower.equals("host")
                || lower.equals("connection")
                || lower.equals("content-length")
                || lower.equals("expect")
                || lower.equals("upgrade")
                || lower.equals("transfer-encoding")
                || lower.equals(FORWARDED_HEADER)
                || lower.equals(OWNER_TERM_HEADER)
                || lower.equals(ALLOCATION_EPOCH_HEADER)
                || lower.equals(DOCUMENT_ID_HEADER);
    }

    private Set<NodeRole> ensureCoordinatingRole(Set<NodeRole> roles) {
        if (roles == null || roles.isEmpty()) {
            return Set.of(NodeRole.COORDINATING);
        }
        if (roles.contains(NodeRole.COORDINATING)) {
            return roles;
        }
        return Arrays.stream(NodeRole.values())
                .filter(role -> roles.contains(role) || role == NodeRole.COORDINATING)
                .collect(Collectors.toSet());
    }

    private record BulkItemRequest(
            String action,
            String index,
            String id,
            String routing,
            Map<String, Object> source
    ) {
    }

    private record BulkItemPlan(
            int ordinal,
            BulkItemRequest item,
            String id,
            WriteRoute route,
            Map<String, FieldMapping> mappings
    ) {
    }

    private record BulkItemResult(int ordinal, BulkItemResponse response) {
    }

    private record BulkShardBatchKey(
            String nodeId,
            String host,
            int httpPort,
            ShardId shardId,
            long ownerTerm,
            long allocationEpoch
    ) {
    }

    private record BulkItemResponse(
            String action,
            String index,
            String id,
            int status,
            String result,
            Object shardId,
            Map<String, Object> error
    ) {
        private boolean failed() {
            return error != null || status >= 300;
        }

        private Map<String, Object> asMap() {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("_index", index);
            if (id != null) {
                item.put("_id", id);
            }
            item.put("status", status);
            if (result != null) {
                item.put("result", result);
            }
            if (shardId != null) {
                item.put("shardId", shardId);
            }
            if (error != null) {
                item.put("error", error);
            }
            return Map.of(action, item);
        }
    }

    private record ShardPit(SearchShardTarget target, String pitId) {
    }

    private record CoordinatingPit(
            String indexName,
            List<SearchShardTarget> targets,
            Map<String, String> shardPitIds,
            Instant expiresAt
    ) {
    }

    private record RemoteHttpResponse(int statusCode, String body) {
    }

    private record SnapshotPin(String indexName, String pinId) {
    }

    private interface MeasuredSupplier<T> {
        T get() throws Exception;
    }

    @Override
    public void close() {
        if (maintenanceTimerId != null) {
            vertx.cancelTimer(maintenanceTimerId);
        }
        if (writeMaintenanceTimerId != null) {
            vertx.cancelTimer(writeMaintenanceTimerId);
        }
        if (httpServer != null) {
            try {
                httpServer.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.warn("failed to close http server", e);
            }
        }
        shutdownMaintenanceExecutor();
        try {
            localShardIndexService.close();
        } catch (IOException e) {
            log.warn("failed to close local shard index service", e);
        }
        clusterCoordinator.close();
        try {
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("failed to close vertx", e);
        }
        if (etcdClient != null) {
            etcdClient.close();
        }
        if (s3Client != null) {
            s3Client.close();
        }
        serverMetrics.close();
    }

    private void shutdownMaintenanceExecutor() {
        maintenanceExecutor.shutdown();
        try {
            if (!maintenanceExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                maintenanceExecutor.shutdownNow();
                if (!maintenanceExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    log.warn("cluster maintenance executor did not stop before timeout");
                }
            }
        } catch (InterruptedException e) {
            maintenanceExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
