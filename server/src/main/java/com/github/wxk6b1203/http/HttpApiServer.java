package com.github.wxk6b1203.http;

import com.github.wxk6b1203.cluster.ClusterIndexService;
import com.github.wxk6b1203.cluster.ClusterNode;
import com.github.wxk6b1203.cluster.ClusterStateRepository;
import com.github.wxk6b1203.cluster.ClusterCoordinator;
import com.github.wxk6b1203.cluster.ClusterState;
import com.github.wxk6b1203.cluster.DefaultClusterIndexService;
import com.github.wxk6b1203.cluster.FieldMapping;
import com.github.wxk6b1203.cluster.HashShardRouter;
import com.github.wxk6b1203.cluster.IndexLifecyclePolicy;
import com.github.wxk6b1203.cluster.IndexSettings;
import com.github.wxk6b1203.cluster.InMemoryClusterStateRepository;
import com.github.wxk6b1203.cluster.IndexLifecycleService;
import com.github.wxk6b1203.cluster.LifecyclePhase;
import com.github.wxk6b1203.cluster.MasterOnlyClusterStateRepository;
import com.github.wxk6b1203.cluster.NodeRole;
import com.github.wxk6b1203.cluster.NoopClusterCoordinator;
import com.github.wxk6b1203.cluster.ShardId;
import com.github.wxk6b1203.cluster.ShardRouting;
import com.github.wxk6b1203.cluster.ShardState;
import com.github.wxk6b1203.cluster.WriteRoute;
import com.github.wxk6b1203.cluster.WriteRouter;
import com.github.wxk6b1203.cluster.etcd.EtcdClusterCoordinator;
import com.github.wxk6b1203.cluster.etcd.EtcdClusterStateRepository;
import com.github.wxk6b1203.config.ServerOptions;
import com.github.wxk6b1203.search.ByQueryRequest;
import com.github.wxk6b1203.search.ByQueryResponse;
import com.github.wxk6b1203.search.PointInTimeResponse;
import com.github.wxk6b1203.search.SearchRequest;
import com.github.wxk6b1203.search.SearchPlanner;
import com.github.wxk6b1203.search.SearchPlan;
import com.github.wxk6b1203.search.SearchResponse;
import com.github.wxk6b1203.search.SearchShardTarget;
import com.github.wxk6b1203.search.SearchHit;
import com.github.wxk6b1203.search.VectorQuery;
import com.github.wxk6b1203.errors.NotMasterException;
import com.github.wxk6b1203.index.IndexDocumentRequest;
import com.github.wxk6b1203.index.LocalShardIndexService;
import com.github.wxk6b1203.index.LuceneLocalShardIndexService;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.metadata.provider.ManifestMetadataManager;
import com.github.wxk6b1203.metadata.provider.etcd.EtcdManifestMetadataManager;
import com.github.wxk6b1203.metadata.provider.mem.MemMockProvider;
import com.github.wxk6b1203.store.manifest.ManifestManager;
import com.github.wxk6b1203.store.manifest.ManifestOptions;
import com.github.wxk6b1203.store.object.LocalFileRemoteObjectStore;
import com.github.wxk6b1203.store.object.RemoteObjectStore;
import com.github.wxk6b1203.store.object.S3RemoteObjectStore;
import com.github.wxk6b1203.util.JsonUtil;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.client.WebClient;
import io.etcd.jetcd.Client;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

@Slf4j
public class HttpApiServer implements AutoCloseable {
    private static final String FORWARDED_HEADER = "x-lucene-s3-forwarded";
    private static final String OWNER_TERM_HEADER = "x-lucene-s3-owner-term";
    private static final String ALLOCATION_EPOCH_HEADER = "x-lucene-s3-allocation-epoch";

    private final Vertx vertx;
    private final int port;
    private final ClusterStateRepository clusterStateRepository;
    private final ClusterIndexService indexService;
    private final IndexLifecycleService lifecycleService;
    private final SearchPlanner searchPlanner;
    private final WriteRouter writeRouter;
    private final LocalShardIndexService localShardIndexService;
    private final ClusterCoordinator clusterCoordinator;
    private final Client etcdClient;
    private final ClusterNode localNode;
    private final WebClient webClient;
    private final ManifestMetadataManager manifestMetadataManager;
    private final RemoteObjectStore remoteObjectStore;
    private final Map<String, CoordinatingPit> pits = new ConcurrentHashMap<>();
    private final Set<String> lifecycleDeletesInProgress = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean maintenanceRunning = new AtomicBoolean();
    private Long maintenanceTimerId;
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
                null,
                null,
                null
        ));
    }

    public HttpApiServer(ServerOptions options) {
        this.vertx = Vertx.vertx();
        this.port = options.httpPort();
        Set<NodeRole> roles = ensureCoordinatingRole(options.roles());
        this.localNode = new ClusterNode(
                options.nodeId(),
                options.nodeName(),
                options.host(),
                options.httpPort(),
                roles,
                Instant.now()
        );
        this.webClient = WebClient.create(vertx);
        if (options.etcdEnabled()) {
            this.etcdClient = Client.builder().endpoints(options.etcdEndpoints()).build();
            EtcdClusterStateRepository etcdRepository = new EtcdClusterStateRepository(
                    EtcdClusterStateRepository.Options.builder()
                            .clusterName(options.clusterName())
                            .endpoints(options.etcdEndpoints())
                            .namespace(options.etcdNamespace())
                            .build(),
                    etcdClient
            );
            this.clusterStateRepository = etcdRepository;
            this.clusterCoordinator = new EtcdClusterCoordinator(
                    EtcdClusterCoordinator.Options.builder()
                            .namespace(options.etcdNamespace())
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
        DefaultClusterIndexService clusterIndexService = new DefaultClusterIndexService(
                new MasterOnlyClusterStateRepository(clusterStateRepository, clusterCoordinator)
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
        this.localShardIndexService = new LuceneLocalShardIndexService(
                dataPath,
                options.s3Enabled() ? options.s3Bucket() : "lucene-s3",
                this.manifestMetadataManager,
                this.remoteObjectStore
        );
    }

    private RemoteObjectStore remoteObjectStore(ServerOptions options, Path dataPath) {
        if (!options.s3Enabled()) {
            return new LocalFileRemoteObjectStore(dataPath.resolve("remote-objects"));
        }
        S3ClientBuilder builder = S3Client.builder();
        if (options.s3Region() != null && !options.s3Region().isBlank()) {
            builder.region(Region.of(options.s3Region()));
        }
        if (options.s3Endpoint() != null && !options.s3Endpoint().isBlank()) {
            builder.endpointOverride(URI.create(options.s3Endpoint()));
        }
        if (options.s3AccessKey() != null && !options.s3AccessKey().isBlank()
                && options.s3SecretKey() != null && !options.s3SecretKey().isBlank()) {
            builder.credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(options.s3AccessKey(), options.s3SecretKey())
            ));
        }
        this.s3Client = builder.build();
        return new S3RemoteObjectStore(options.s3Bucket(), s3Client);
    }

    public Future<HttpServer> start() {
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());

        router.post("/_internal/:index/:shard/_search").handler(this::internalShardSearch);
        router.post("/_internal/:index/:shard/_pit").handler(this::internalShardOpenPit);
        router.delete("/_internal/_pit/:pit").handler(this::internalShardClosePit);
        router.post("/_internal/:index/:shard/_delete_by_query").handler(this::internalShardDeleteByQuery);
        router.post("/_internal/:index/:shard/_update_by_query").handler(this::internalShardUpdateByQuery);
        router.get("/_cluster/state").handler(this::clusterState);
        router.get("/_nodes").handler(this::nodes);
        router.delete("/_pit").handler(this::closePointInTime);
        router.put("/:index").handler(this::createIndex);
        router.delete("/:index").handler(this::deleteIndex);
        router.get("/:index/_mapping").handler(this::getMapping);
        router.put("/:index/_mapping").handler(this::putMapping);
        router.post("/:index/_doc").handler(this::indexDocument);
        router.post("/:index/_doc/:id").handler(this::indexDocument);
        router.post("/:index/_search").handler(this::search);
        router.post("/:index/_pit").handler(this::openPointInTime);
        router.post("/:index/_search_plan").handler(this::searchPlan);
        router.post("/:index/_knn_search").handler(this::knnSearch);
        router.get("/:index/_write_route").handler(this::writeRoute);
        router.post("/:index/_update_by_query").handler(this::updateByQuery);
        router.post("/:index/_delete_by_query").handler(this::deleteByQuery);
        router.put("/_ilm/policy/:policy").handler(this::putLifecyclePolicy);
        router.put("/:index/_ilm/policy/:policy").handler(this::attachLifecyclePolicy);

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
                    log.info("HTTP API listening on {}", port);
                });
    }

    private void clusterState(RoutingContext context) {
        try {
            json(context, 200, clusterStateRepository.current());
        } catch (IOException e) {
            error(context, 500, e);
        }
    }

    private void nodes(RoutingContext context) {
        try {
            json(context, 200, clusterStateRepository.current().nodes());
        } catch (IOException e) {
            error(context, 500, e);
        }
    }

    private void createIndex(RoutingContext context) {
        try {
            if (forwardToMasterIfNeeded(context)) {
                return;
            }
            requireMaster();
            String index = context.pathParam("index");
            Map<String, Object> body = bodyAsMap(context);
            int shards = intValue(body.get("number_of_shards"), 1);
            int redundantCopies = intValue(body.get("number_of_replicas"), 0);
            if (redundantCopies != 0) {
                throw new IllegalArgumentException(
                        "extra shard copies are not supported; committed shard data is recovered from S3"
                );
            }
            String policy = stringValue(body.get("lifecycle_policy"));
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

    private void deleteIndex(RoutingContext context) {
        try {
            if (forwardToMasterIfNeeded(context)) {
                return;
            }
            requireMaster();
            String index = context.pathParam("index");
            IndexSettings settings = indexSettings(index, clusterStateRepository.current());
            ClusterState state = deleteIndexAndData(index, settings.numberOfShards());
            json(context, 200, state);
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
            String routing = context.queryParams().get("routing");
            if (routing == null || routing.isBlank()) {
                routing = context.pathParam("id");
            }
            ClusterState state = clusterStateRepository.current();
            IndexSettings settings = indexSettings(index, state);
            WriteRoute route = writeRouter.route(index, routing, state);
            if (!localNode.id().equals(route.nodeId())) {
                if (context.request().getHeader(FORWARDED_HEADER) != null) {
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
            json(context, 201, localShardIndexService.index(new IndexDocumentRequest(
                    index,
                    route.shardId(),
                    context.pathParam("id"),
                    bodyAsMap(context),
                    settings.mappings()
            )));
        } catch (Exception e) {
            error(context, status(e), e);
        }
    }

    private void search(RoutingContext context) {
        try {
            SearchRequest request = searchRequest(context);
            ClusterState state = clusterStateRepository.current();
            Map<String, FieldMapping> mappings = indexSettings(request.indexName(), state).mappings();
            validateVectorQuery(request.vector(), mappings);
            request = withMappings(request, mappings);
            CoordinatingPit pit = coordinatingPit(request.pitId());
            if (pit != null && !pit.indexName().equals(request.indexName())) {
                throw new IllegalArgumentException("point in time does not belong to index: " + request.indexName());
            }
            SearchPlan plan = pit == null
                    ? searchPlan(request, state)
                    : new SearchPlan(request.indexName(), request.routing(), state.version(), pit.targets());
            executeSearchPlan(plan, request, pit)
                    .onSuccess(response -> json(context, 200, response))
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
                    "owner"
            );
            SearchPlan plan = searchPlanner.plan(request, state);
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
        return executeSearchPlan(plan, request, null);
    }

    private Future<SearchResponse> executeSearchPlan(SearchPlan plan, SearchRequest request, CoordinatingPit pit) {
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
                request.readPreference()
        );
        List<Future<SearchResponse>> futures = plan.targets().stream()
                .map(target -> executeShardSearch(target, withPit(shardRequest, pit, target)))
                .toList();
        if (futures.isEmpty()) {
            return Future.succeededFuture(new SearchResponse(0, 0, 0, 0, List.of(), Map.of(), List.of()));
        }
        long started = System.nanoTime();
        return Future.all(futures)
                .map(ignored -> mergeSearchResponses(futures.stream().map(Future::result).toList(), request, started));
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
        if (consistency.equalsIgnoreCase("strong") || remoteSnapshotReady(base.shardId())) {
            ClusterNode node = leastLoaded(dataNodes, load);
            load.merge(node.id(), 1, Integer::sum);
            return new SearchShardTarget(
                    base.shardId(),
                    node.id(),
                    node.host(),
                    node.httpPort(),
                    routing.ownerTerm(),
                    routing.allocationEpoch(),
                    true
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
                false
        );
    }

    private boolean remoteSnapshotReady(ShardId shardId) {
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

    private ShardRouting routingFor(ShardId shardId, ClusterState state) {
        return state.routingTable().stream()
                .filter(routing -> routing.shardId().equals(shardId))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("shard routing not found: " + shardId.routeKey()));
    }

    private Map<String, String> writeFenceHeaders(WriteRoute route) {
        return Map.of(
                OWNER_TERM_HEADER, Long.toString(route.ownerTerm()),
                ALLOCATION_EPOCH_HEADER, Long.toString(route.allocationEpoch())
        );
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
                request.readPreference()
        );
    }

    private Future<SearchResponse> executeShardSearch(SearchShardTarget target, SearchRequest request) {
        if (target.remoteSnapshot()) {
            request = withReadPreference(request, "remote");
        }
        if (localNode.id().equals(target.nodeId())) {
            try {
                return Future.succeededFuture(localShardIndexService.search(target.shardId(), request));
            } catch (Exception e) {
                return Future.failedFuture(e);
            }
        }
        String uri = "/_internal/" + target.shardId().indexName() + "/" + target.shardId().shardNumber() + "/_search";
        return webClient.post(target.httpPort(), target.host(), uri)
                .putHeader("content-type", "application/json")
                .sendBuffer(Buffer.buffer(JsonUtil.writeValueAsBytes(request)))
                .map(response -> {
                    if (response.statusCode() >= 300) {
                        throw new IllegalStateException("remote shard search failed on " + target.nodeId()
                                + ": status=" + response.statusCode()
                                + ", body=" + response.bodyAsString());
                    }
                    return JsonUtil.readValue(response.bodyAsString(), SearchResponse.class);
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
                readPreference
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
        String uri = "/_internal/" + target.shardId().indexName() + "/" + target.shardId().shardNumber()
                + "/_pit?keep_alive=" + keepAlive.toMillis() + "ms"
                + "&read_preference=" + readPreference;
        return webClient.post(target.httpPort(), target.host(), uri)
                .send()
                .map(response -> {
                    if (response.statusCode() >= 300) {
                        throw new IllegalStateException("remote shard pit open failed on " + target.nodeId()
                                + ": status=" + response.statusCode()
                                + ", body=" + response.bodyAsString());
                    }
                    return new ShardPit(target, JsonUtil.readValue(response.bodyAsString(), PointInTimeResponse.class).id());
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
        return webClient.delete(target.httpPort(), target.host(), "/_internal/_pit/" + pitId)
                .send()
                .map(response -> {
                    if (response.statusCode() >= 300) {
                        throw new IllegalStateException("remote shard pit close failed on " + target.nodeId()
                                + ": status=" + response.statusCode()
                                + ", body=" + response.bodyAsString());
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

    private SearchResponse mergeSearchResponses(List<SearchResponse> responses, SearchRequest request, long started) {
        List<String> failures = responses.stream()
                .flatMap(response -> response.shardFailures().stream())
                .toList();
        List<SearchHit> hits = responses.stream()
                .flatMap(response -> response.hits().stream())
                .sorted((left, right) -> compareHits(left, right, request.sort()))
                .toList();
        int from = Math.min(request.searchAfter().isEmpty() ? Math.max(0, request.from()) : 0, hits.size());
        int to = Math.min(from + Math.max(0, request.size()), hits.size());
        return new SearchResponse(
                (System.nanoTime() - started) / 1_000_000,
                responses.stream().mapToInt(SearchResponse::totalShards).sum(),
                responses.stream().mapToInt(SearchResponse::successfulShards).sum(),
                responses.stream().mapToInt(SearchResponse::failedShards).sum(),
                hits.subList(from, to),
                mergeAggregations(responses),
                failures
        );
    }

    private int compareHits(SearchHit left, SearchHit right, List<Map<String, Object>> sort) {
        if (sort == null || sort.isEmpty()) {
            return Float.compare(right.score(), left.score());
        }
        for (int i = 0; i < sort.size(); i++) {
            Object leftValue = i < left.sortValues().size() ? left.sortValues().get(i) : null;
            Object rightValue = i < right.sortValues().size() ? right.sortValues().get(i) : null;
            int compared = compareValues(leftValue, rightValue);
            if (compared != 0) {
                return sortDescending(sort.get(i)) ? -compared : compared;
            }
        }
        int byScore = Float.compare(right.score(), left.score());
        return byScore != 0 ? byScore : left.id().compareTo(right.id());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private int compareValues(Object left, Object right) {
        if (left == null && right == null) {
            return 0;
        }
        if (left == null) {
            return 1;
        }
        if (right == null) {
            return -1;
        }
        if (left instanceof Number leftNumber && right instanceof Number rightNumber) {
            return Double.compare(leftNumber.doubleValue(), rightNumber.doubleValue());
        }
        if (left instanceof Comparable comparable && left.getClass().isInstance(right)) {
            return comparable.compareTo(right);
        }
        return String.valueOf(left).compareTo(String.valueOf(right));
    }

    private String sortField(Map<String, Object> spec) {
        return spec.keySet().stream().findFirst().orElse("_score");
    }

    private boolean sortDescending(Map<String, Object> spec) {
        String field = sortField(spec);
        Object value = spec.get(field);
        if (value instanceof Map<?, ?> map) {
            Object order = map.get("order");
            return order == null ? "_score".equals(field) : "desc".equalsIgnoreCase(String.valueOf(order));
        }
        return value == null ? "_score".equals(field) : "desc".equalsIgnoreCase(String.valueOf(value));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> mergeAggregations(List<SearchResponse> responses) {
        Map<String, List<Map<String, Object>>> grouped = new HashMap<>();
        for (SearchResponse response : responses) {
            response.aggregations().forEach((name, value) -> {
                if (value instanceof Map<?, ?> map) {
                    grouped.computeIfAbsent(name, ignored -> new ArrayList<>()).add((Map<String, Object>) map);
                }
            });
        }
        if (grouped.isEmpty()) {
            return Map.of();
        }
        Map<String, Object> merged = new HashMap<>();
        grouped.forEach((name, shards) -> merged.put(name, mergeAggregation(shards)));
        return merged;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> mergeAggregation(List<Map<String, Object>> shards) {
        String type = stringValue(shards.getFirst().get("type"));
        if ("terms".equals(type)) {
            Map<String, Long> counts = new HashMap<>();
            String field = stringValue(shards.getFirst().get("field"));
            for (Map<String, Object> shard : shards) {
                Object bucketsValue = shard.get("buckets");
                if (bucketsValue instanceof List<?> buckets) {
                    for (Object bucketValue : buckets) {
                        if (bucketValue instanceof Map<?, ?> bucket) {
                            Object key = bucket.get("key");
                            Object docCount = bucket.get("doc_count");
                            if (key != null && docCount instanceof Number number) {
                                counts.merge(String.valueOf(key), number.longValue(), Long::sum);
                            }
                        }
                    }
                }
            }
            List<Map<String, Object>> buckets = counts.entrySet().stream()
                    .sorted((left, right) -> {
                        int byCount = Long.compare(right.getValue(), left.getValue());
                        return byCount != 0 ? byCount : left.getKey().compareTo(right.getKey());
                    })
                    .map(entry -> Map.<String, Object>of("key", entry.getKey(), "doc_count", entry.getValue()))
                    .toList();
            return Map.of("type", "terms", "field", field, "buckets", buckets);
        }
        String field = stringValue(shards.getFirst().get("field"));
        long count = shards.stream()
                .map(shard -> shard.get("count"))
                .filter(Number.class::isInstance)
                .map(Number.class::cast)
                .mapToLong(Number::longValue)
                .sum();
        Object value = switch (type) {
            case "min" -> shards.stream()
                    .map(shard -> shard.get("value"))
                    .filter(Number.class::isInstance)
                    .map(Number.class::cast)
                    .mapToDouble(Number::doubleValue)
                    .min()
                    .stream()
                    .boxed()
                    .findFirst()
                    .orElse(null);
            case "max" -> shards.stream()
                    .map(shard -> shard.get("value"))
                    .filter(Number.class::isInstance)
                    .map(Number.class::cast)
                    .mapToDouble(Number::doubleValue)
                    .max()
                    .stream()
                    .boxed()
                    .findFirst()
                    .orElse(null);
            case "sum" -> shards.stream()
                    .map(shard -> shard.get("value"))
                    .filter(Number.class::isInstance)
                    .map(Number.class::cast)
                    .mapToDouble(Number::doubleValue)
                    .sum();
            case "avg" -> {
                double sum = shards.stream()
                        .map(shard -> shard.get("sum"))
                        .filter(Number.class::isInstance)
                        .map(Number.class::cast)
                        .mapToDouble(Number::doubleValue)
                        .sum();
                yield count == 0 ? null : sum / count;
            }
            case "value_count" -> count;
            default -> null;
        };
        Map<String, Object> result = new HashMap<>();
        result.put("type", type);
        result.put("field", field);
        result.put("value", value);
        result.put("count", count);
        if ("avg".equals(type)) {
            double sum = shards.stream()
                    .map(shard -> shard.get("sum"))
                    .filter(Number.class::isInstance)
                    .map(Number.class::cast)
                    .mapToDouble(Number::doubleValue)
                    .sum();
            result.put("sum", sum);
        }
        return result;
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
        String uri = "/_internal/" + target.shardId().indexName() + "/" + target.shardId().shardNumber() + "/_update_by_query";
        return webClient.post(target.httpPort(), target.host(), uri)
                .putHeader("content-type", "application/json")
                .sendBuffer(Buffer.buffer(JsonUtil.writeValueAsBytes(byQueryBody(fencedRequest))))
                .map(response -> {
                    if (response.statusCode() >= 300) {
                        throw new IllegalStateException("remote shard update_by_query failed on " + target.nodeId()
                                + ": status=" + response.statusCode()
                                + ", body=" + response.bodyAsString());
                    }
                    return JsonUtil.readValue(response.bodyAsString(), ByQueryResponse.class);
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
        String uri = "/_internal/" + target.shardId().indexName() + "/" + target.shardId().shardNumber() + "/_delete_by_query";
        return webClient.post(target.httpPort(), target.host(), uri)
                .putHeader("content-type", "application/json")
                .sendBuffer(Buffer.buffer(JsonUtil.writeValueAsBytes(byQueryBody(fencedRequest))))
                .map(response -> {
                    if (response.statusCode() >= 300) {
                        throw new IllegalStateException("remote shard delete_by_query failed on " + target.nodeId()
                                + ": status=" + response.statusCode()
                                + ", body=" + response.bodyAsString());
                    }
                    return JsonUtil.readValue(response.bodyAsString(), ByQueryResponse.class);
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
            executeSearchPlan(plan, request)
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
            ClusterState state = clusterStateRepository.current();
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
            WriteRoute route = writeRouter.route(index, routing, clusterStateRepository.current());
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
            ClusterState state = clusterStateRepository.current();
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

    private VectorQuery vectorQuery(Map<String, Object> knn) {
        return new VectorQuery(
                stringValue(knn.get("field")),
                floatList(knn.get("query_vector")),
                intValue(knn.get("k"), 10),
                intValue(knn.get("num_candidates"), Math.max(10, intValue(knn.get("k"), 10))),
                floatObject(knn.containsKey("min_score") ? knn.get("min_score") : knn.get("similarity"))
        );
    }

    private Map<String, Object> searchQuery(Map<String, Object> body) {
        Map<String, Object> query = queryObject(body.get("query"));
        Map<String, Object> knn = mapValue(body.get("knn"));
        Map<String, Object> knnFilter = queryObject(knn.get("filter"));
        return combineFilters(query, knnFilter);
    }

    private Map<String, Object> knnSearchFilter(Map<String, Object> body, Map<String, Object> knn) {
        return combineFilters(queryObject(body.get("filter")), queryObject(knn.get("filter")));
    }

    private Map<String, Object> combineFilters(Map<String, Object> left, Map<String, Object> right) {
        if (left.isEmpty()) {
            return right;
        }
        if (right.isEmpty()) {
            return left;
        }
        return Map.of("bool", Map.of("filter", List.of(left, right)));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> queryObject(Object value) {
        if (value instanceof Map<?, ?> map) {
            return (Map<String, Object>) map;
        }
        if (value instanceof List<?> list && !list.isEmpty()) {
            return Map.of("bool", Map.of("filter", list.stream()
                    .filter(Map.class::isInstance)
                    .map(Map.class::cast)
                    .toList()));
        }
        return Map.of();
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

    private String pitId(Map<String, Object> body) {
        if (body.get("pit") instanceof Map<?, ?> pit) {
            return stringValue(mapValue(pit).get("id"));
        }
        return stringValue(body.get("pit_id"));
    }

    private VectorQuery vectorFromBody(Map<String, Object> body) {
        if (body.get("vector") instanceof Map<?, ?>) {
            return vectorQuery(mapValue(body.get("vector")));
        }
        return body.get("knn") instanceof Map<?, ?>
                ? vectorQuery(mapValue(body.get("knn")))
                : null;
    }

    private IndexSettings indexSettings(String index, ClusterState state) {
        IndexSettings settings = state.indices().get(index);
        if (settings == null) {
            throw new IllegalArgumentException("index not found: " + index);
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
        if (!mapping.denseVector()) {
            throw new IllegalArgumentException("knn field is not a dense_vector: " + vector.field());
        }
        if (!Boolean.TRUE.equals(mapping.indexed())) {
            throw new IllegalArgumentException("knn field is not indexed: " + vector.field());
        }
        if (vector.vector() == null || vector.vector().size() != mapping.dimension()) {
            throw new IllegalArgumentException("knn query_vector dimension mismatch: " + vector.field());
        }
    }

    private Map<String, FieldMapping> parseMappings(Map<String, Object> body) {
        Object mappingsValue = body.get("mappings");
        Map<String, Object> mappings = mappingsValue instanceof Map<?, ?>
                ? mapValue(mappingsValue)
                : Map.of();
        Map<String, Object> properties = mappings.get("properties") instanceof Map<?, ?>
                ? mapValue(mappings.get("properties"))
                : mappings;
        if (properties.isEmpty() && body.get("properties") instanceof Map<?, ?>) {
            properties = mapValue(body.get("properties"));
        }
        if (properties.isEmpty()) {
            return Map.of();
        }
        Map<String, FieldMapping> result = new HashMap<>();
        properties.forEach((field, specValue) -> {
            if (specValue instanceof Map<?, ?> spec) {
                Map<String, Object> fieldSpec = mapValue(spec);
                result.put(field, new FieldMapping(
                        stringValue(fieldSpec.get("type")),
                        intObject(fieldSpec.containsKey("dimension") ? fieldSpec.get("dimension") : fieldSpec.get("dims")),
                        stringValue(fieldSpec.get("similarity")),
                        booleanObject(fieldSpec.containsKey("indexed") ? fieldSpec.get("indexed") : fieldSpec.get("index")),
                        booleanObject(fieldSpec.containsKey("stored") ? fieldSpec.get("stored") : fieldSpec.get("store")),
                        booleanObject(fieldSpec.containsKey("doc_values") ? fieldSpec.get("doc_values") : fieldSpec.get("docValues"))
                ));
            }
        });
        return result;
    }

    private Map<String, Object> bodyAsMap(RoutingContext context) {
        String body = bodyAsString(context);
        if (body == null || body.isBlank()) {
            return Map.of();
        }
        return JsonUtil.readValueAsMap(body);
    }

    private String bodyAsString(RoutingContext context) {
        return context.body() == null ? null : context.body().asString();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> mapValue(Object value) {
        return value instanceof Map<?, ?> map ? (Map<String, Object>) map : Map.of();
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> listOfMaps(Object value) {
        if (value instanceof List<?> list) {
            return (List<Map<String, Object>>) list;
        }
        if (value instanceof Map<?, ?> map) {
            List<Map<String, Object>> aggregations = new ArrayList<>();
            map.forEach((name, spec) -> {
                if (spec instanceof Map<?, ?> specMap) {
                    Map<String, Object> aggregation = new HashMap<>((Map<String, Object>) specMap);
                    aggregation.put("name", String.valueOf(name));
                    aggregations.add(aggregation);
                }
            });
            return aggregations;
        }
        return List.of();
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> sortList(Object value) {
        if (value instanceof List<?> list) {
            List<Map<String, Object>> sorts = new ArrayList<>();
            for (Object item : list) {
                if (item instanceof Map<?, ?> map) {
                    sorts.add((Map<String, Object>) map);
                } else if (item instanceof String field) {
                    sorts.add(Map.of(field, Map.of()));
                }
            }
            return sorts;
        }
        if (value instanceof Map<?, ?> map) {
            return List.of((Map<String, Object>) map);
        }
        if (value instanceof String field) {
            return List.of(Map.of(field, Map.of()));
        }
        return List.of();
    }

    private List<Object> objectList(Object value) {
        return value instanceof List<?> list ? new ArrayList<>(list) : List.of();
    }

    private List<Float> floatList(Object value) {
        if (!(value instanceof List<?> list)) {
            return List.of();
        }
        return list.stream()
                .filter(Number.class::isInstance)
                .map(Number.class::cast)
                .map(Number::floatValue)
                .toList();
    }

    private Duration keepAlive(RoutingContext context) {
        String value = context.queryParams().get("keep_alive");
        if (value == null || value.isBlank()) {
            value = stringValue(bodyAsMap(context).get("keep_alive"));
        }
        if (value == null || value.isBlank()) {
            return Duration.ofMinutes(1);
        }
        value = value.trim().toLowerCase(Locale.ROOT);
        try {
            if (value.endsWith("ms")) {
                return Duration.ofMillis(Long.parseLong(value.substring(0, value.length() - 2)));
            }
            if (value.endsWith("s")) {
                return Duration.ofSeconds(Long.parseLong(value.substring(0, value.length() - 1)));
            }
            if (value.endsWith("m")) {
                return Duration.ofMinutes(Long.parseLong(value.substring(0, value.length() - 1)));
            }
            if (value.endsWith("h")) {
                return Duration.ofHours(Long.parseLong(value.substring(0, value.length() - 1)));
            }
            if (value.endsWith("d")) {
                return Duration.ofDays(Long.parseLong(value.substring(0, value.length() - 1)));
            }
            return Duration.ofMillis(Long.parseLong(value));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid keep_alive: " + value, e);
        }
    }

    private long durationMillis(Object value) {
        if (value == null) {
            return 0L;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        String text = String.valueOf(value).trim().toLowerCase(Locale.ROOT);
        try {
            if (text.endsWith("ms")) {
                return Long.parseLong(text.substring(0, text.length() - 2));
            }
            if (text.endsWith("s")) {
                return Duration.ofSeconds(Long.parseLong(text.substring(0, text.length() - 1))).toMillis();
            }
            if (text.endsWith("m")) {
                return Duration.ofMinutes(Long.parseLong(text.substring(0, text.length() - 1))).toMillis();
            }
            if (text.endsWith("h")) {
                return Duration.ofHours(Long.parseLong(text.substring(0, text.length() - 1))).toMillis();
            }
            if (text.endsWith("d")) {
                return Duration.ofDays(Long.parseLong(text.substring(0, text.length() - 1))).toMillis();
            }
            return Long.parseLong(text);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid duration: " + value, e);
        }
    }

    private int intValue(Object value, int defaultValue) {
        return value instanceof Number number ? number.intValue() : defaultValue;
    }

    private Integer intObject(Object value) {
        return value instanceof Number number ? number.intValue() : null;
    }

    private Long longObject(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value instanceof String text && !text.isBlank()) {
            return Long.parseLong(text);
        }
        return null;
    }

    private Float floatObject(Object value) {
        return value instanceof Number number ? number.floatValue() : null;
    }

    private Boolean booleanObject(Object value) {
        return value instanceof Boolean bool ? bool : null;
    }

    private String stringValue(Object value) {
        return value == null ? null : value.toString();
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
        if (!maintenanceRunning.compareAndSet(false, true)) {
            return;
        }
        try {
            cleanupExpiredPits();
            runLifecyclePolicies();
        } finally {
            maintenanceRunning.set(false);
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

    private void runLifecyclePolicies() {
        if (!clusterCoordinator.isMaster()) {
            return;
        }
        ClusterState state;
        try {
            state = clusterStateRepository.current();
        } catch (IOException e) {
            log.warn("failed to load cluster state for lifecycle execution", e);
            return;
        }
        Instant now = Instant.now();
        state.indices().forEach((indexName, settings) -> {
            String policyName = settings.lifecyclePolicy();
            if (policyName == null || policyName.isBlank()) {
                return;
            }
            IndexLifecyclePolicy policy = state.lifecyclePolicies().get(policyName);
            if (policy == null) {
                return;
            }
            Long deleteAgeMillis = policy.minAgeMillisByPhase().get(LifecyclePhase.DELETE);
            if (deleteAgeMillis == null) {
                return;
            }
            long indexAgeMillis = Math.max(0, Duration.between(settings.createdAt(), now).toMillis());
            if (indexAgeMillis < deleteAgeMillis || !lifecycleDeletesInProgress.add(indexName)) {
                return;
            }
            try {
                deleteIndexAndData(indexName, settings.numberOfShards());
                log.info("deleted index {} by lifecycle policy {}", indexName, policyName);
            } catch (Exception e) {
                log.warn("failed to delete index {} by lifecycle policy {}", indexName, policyName, e);
            } finally {
                lifecycleDeletesInProgress.remove(indexName);
            }
        });
    }

    private ClusterState deleteIndexAndData(String indexName, int numberOfShards) throws IOException {
        ClusterState state = indexService.deleteIndex(indexName);
        localShardIndexService.deleteIndex(indexName, numberOfShards);
        try (ManifestManager manifestManager = new ManifestManager(
                new ManifestOptions(""),
                remoteObjectStore,
                manifestMetadataManager
        )) {
            manifestManager.deleteIndexShards(indexName, numberOfShards);
        }
        return state;
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
        String body = context.body() == null ? null : context.body().asString();
        Buffer buffer = body == null ? Buffer.buffer() : Buffer.buffer(body);
        var request = webClient
                .request(context.request().method(), targetPort, targetHost, context.request().uri())
                .putHeader(FORWARDED_HEADER, localNode.id());
        context.request().headers().forEach(header -> {
            if (!skipForwardHeader(header.getKey())) {
                request.putHeader(header.getKey(), header.getValue());
            }
        });
        extraHeaders.forEach(request::putHeader);
        request.sendBuffer(buffer)
                .onSuccess(response -> {
                    context.response().setStatusCode(response.statusCode());
                    response.headers().forEach(header -> {
                        if (!skipForwardHeader(header.getKey())) {
                            context.response().putHeader(header.getKey(), header.getValue());
                        }
                    });
                    Buffer responseBody = response.body();
                    if (responseBody == null) {
                        context.response().end();
                    } else {
                        context.response().end(responseBody);
                    }
                })
                .onFailure(e -> error(context, 502, new IOException(
                        "failed to forward request to node " + targetNodeId + " at "
                                + targetHost + ":" + targetPort,
                        e
                )));
    }

    private boolean skipForwardHeader(String headerName) {
        String lower = headerName.toLowerCase(Locale.ROOT);
        return lower.equals("host")
                || lower.equals("connection")
                || lower.equals("content-length")
                || lower.equals("transfer-encoding")
                || lower.equals(FORWARDED_HEADER)
                || lower.equals(OWNER_TERM_HEADER)
                || lower.equals(ALLOCATION_EPOCH_HEADER);
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

    private void json(RoutingContext context, int status, Object value) {
        context.response()
                .setStatusCode(status)
                .putHeader("content-type", "application/json")
                .end(new String(JsonUtil.writeValueAsBytes(value)));
    }

    private void error(RoutingContext context, int status, Exception e) {
        context.response()
                .setStatusCode(status)
                .putHeader("content-type", "application/json")
                .end(new String(JsonUtil.writeValueAsBytes(Map.of(
                        "error", e.getMessage(),
                        "type", e.getClass().getSimpleName(),
                        "status", status
                ))));
    }

    private int status(Exception e) {
        if (e instanceof NotMasterException) {
            return 503;
        }
        return 400;
    }

    private Exception exception(Throwable throwable) {
        return throwable instanceof Exception exception ? exception : new RuntimeException(throwable);
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

    @Override
    public void close() {
        if (maintenanceTimerId != null) {
            vertx.cancelTimer(maintenanceTimerId);
        }
        if (httpServer != null) {
            try {
                httpServer.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.warn("failed to close http server", e);
            }
        }
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
    }
}
