# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build / Test Commands

```bash
./gradlew build                          # Build all modules
./gradlew test                           # Run all tests
./gradlew :core:test                     # Run core module tests only
./gradlew :core:test --tests "com.github.wxk6b1203.store.S3CachingDirectoryTest"  # Single test
./gradlew :server:run                    # Start the server (default port 9200)
./gradlew :server:distZip                # Build server distribution zip
./gradlew :server:test --tests "com.github.wxk6b1203.http.HttpApiServerStressTest"  # Stress test
```

Uses Java 25 toolchains. Tests use JUnit Jupiter 5.10.

## Module Architecture

```
lucene-s3/
├── utility/     Shared: JSON/YAML (Jackson), ScaleFirstExecutor (thread pool)
├── core/        Core library — cluster state, Lucene+S3 storage, metadata mgmt, search/index APIs
└── server/      Vert.x HTTP server — Elasticsearch-compatible REST API, CLI entry point
```

Dependency chain: `server → core → utility`.

## Key Architecture

### Cluster State & Coordination

`ClusterState` is the central immutable record holding nodes, indices, routing table, and lifecycle policies. All cluster-state mutations (create/delete index, put mapping) must go through the elected master.

- **Single-node (no etcd):** Uses `InMemoryClusterStateRepository` + `NoopClusterCoordinator`. The node is always master.
- **Multi-node (with etcd):** Uses `EtcdClusterStateRepository` + `EtcdClusterCoordinator`. Nodes heartbeat via etcd leases; master election is lease-based with a CAS transaction on the `master` key.

`ClusterIndexService` handles index lifecycle. `ShardRouter` (implemented by `HashShardRouter`) maps documents to shards via hash of the routing key. `WriteRouter` uses it to build write routes; `SearchPlanner` uses it to build distributed search plans.

**Rebalance deduplication:** `EtcdClusterCoordinator` uses a fingerprint (state version + sorted data-node IDs) to skip no-op rebalance ticks. A fallback interval (2× lease TTL) guarantees eventual re-run even when the fingerprint hasn't changed.

### Write Fence

Every write carries `ownerTerm` and `allocationEpoch` from the routing table. The shard owner validates these against the current cluster state before accepting writes — stale or duplicate writes from a deposed owner are rejected. When forwarding write requests between nodes, the headers `x-lucene-s3-owner-term` and `x-lucene-s3-allocation-epoch` carry the fence through every hop.

### Storage Pipeline: WAL → S3

`S3CachingDirectory` extends Lucene's `BaseDirectory` with three storage tiers:

| Tier | Path | Backing |
|------|------|---------|
| WAL | `/{dataPath}/_wal/{indexName}/_data/` | `NIOFSDirectory` |
| Shared cache | `/{dataPath}/_shared/{indexName}/_data/` | `NIOFSDirectory` |
| Remote | `s3://{bucket}/{key}` | `RemoteObjectStore` |

1. **Writes** go to the WAL directory. On `syncMetaData()` (triggered by `IndexWriter.finishCommit()`), committed segment files are published to `ManifestManager`.
2. **Reads** check WAL first, then shared cache, then download from S3 on-demand. Cache downloads use per-file locks for deduplication and CRC32 validation.
3. File status lifecycle: `DIRTY → UPLOADING → CLEAN → PINNED`. Transitions use etcd CAS in `EtcdManifestMetadataManager`. S3 object keys embed `{fileName}.{checksum_hex}.{size}`, making uploads idempotent — identical content produces the same object key.

`RemoteObjectStore` is the pluggable object storage abstraction. `S3RemoteObjectStore` uses AWS SDK v2; `LocalFileRemoteObjectStore` copies to a local directory for dev/testing (used automatically when S3 is not configured).

### ManifestManager & Thread Pool

`ManifestManager` manages file metadata and async uploads. It accepts an optional external `ExecutorService`; when provided, it does NOT close the pool on `close()`. The canonical setup in `HttpApiServer` creates a single `ManifestManager`-scoped thread pool inside `LuceneLocalShardIndexService`, which owns the pool's lifecycle and passes it to every `ManifestManager` instance via `openManifestManager()`. Construction without an external pool (backwards-compatible path) lazily creates and closes its own virtual-thread-per-task pool.

### Upload Wait Strategy

`ManifestOptions.uploadWaitStrategy` controls post-commit behavior:

- **`async` (default):** Commit returns immediately; uploads run in the background.
- **`wait_for_upload`:** The commit call blocks until all files are uploaded and a clean snapshot is published, or until `uploadWaitTimeout` expires (default 30s). This uses `CompletableFuture.get()` with a `waitForSnapshot()` polling fallback for race conditions.

### Metadata Providers

`ManifestMetadataManager` is the abstract class for file metadata storage. Two implementations:
- `EtcdManifestMetadataManager` — stores file manifest metadata in etcd with per-key CAS.
- `MemMockProvider` — in-memory, used when etcd is not configured.

Index settings, mappings, lifecycle policies, node membership, and shard routing live in cluster state. Manifest metadata only tracks committed Lucene files and upload status for searchable snapshot reads.

### Search: Read Preferences

Public APIs accept `weak` and `strong`:

- **`weak` (default):** `SearchPlanner.hybridTarget()` checks per-shard whether a clean remote snapshot exists. If yes, the read is distributed to the least-loaded DATA node for remote/cache reads; if no, the read goes to the shard owner for local reads.
- **`strong`:** Fixes a clean/pinned remote snapshot generation at plan time; every shard reads only that generation. Prevents result drift from concurrent writes.

Internal shard APIs accept `owner` (read from the shard owner's local IndexWriter) and `remote` (read from a specific remote snapshot generation).

### Field Mappings

Documents require explicit field mappings (`FieldMapping` record). All types:

`keyword`, `text`, `long`, `integer`, `double`, `float`, `boolean`, `date`, `date_nanos`, `dense_vector`, `byte_vector`, `ip`, `binary`, `geo_point`, `long_range`, `integer_range`, `date_range`, `double_range`, `float_range`, `ip_range`.

Each field can independently control `indexed`, `stored`, `docValues`, and `multiValued`. Vector fields require `dimension` and accept `similarity` (cosine / dot_product / euclidean / maximum_inner_product). `date` fields are stored as epoch millis; `date_nanos` as epoch nanos. kNN search uses `KnnFloatVectorQuery` for `dense_vector` and `KnnByteVectorQuery` for `byte_vector`, both with optional pre-filter.

Custom Lucene Analyzers can be loaded via `AnalyzerRegistry`: built-in names (`standard`, `keyword`, `whitespace`, `simple`, `stop`, `english`), alias names (`ik_max_word`, `ik_smart`, `pinyin`), or `class:<fully-qualified-name>`.

### ILM (Index Lifecycle Management)

Two active phases, executed in `ClusterMaintenanceService.tick()`:

- **`warm`:** Runs on every node for shards it owns. Executes `forceMerge(1)` on the local IndexWriter, then commits. Deduplicated per `(shardId, ownerTerm, allocationEpoch)` tuple so each ownership epoch triggers at most one force-merge.
- **`delete`:** Master-only. Removes index from cluster state, deletes local shard data, S3 objects, and manifest metadata.

`hot`, `cold`, and `frozen` phases are accepted in policy configuration but trigger no action.

### PIT & Snapshot GC

Opening a PIT pins the current snapshot generation per shard. Pinned files are protected from garbage collection. PIT queries use a fixed `remoteSnapshotGeneration` in `S3CachingDirectory` to ensure stable results. Expired PITs are cleaned up on the maintenance tick; the master runs periodic snapshot GC, retaining the latest `--snapshot-retain-latest` generations plus any pinned generations.

### Observability

Prometheus metrics are exposed on a configurable `--metrics-port` (independent HTTP server). Key metrics: HTTP request counts and latency histograms, shard counts by state, pending/stuck upload counts, PIT counts, remote cache hit rate and corruption count, S3 operation and error counters. JVM metrics are auto-registered via `JvmMetrics`. Debug endpoints: `/_cluster/health`, `/_shards`, `/_nodes/stats`, `/_snapshot_status`, `/_uploads`, `/_indices`.

## HTTP API Endpoints

Public endpoints:

| Method | Path | Purpose |
|--------|------|---------|
| PUT/DELETE | `/:index` | Create/delete index |
| GET | `/:index` | Get index details |
| POST | `/:index/_doc[/:id]` | Index a document |
| DELETE | `/:index/_doc/:id` | Delete a document |
| POST | `/:index/_bulk` | Bulk index/delete |
| POST | `/_bulk` | Bulk (index specified per action) |
| POST | `/:index/_search` | Search |
| POST | `/:index/_knn_search` | kNN vector search |
| POST | `/:index/_pit` | Open point-in-time |
| DELETE | `/_pit` | Close point-in-time |
| POST | `/:index/_delete_by_query` | Delete by query |
| POST | `/:index/_update_by_query` | Update by query |
| GET/PUT | `/:index/_mapping` | Get/define field mappings |
| PUT | `/_ilm/policy/:policy` | Create lifecycle policy |
| PUT | `/:index/_ilm/policy/:policy` | Attach lifecycle policy to index |
| GET | `/_cluster/health` | Cluster health summary |
| GET | `/_cluster/state` | Full cluster state |
| GET | `/_shards` | Per-shard status |
| GET | `/_nodes` / `/_nodes/stats` | Node list / statistics |
| GET | `/_indices` | List all indices |
| GET | `/_snapshot_status` / `/_uploads` | Upload/snapshot status |
| POST | `/_uploads/_retry` | Retry pending uploads |

Internal shard endpoints (for cross-node forwarding): `/_internal/:index/:shard/_search`, `/_pit`, `/_bulk`, `/_delete_by_query`, `/_update_by_query`.

**Request forwarding:** Cluster-state mutations are forwarded to the master; write requests are forwarded to the shard owner. Forwarding uses `java.net.http.HttpClient` with configurable timeout (`--http-forward-timeout`).

## Project Status

This is an experimental project. The current direction is stateless data nodes with etcd-backed cluster state, a single shard owner for writes, and S3/compatible object storage as the durable source for committed Lucene files. Do not reintroduce primary/replica shard semantics unless the design changes explicitly.
