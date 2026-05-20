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

### Storage Pipeline: WAL → S3

`S3CachingDirectory` extends Lucene's `BaseDirectory`:

1. **Writes** go to a local NIO-based WAL directory (`/{dataPath}/_wal/{indexName}/_data/`).
2. On `syncMetaData()` (called by `IndexWriter.finishCommit()`), committed segment files are published to the `ManifestManager`.
3. `ManifestManager` marks file metadata as `DIRTY` in the metadata store, then asynchronously uploads to S3 (`UPLOADING` → `CLEAN`).
4. **Reads** check WAL first, then local shared cache, then download from S3 on-demand.

File status lifecycle: `DIRTY → UPLOADING → CLEAN → PINNED`. Status transitions are validated — only these transitions are allowed.

`RemoteObjectStore` is the pluggable object storage abstraction. `S3RemoteObjectStore` uses AWS SDK v2; `LocalFileRemoteObjectStore` copies to a local directory for dev/testing.

### Metadata Providers

`ManifestMetadataManager` is the abstract class for file metadata storage. Two implementations:
- `EtcdManifestMetadataManager` — stores file manifest metadata in etcd.
- `MemMockProvider` — in-memory, used when etcd is not configured.

Index settings, mappings, lifecycle policies, node membership, and shard routing live in cluster state. Manifest metadata only tracks committed Lucene files and upload status for searchable snapshot reads.

### HTTP API (Elasticsearch-compatible)

`HttpApiServer` wires everything together. REST endpoints:

| Method | Path | Purpose |
|--------|------|---------|
| PUT/DELETE | `/:index` | Create/delete index |
| POST | `/:index/_doc[/:id]` | Index a document (with routing) |
| POST | `/:index/_search` | Search (with from/size/sort/aggs/knn) |
| POST | `/:index/_knn_search` | kNN vector search |
| POST | `/:index/_pit` | Open point-in-time |
| DELETE | `/_pit` | Close point-in-time |
| POST | `/:index/_delete_by_query` | Delete by query |
| POST | `/:index/_update_by_query` | Update by query |
| PUT | `/:index/_mapping` | Define field mappings |
| PUT | `/_ilm/policy/:policy` | Create lifecycle policy |

**Request forwarding:** Cluster-state mutations are forwarded to the master; write requests are forwarded to the shard owner. Write fence headers (`x-lucene-s3-owner-term`, `x-lucene-s3-allocation-epoch`) prevent stale writes.

**Search with read preferences:** public APIs accept `weak` and `strong`. `weak` can mix shard owner local reads for dirty/uploading shards with remote snapshot reads for clean shards. `strong` reads only remote clean/pinned snapshots. Internal shard APIs also accept `owner` and `remote`. The coordinator merges shard-level results.

### Field Mappings

Documents require explicit field mappings (`FieldMapping` record). Supported types: `keyword`, `text`, `long`, `double`, `boolean`, `dense_vector`. Dense vector fields require `dimension` and support cosine/dot_product/euclidean similarity. kNN search uses `KnnFloatVectorQuery` with optional pre-filter.

## Project Status

This is an experimental project (as stated in README). The current direction is stateless data nodes with etcd-backed cluster state, a single shard owner for writes, and S3/compatible object storage as the durable source for committed Lucene files. Do not reintroduce primary/replica shard semantics unless the design changes explicitly.
