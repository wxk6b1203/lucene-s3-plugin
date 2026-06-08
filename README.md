# Lucene S3

[中文](README_zh.md)

Lucene S3 is an experimental distributed search service. It uses Lucene for local writes and query execution, S3 or S3-compatible object storage as the durable data source, and etcd for cluster metadata, node heartbeats, master lease, and shard routing.

The current implementation has removed replica and primary shard semantics: S3 is the durable source for committed data, and each shard retains only a single write owner. Read requests can be coordinated across multiple nodes, while write requests are routed or forwarded to the corresponding shard owner.

## Architecture

```text
Client
  |
  v
Any HTTP node, automatically COORDINATING capable
  |
  +-- Cluster metadata mutations: forwarded to the node holding the master lease
  |      |
  |      v
  |    etcd: cluster state version, node heartbeat, master lease,
  |          index settings, mappings, ILM policy, shard routing
  |
  +-- Write requests: shard computed from routing or document ID, forwarded to shard owner
  |      |
  |      v
  |    Lucene IndexWriter writes to local WAL directory
  |      |
  |      v
  |    After commit, ManifestManager asynchronously uploads immutable Lucene files to S3
  |
  +-- Query requests: per-shard choice of owner-local or remote snapshot read path
         |
         v
       Lucene IndexSearcher + S3CachingDirectory
```

Core components:

- `server`: Vert.x HTTP API, cluster coordination, cross-node forwarding, ILM/PIT background maintenance.
- `core`: Lucene local shard service, shard routing, cluster state, mapping, search planner, S3 caching directory, manifest management.
- `utility`: Shared utilities.

### Metadata and Cluster State

- Multi-node mode uses etcd. When `--etcd-endpoints` is non-empty, the etcd cluster state repository and master lease are enabled.
- Single-node development mode can run without etcd, using in-memory metadata — suitable only for local debugging.
- Redis is no longer a metadata storage dependency.
- Cluster state uses an incrementing version; routing changes are written via atomic updates.
- Nodes participate in discovery via heartbeats; master-eligible nodes compete for the master lease.
- Shard routing records `nodeId`, `ownerTerm`, `allocationEpoch`, and status. Forwarded write requests carry the owner fence; the target node validates against current routing to prevent a deposed owner from continuing to write.

### Storage Model

Each logical shard maps to a physical Lucene index name:

```text
<index>__shard_<shardNumber>
```

Local directories default under `--data-path`:

```text
<data-path>/_wal/<index>__shard_<n>/_data       # local Lucene write directory for the current owner
<data-path>/_shared/<index>__shard_<n>/_data    # remote snapshot download cache
<data-path>/_shared/<index>__shard_<n>/_temp    # download temp files
<data-path>/remote-objects                      # local object store fallback when S3 is not configured
```

Remote object keys take the form:

```text
<index>__shard_<n>/_data/<lucene-file>.<crc32-hex>.<size>
```

Lucene files are only considered publishable once a specific `segments_N` commit becomes visible. ManifestManager uploads data files first, then committed segment files, and distinguishes file status as follows:

- `DIRTY`: committed locally, but remote object not yet complete.
- `UPLOADING`: upload in progress.
- `CLEAN`: uploaded, readable as a remote snapshot.
- `PINNED`: readable and retained remote file.

Remote object keys include the file name, CRC32, and size, distinguishing different content versions of files with the same logical name. Background maintenance tasks periodically retry `DIRTY`/`UPLOADING` files for shards owned by the local node; if an upload task fails, the next maintenance round or the next local commit will continue publishing the incomplete local commit.

After each complete upload of a Lucene commit, ManifestManager publishes a commit snapshot generation. The snapshot records the set of remote files visible to that commit and the `segments_N` file name. `strong` reads and remote-only reads bind to the latest complete snapshot rather than reading the live manifest directly. PIT pins the snapshot generation at open time, releasing the pin on close or expiry; the master background maintainer performs snapshot GC, cleaning up only generations not protected by the latest retention window or PIT pins and their exclusive objects.

### Read Consistency

The public API's `read_preference` exposes only two values:

- `weak`, the default. Each shard independently chooses a read path: if the shard still has `DIRTY` or `UPLOADING` files, read the current owner's locally committed data; if the remote snapshot is clean, assign to a less-loaded data node to read from S3/cache.
- `strong`. At query planning time, the coordinating node fixes a remote clean/pinned snapshot generation for each shard. During the shard execution phase, only that set of generations is read, so a single non-PIT query will not drift to a newer snapshot mid-execution. If a shard's latest commit is still uploading, a strong read will see an older but fully uploaded snapshot for that shard; if the shard has no remote snapshot at all, that shard returns empty results.

This model allows different shards of the same index to be in different states simultaneously: e.g., 2 out of 3 shards already uploaded to S3 and 1 still uploading on the owner, a `weak` query mixes remote and local reads; a `strong` query only reads the remote snapshots selected at planning time. `strong` guarantees "the set of shard snapshots for this query is stable," not a cross-shard global transaction timestamp.

The shared read cache reuses files only after validating against the remote manifest's size and checksum. If the remote metadata for the same logical file name has changed, the old cache is discarded and re-fetched from object storage, avoiding stale file reads or overwriting locally written files in the WAL.

### No-Replica Semantics

This project does not maintain Lucene replicas and has no primary/replica shard concept:

- `number_of_replicas` must be omitted or `0`.
- The shard owner is only the current write node; it does not represent a primary.
- The owner is determined by the cluster state routing table, not by whether a local directory exists.
- When an owner goes offline, the master reassigns the shard owner based on surviving DATA nodes; when a write request arrives at the master, it may also trigger a no-op-safe reroute on demand, shortening the tick window.
- Uploaded committed data is recovered from S3; if all node local directories are lost but remote clean snapshots are intact, read requests can be served by any DATA node downloading the cache and querying. A new owner lazily recovers from the remote snapshot before continuing to commit.
- The owner's latest unuploaded local commit still depends on the original owner's survival.
- If a new owner has no recoverable local WAL, it discards the shard's `DIRTY`/`UPLOADING` manifest in background maintenance, falling back to the last clean snapshot, preventing unrecoverable pending metadata from blocking remote snapshot scheduling long-term.
- Read scaling primarily comes from remote snapshots: clean shards can be downloaded, cached, and queried by any DATA node.

## Requirements

- JDK 25. The `--add-modules jdk.incubator.vector` flag is required at startup to enable Lucene's vectorized acceleration.
- Gradle Wrapper.
- etcd for multi-node mode.
- S3 or S3-compatible object storage for production or multi-node persistence. When `--s3-bucket` is not configured, the local `data/remote-objects` fallback is used.

Windows PowerShell example:

```powershell
$env:JAVA_HOME = "C:\Users\wxk6b\Documents\Helper\jdk-25.0.2\"
$env:Path = "$env:JAVA_HOME\bin;$env:Path"
.\gradlew.bat test --stacktrace
```

Linux/macOS example:

```bash
export JAVA_HOME=/path/to/jdk-25
export PATH="$JAVA_HOME/bin:$PATH"
./gradlew test --stacktrace
```

## Starting the Server

### Single-Node Local Development

Without etcd and S3 configured, the service uses in-memory cluster state and local object store fallback:

```powershell
.\gradlew.bat :server:run --args="server --http-port 9200 --data-path data/node-1"
```

Linux/macOS:

```bash
./gradlew :server:run --args='server --http-port 9200 --data-path data/node-1'
```

### Multi-Node Cluster

Start etcd first, then start multiple server processes. In the examples below, port 9200 is a master-eligible + data + coordinating node, and port 9201 is a data + coordinating node.

```bash
etcd --listen-client-urls http://127.0.0.1:2379 \
  --advertise-client-urls http://127.0.0.1:2379
```

Windows PowerShell:

```powershell
.\gradlew.bat :server:run --args="server --node-id n1 --node-name n1 --http-port 9200 --host 127.0.0.1 --roles MASTER,DATA,COORDINATING --etcd-endpoints http://127.0.0.1:2379 --data-path data/n1 --s3-bucket lucene-s3-dev --s3-region us-east-1 --s3-endpoint http://127.0.0.1:9000 --s3-access-key minioadmin --s3-secret-key minioadmin"
```

Another terminal:

```powershell
.\gradlew.bat :server:run --args="server --node-id n2 --node-name n2 --http-port 9201 --host 127.0.0.1 --roles DATA,COORDINATING --etcd-endpoints http://127.0.0.1:2379 --data-path data/n2 --s3-bucket lucene-s3-dev --s3-region us-east-1 --s3-endpoint http://127.0.0.1:9000 --s3-access-key minioadmin --s3-secret-key minioadmin"
```

Linux/macOS: replace `.\gradlew.bat` with `./gradlew` and use shell quoting:

```bash
./gradlew :server:run --args='server --node-id n1 --node-name n1 --http-port 9200 --host 127.0.0.1 --roles MASTER,DATA,COORDINATING --etcd-endpoints http://127.0.0.1:2379 --data-path data/n1 --s3-bucket lucene-s3-dev --s3-region us-east-1 --s3-endpoint http://127.0.0.1:9000 --s3-access-key minioadmin --s3-secret-key minioadmin'
```

Building the distribution:

```bash
./gradlew :server:distZip
```

On Windows:

```powershell
.\gradlew.bat :server:distZip
```

The distribution zip is output to `server/build/distributions/server-1.0-SNAPSHOT.zip`. The Windows start script `bin/server.bat` uses `lib/server-1.0-SNAPSHOT-classpath.jar` as a pathing jar to avoid excessively long command-line classpaths. The start scripts automatically include the `--add-modules jdk.incubator.vector` flag.

To install directly into a local directory:

```bash
./gradlew :server:installDist
```

## Configuration

The service supports reading YAML or JSON configuration files via `--conf`; explicitly passed command-line arguments override same-name settings in the config file.

| Parameter | Default | Description |
| --- | --- | --- |
| `--http-port`, `-p` | `9200` | HTTP API listen port. |
| `--cluster-name` | `lucene-s3` | Cluster name. |
| `--node-id` | random UUID | Stable node ID. Recommended to set explicitly for multi-node restarts. |
| `--node-name` | `local-node` | Node display name. |
| `--host` | `127.0.0.1` | Advertised host, used for cross-node forwarding. |
| `--roles` | `MASTER,DATA,COORDINATING` | Comma-separated roles: `MASTER`, `DATA`, `INGEST`, `COORDINATING`. Coordinating capability is automatically added. |
| `--metrics-port` | `0` | Prometheus metrics standalone HTTP port. `0` means disabled. |
| `--http-forward-timeout` | `10` | Internal inter-node HTTP forward timeout in seconds. |
| `--etcd-endpoints` | empty | etcd endpoint, e.g. `http://127.0.0.1:2379`. When empty, uses single-node in-memory metadata. |
| `--etcd-namespace` | `lucene-s3/cluster` | etcd namespace for cluster state and manifest metadata. |
| `--etcd-timeout` | `10` | etcd startup and metadata operation timeout in seconds. Exits on failure if exceeded during startup. |
| `--data-path` | `data` | Local WAL, shared cache, and local fallback object store directory. |
| `--cache-max-bytes` | `0` | Local remote snapshot cache capacity limit. `0` means no capacity-based eviction. |
| `--cache-cleanup-interval` | `60` | Local remote snapshot cache background cleanup interval in seconds. |
| `--analyzer-plugin-path` | empty | Third-party Lucene Analyzer plugin directory or jar file path. Jars or class directories under the path are loaded by an isolated classloader. |
| `--s3-bucket` | empty | S3 bucket. When empty, enables local `remote-objects` fallback. |
| `--s3-region` | empty | S3 region. Uses AWS SDK default config when not specified. |
| `--s3-protocol` | `https` | Protocol when `--s3-endpoint` does not include a scheme: `http` or `https`. If the endpoint already includes `http://` or `https://`, the endpoint takes precedence. |
| `--s3-endpoint` | empty | S3-compatible service endpoint, e.g. `oss-cn-shanghai.aliyuncs.com` or `http://127.0.0.1:9000`. |
| `--s3-chunked-encoding` | `false` | Enable AWS SDK S3 chunked encoding. Disabled by default for compatibility with OSS/MinIO and other S3-compatible services; use `--s3-chunked-encoding` to enable or `--no-s3-chunked-encoding` to explicitly disable. |
| `--s3-content-md5` | `false` | Enable legacy `Content-MD5` header. Enable for S3-compatible services like Alibaba Cloud OSS that require `Content-MD5` on `DeleteObjects`. |
| `--s3-access-key` | empty | S3 access key. Uses AWS SDK default credential chain when not specified. |
| `--s3-secret-key` | empty | S3 secret key. Uses AWS SDK default credential chain when not specified. |
| `--commit-every-request` | `true` | Whether to execute Lucene commit and trigger remote publish on every successful write request. Set to `false` during write load testing, deferring to document count or time thresholds. |
| `--commit-interval` | `1000` | Deferred commit interval in milliseconds when per-request commit is off; only takes effect when `--commit-every-request=false` and the doc count threshold is not reached. |
| `--commit-after-docs` | `0` | Number of changed documents to trigger a commit when per-request commit is off. `0` means no doc-count-based trigger. |
| `--refresh-policy` | `immediate` | Local weak-read searcher refresh policy. `immediate` refreshes before write requests return; `interval` defers to background maintenance according to `--refresh-interval`. |
| `--refresh-interval` | `1000` | Local searcher refresh interval in milliseconds when `refresh-policy=interval`. Refresh only affects local searchability; it is not equivalent to Lucene commit or remote snapshot publishing. |
| `--upload-wait-strategy` | `async` | Remote upload wait strategy after Lucene commit. `async` returns after local commit; `wait_for_upload` waits for this commit's files to be uploaded and a clean snapshot published before returning. If per-request commit is off, upload waiting only occurs when a commit actually fires. |
| `--upload-wait-timeout` | `30` | Timeout in seconds for `wait_for_upload` to wait for upload and snapshot publishing. |
| `--snapshot-retain-latest` | `2` | Minimum number of latest commit snapshot generations to retain per shard. PIT-pinned generations are additionally retained. |
| `--max-write-requests` | `0` | Maximum concurrent write requests allowed on a single node. `0` means unlimited; returns `429` when exceeded. |
| `--max-bulk-items` | `0` | Maximum items allowed in a single bulk request. `0` means unlimited; returns `429` when exceeded. |
| `--max-bulk-bytes` | `0` | Maximum body bytes for a single public bulk request. `0` means unlimited; returns `413` during body reading when exceeded. |

YAML example:

```yaml
server:
  http:
    port: 9200
    forwardTimeoutSeconds: 10
  cluster:
    name: lucene-s3
  node:
    id: n1
    name: n1
    host: 127.0.0.1
  metrics:
    port: 9300
  roles: [MASTER, DATA, COORDINATING]
  etcd:
    endpoints: http://127.0.0.1:2379
    namespace: lucene-s3/cluster
    timeoutSeconds: 10
  data:
    path: data/n1
  cache:
    maxBytes: 10737418240
    cleanupIntervalSeconds: 60
  write:
    maxRequests: 0
  bulk:
    maxItems: 0
    maxBytes: 0
  index:
    commit:
      everyRequest: true
      intervalMillis: 1000
      afterDocs: 0
    refresh:
      policy: immediate
      intervalMillis: 1000
  analyzer:
    pluginPath: plugins/analyzers
  upload:
    waitStrategy: async
    waitTimeoutSeconds: 30
  s3:
    bucket: lucene-s3-dev
    region: us-east-1
    protocol: http
    endpoint: http://127.0.0.1:9000
    chunkedEncoding: false
    contentMd5: false
    accessKey: minioadmin
    secretKey: minioadmin
  snapshot:
    retainLatest: 2
```

JSON also supports the same grouped structure; flat fields such as `httpPort`, `nodeId`, `s3Bucket` can also be used.
Real access keys / secret keys should be passed via environment variables or local private config files; `config/local.*`, `config/*.local.yaml`, `config/*.local.json`, and `.env*` are in `.gitignore`.

```powershell
.\gradlew.bat :server:run --args="server --conf config/server.yaml --http-port 9201"
```

### Logging

The service outputs to both stdout and local rolling files by default:

- `LUCENE_S3_LOG_LEVEL`: default `INFO`.
- `LUCENE_S3_LOG_DIR`: default `logs`.
- `LUCENE_S3_LOG_MAX_FILE_SIZE`: default `100MB`.
- `LUCENE_S3_LOG_MAX_HISTORY`: default `14`.
- `LUCENE_S3_LOG_TOTAL_SIZE_CAP`: default `2GB`.

## HTTP API

On PowerShell, if `curl` is an alias, use `curl.exe` instead. All examples assume the service is at `http://127.0.0.1:9200`.

### Cluster State

```bash
curl http://127.0.0.1:9200/_cluster/state
curl http://127.0.0.1:9200/_cluster/health
curl http://127.0.0.1:9200/_nodes
curl http://127.0.0.1:9200/_nodes/stats
curl http://127.0.0.1:9200/_shards
curl http://127.0.0.1:9200/_indices
curl http://127.0.0.1:9200/books
curl http://127.0.0.1:9200/_snapshot_status
```

`/_cluster/health` summarizes the master, node count, active shards, pending/stuck uploads, and cluster state version. `/_shards` shows each shard's owner, owner term, routing allocation epoch, remote snapshot generation, and pending uploads. `/_indices` shows index-level status, shard summary, mapping field count, ILM binding, and delete tombstone status; `/:index` returns single index details and mappings. `/_nodes/stats` currently returns the local node's PIT count, remote cache hit rate, cache cleanup status, and S3 operation/error counts.

If started with `--metrics-port 9300`, Prometheus can scrape metrics on a dedicated port, avoiding metrics scrape being blocked by Lucene/IO requests on the main HTTP API:

```bash
curl http://127.0.0.1:9300/metrics
```

Currently exposed: HTTP request count/total duration, HTTP phase durations, inter-node internal HTTP durations, active requests, shard state, pending/stuck uploads, PIT count, local remote cache hits, and cumulative S3 operation/error/duration counters.

Check which node a routing key writes to:

```bash
curl "http://127.0.0.1:9200/books/_write_route?routing=user-1"
```

### Creating an Index

```bash
curl -X PUT http://127.0.0.1:9200/books \
  -H "content-type: application/json" \
  -d '{
    "settings": {
      "number_of_shards": 3,
      "number_of_replicas": 0
    },
    "mappings": {
      "properties": {
        "title": {"type": "text"},
        "author": {"type": "keyword"},
        "price": {"type": "double"},
        "published_at": {"type": "long"},
        "available": {"type": "boolean"},
        "embedding": {
          "type": "dense_vector",
          "dimension": 3,
          "similarity": "cosine"
        }
      }
    }
  }'
```

`settings.number_of_shards` defaults to `1`. `number_of_replicas` must be omitted or `0`; this project has no primary/replica semantics — committed data is recovered from S3. The legacy top-level `number_of_shards` and `number_of_replicas` keys are also accepted, as are ES-style nested/dotted keys like `settings.index.number_of_shards`.

Supported mapping types:

- `keyword`: exact match, doc values enabled by default, sortable and aggregatable.
- `text`: uses `standard` analyzer by default, doc values disabled by default; configurable `analyzer` and `search_analyzer`.
- `long`, `integer`, `double`, `float`: numeric query, sort, aggregation.
- `boolean`: boolean query, sort, aggregation.
- `dense_vector`: vector search field; requires `dimension` or `dims`; optional `similarity`.
- `byte_vector`: byte vector search field; requires `dimension` or `dims`; optional `similarity`.

Field parameters:

- `type`: field type, default `keyword`.
- `dimension` or `dims`: vector dimension.
- `similarity`: `cosine`, `dot_product`, `euclidean`, `maximum_inner_product`.
- `indexed` or `index`: whether to build an index, default `true`.
- `stored` or `store`: whether stored, default `true`.
- `doc_values` or `docValues`: whether to enable doc values; enabled by default for keyword, numeric, and boolean.
- `analyzer`: index analyzer for `text` fields, default `standard`.
- `search_analyzer` or `searchAnalyzer`: query analyzer for `text` fields, defaults to the same as `analyzer`.

Built-in analyzer names include `standard`, `keyword`, `whitespace`, `simple`, `stop`, `english`. Third-party analyzers can be specified with `class:<fully-qualified-class-name>` as long as the corresponding jar is on the runtime classpath; alternatively, use `--analyzer-plugin-path` / `server.analyzer.pluginPath` to point to a plugin directory — jars or class directories under it will be loaded. The system also reserves the aliases `ik`, `ik_smart`, `ik_max_word`, `pinyin`. Example:

```json
{
  "mappings": {
    "properties": {
      "title": {
        "type": "text",
        "analyzer": "ik_max_word",
        "search_analyzer": "ik_smart"
      },
      "suggest": {
        "type": "text",
        "analyzer": "class:com.example.lucene.PinyinAnalyzer"
      }
    }
  }
}
```

Adding a mapping:

```bash
curl -X PUT http://127.0.0.1:9200/books/_mapping \
  -H "content-type: application/json" \
  -d '{
    "properties": {
      "category": {"type": "keyword"}
    }
  }'
```

Existing fields cannot be overwritten with a different definition.

Viewing mappings:

```bash
curl http://127.0.0.1:9200/books/_mapping
```

Deleting an index:

```bash
curl -X DELETE http://127.0.0.1:9200/books
```

Deleting an index first writes a delete tombstone in cluster state, then removes local shard data, remote objects, and manifest metadata. The index metadata is only fully removed once all steps succeed. If any step fails, the background maintenance task will continue retrying this pending delete.

### Writing Documents

```bash
curl -X POST "http://127.0.0.1:9200/books/_doc/doc-1?routing=user-1" \
  -H "content-type: application/json" \
  -d '{
    "title": "Distributed Lucene",
    "author": "kimchy",
    "price": 42.5,
    "published_at": 20260519,
    "available": true,
    "embedding": [0.1, 0.2, 0.3]
  }'
```

You can also let the service generate the document ID:

```bash
curl -X POST "http://127.0.0.1:9200/books/_doc?routing=user-1" \
  -H "content-type: application/json" \
  -d '{"title":"Lucene Basics","author":"alice","price":19.9}'
```

Write requests can be sent to any node. The coordinating node selects the shard owner by routing or document ID and forwards the request.

Deleting a single document:

```bash
curl -X DELETE "http://127.0.0.1:9200/books/_doc/doc-1?routing=user-1"
```

Bulk writes use NDJSON, supporting `index`, `create`, and `delete` actions. `POST /_bulk` requires each action to specify `_index`; `POST /<index>/_bulk` can omit `_index`:

```bash
curl -X POST http://127.0.0.1:9200/_bulk \
  -H "content-type: application/x-ndjson" \
  --data-binary $'{"index":{"_index":"books","_id":"doc-2","routing":"user-1"}}\n{"title":"Bulk Lucene","author":"alice","price":29.9}\n{"index":{"_index":"books"}}\n{"title":"Auto ID","author":"bob","price":9.9}\n{"delete":{"_index":"books","_id":"doc-1","routing":"user-1"}}\n'
```

The coordinating node first generates stable IDs for items without `_id`, then groups and forwards to internal shard bulk APIs by target shard owner. The `create` action is create-only; encountering an existing `_id` returns an error for that item.

### Protobuf Requests

JSON remains the default format. To reduce HTTP encoding/decoding overhead, you can send binary requests using the predefined messages in `server/src/main/proto/lucene_s3/http/v1/http_api.proto` with:

- `Content-Type: application/x-protobuf`
- `Accept: application/x-protobuf`

High-frequency endpoints using typed Protobuf:

- `_search` / `_knn_search`: `SearchRequest` -> `SearchResponse`
- `_bulk` / `/<index>/_bulk`: `BulkRequest` -> `BulkResponse`
- Error responses: `ErrorResponse`

Naturally open structures like document `_source` and dynamic aggregation results still use `google.protobuf.Struct`. The typed `_bulk` request structure is equivalent to:

```json
{
  "items": [
    {
      "action": "BULK_ACTION_INDEX",
      "index": "books",
      "id": "doc-2",
      "routing": "user-1",
      "source": {"title": "Bulk Lucene", "author": "alice", "price": 29.9}
    },
    {
      "action": "BULK_ACTION_DELETE",
      "index": "books",
      "id": "doc-1",
      "routing": "user-1"
    }
  ]
}
```

Other low-frequency dynamic endpoints temporarily retain the `google.protobuf.Struct` fallback. `Struct` number types are represented as Protobuf `double`; external clients requiring strict 64-bit integer semantics should prefer typed fields or pass strings.

### Search

```bash
curl -X POST "http://127.0.0.1:9200/books/_search?read_preference=weak" \
  -H "content-type: application/json" \
  -d '{
    "query": {
      "bool": {
        "filter": [
          {"term": {"author": "kimchy"}},
          {"range": {"price": {"lte": 100}}}
        ]
      }
    },
    "from": 0,
    "size": 10,
    "sort": [
      {"price": {"order": "asc"}},
      {"_id": {"order": "asc"}}
    ],
    "aggs": {
      "by_author": {
        "terms": {
          "field": "author",
          "size": 10,
          "missing": "__missing__",
          "min_doc_count": 1,
          "order": {"_count": "desc"}
        }
      },
      "price_ranges": {
        "range": {
          "field": "price",
          "ranges": [
            {"key": "cheap", "to": 20},
            {"key": "normal", "from": 20, "to": 100},
            {"key": "premium", "from": 100}
          ]
        }
      },
      "avg_price": {"avg": {"field": "price"}}
    }
  }'
```

Supported queries:

- `match_all`
- `ids.values`
- `term`
- `terms`
- `prefix`
- `exists`
- `match`
- `range`
- `bool.filter`
- `bool.must`
- `bool.should`
- `bool.must_not`

Supported aggregations:

- `terms`
- `range`
- `min`
- `max`
- `sum`
- `avg`
- `value_count`

`terms` supports `size`, `missing`, `min_doc_count`, and `order`, where `order` can sort by `_count` or `_key`. `range` supports a `ranges` array with `from` (inclusive) and `to` (exclusive) and an optional custom `key`.

Sorting supports `_score`, `_id`, and mapping fields with doc values enabled. Large result set sorting is pushed down to Lucene's native Sort to avoid fetching everything into memory first.

### search_after

`search_after` must be used together with `sort`, and the number of values must match the number of sort fields. The `sortValues` in response hits can be used as input for the next page.

```bash
curl -X POST http://127.0.0.1:9200/books/_search \
  -H "content-type: application/json" \
  -d '{
    "query": {"match_all": {}},
    "size": 10,
    "sort": [
      {"price": {"order": "asc"}},
      {"_id": {"order": "asc"}}
    ],
    "search_after": [42.5, "doc-1"]
  }'
```

### PIT

Open a PIT:

```bash
curl -X POST "http://127.0.0.1:9200/books/_pit?keep_alive=1m&read_preference=strong"
```

Query with PIT:

```bash
curl -X POST http://127.0.0.1:9200/books/_search \
  -H "content-type: application/json" \
  -d '{
    "pit": {"id": "<pit-id>"},
    "query": {"match_all": {}},
    "size": 10
  }'
```

Close a PIT:

```bash
curl -X DELETE http://127.0.0.1:9200/_pit \
  -H "content-type: application/json" \
  -d '{"id": "<pit-id>"}'
```

`keep_alive` supports `ms`, `s`, `m`, `h`, `d`; without a unit, it is treated as milliseconds. The background maintainer actively closes expired coordinating PITs and per-shard PITs.

### Vector Search

Standalone `_knn_search`:

```bash
curl -X POST http://127.0.0.1:9200/books/_knn_search \
  -H "content-type: application/json" \
  -d '{
    "knn": {
      "field": "embedding",
      "query_vector": [0.1, 0.2, 0.3],
      "k": 10,
      "num_candidates": 100,
      "filter": {"term": {"author": "kimchy"}},
      "min_score": 0.1
    },
    "read_preference": "weak"
  }'
```

You can also include `knn` within `_search`:

```bash
curl -X POST http://127.0.0.1:9200/books/_search \
  -H "content-type: application/json" \
  -d '{
    "knn": {
      "field": "embedding",
      "query_vector": [0.1, 0.2, 0.3],
      "k": 5,
      "num_candidates": 50
    },
    "filter": {"range": {"price": {"gte": 10}}},
    "size": 5
  }'
```

Distributed vector queries expand the candidate set on the shard side, then the coordinating node performs a global merge.

### update_by_query and delete_by_query

```bash
curl -X POST http://127.0.0.1:9200/books/_update_by_query \
  -H "content-type: application/json" \
  -d '{
    "query": {"term": {"author": "kimchy"}},
    "doc": {"category": "search"},
    "conflicts_proceed": true
  }'
```

```bash
curl -X POST http://127.0.0.1:9200/books/_delete_by_query \
  -H "content-type: application/json" \
  -d '{
    "query": {"range": {"published_at": {"lt": 20200101}}}
  }'
```

These endpoints execute distributed per-shard and commit Lucene changes on the corresponding shard owners.

### Upload Status and Self-Healing

View async upload status for all indices:

```bash
curl http://127.0.0.1:9200/_uploads
curl http://127.0.0.1:9200/_snapshot_status
```

View a single index:

```bash
curl http://127.0.0.1:9200/books/_uploads
```

The response returns per-shard owner, pending file count, status breakdown, latest remote snapshot generation, and whether pending files older than 1 minute are marked as `stuck`. The background maintainer continuously retries pending uploads on the current owner; you can also manually trigger a retry:

```bash
curl -X POST http://127.0.0.1:9200/books/_uploads/_retry -d '{}'
```

### ILM

Create a lifecycle policy:

```bash
curl -X PUT http://127.0.0.1:9200/_ilm/policy/delete-old \
  -H "content-type: application/json" \
  -d '{
    "policy": {
      "phases": {
        "delete": {"min_age": "7d"}
      }
    }
  }'
```

Attach to an index:

```bash
curl -X PUT http://127.0.0.1:9200/books/_ilm/policy/delete-old \
  -H "content-type: application/json" \
  -d '{}'
```

The current background executor supports:

- `warm` phase: upon expiry, the current shard owner executes `forceMerge(1)` on the local shard and commits.
- `delete` phase: upon expiry, deletes index metadata, local shard data, remote objects, and manifest metadata.

`min_age` supports `ms`, `s`, `m`, `h`, `d`, or milliseconds. The `hot`, `cold`, and `frozen` phases are accepted in configuration but trigger no action.

### Debug Endpoints

```bash
curl -X POST http://127.0.0.1:9200/books/_search_plan \
  -H "content-type: application/json" \
  -d '{"query":{"match_all":{}}}'
```

`/_internal/...` routes are only for inter-node forwarding and are not intended for direct client use.

## Response Shapes

Search response:

```json
{
  "tookMillis": 12,
  "totalShards": 3,
  "successfulShards": 3,
  "failedShards": 0,
  "hits": [
    {
      "indexName": "books",
      "id": "doc-1",
      "score": 1.0,
      "source": {"title": "Distributed Lucene"},
      "sortValues": [42.5, "doc-1"]
    }
  ],
  "aggregations": {},
  "shardFailures": []
}
```

Write response:

```json
{
  "indexName": "books",
  "shardId": {"indexName": "books", "shardNumber": 0},
  "id": "doc-1",
  "result": "created",
  "committed": true
}
```

Error response:

```json
{
  "error": "index not found: books",
  "type": "IllegalArgumentException",
  "status": 404
}
```

Common status codes:

- `400`: client input errors — request parameters, mapping, query, sort, bulk format, etc.
- `404`: resource not found or PIT expired — index, mapping, PIT, etc.
- `409`: state conflicts — index/document/mapping already exists, write fence expired, shard currently not writable, etc.
- `413`: request body exceeds body limit, e.g. public bulk exceeds `--max-bulk-bytes`.
- `502`: remote shard RPC or S3-compatible service returned an error.
- `503`: temporary backend unavailability — current node is not master, master unavailable, no available DATA nodes, remote commit snapshot not yet readable, etcd/S3 client unavailable, etc.
- `500`: local IO or unclassified internal error.

## Stress Testing

Stress tests are not executed with normal tests and must be explicitly enabled:

```powershell
$env:LUCENE_S3_STRESS="true"
$env:LUCENE_S3_STRESS_DOCS="1000"
$env:LUCENE_S3_STRESS_BULK_SIZE="50"
$env:LUCENE_S3_STRESS_WRITE_THREADS="4"
$env:LUCENE_S3_STRESS_SEARCH_THREADS="4"
$env:LUCENE_S3_STRESS_SECONDS="8"
.\gradlew.bat :server:test --tests com.github.wxk6b1203.http.HttpApiServerStressTest --rerun-tasks --stacktrace
```

The test starts a single-node service, runs a mix of `_bulk`, `_search`, `_knn_search`, and observation endpoint requests, and writes a JSON report to `server/build/reports/stress/http-api-stress.json`. Optional environment variables also include `LUCENE_S3_STRESS_SHARDS`, `LUCENE_S3_STRESS_CATEGORIES`, `LUCENE_S3_STRESS_CACHE_MAX_BYTES`, and `LUCENE_S3_STRESS_METRICS_PORT`.

You can also use `test/http_stress.py` to run external stress tests against an already-running HTTP service. Below is a sample result from a local macOS M1 Pro 10C / 32GB machine, intended only for observing endpoint latency characteristics under the current mixed workload. Results are affected by machine configuration, S3/OSS endpoint, etcd, JVM warmup, index state, and stress test parameters, and should not be considered universal performance benchmarks.

```text
HTTP stress summary
base_url: http://127.0.0.1:9200
index:    stress_books
duration: 10s

operation                             count       avg       p95       p99           statuses
----------------------------------------------------------------------------------------------
aggregation_search                      139   155.533   897.734  1302.732            200:139
bulk_write                               28  2228.714  3316.161  3510.371             200:28
knn_search                              214   143.672   470.812  1287.135            200:214
observe /_cluster/health                  5   174.234   229.768   236.807              200:5
observe /_indices                         5   198.253   245.038   254.167              200:5
observe /_nodes/stats                     4    16.491    19.513    19.704              200:4
observe /_shards                          5   193.883   235.352   243.935              200:5
observe /_snapshot_status                 4   141.971    149.25   149.855              200:4
observe /stress_books/_uploads            4    54.166    57.665    58.073              200:4
search_after                            168   176.198  1097.121  1315.022            200:168
strong_search                           145   309.758  1235.849  1865.464            200:145
warmup_bulk                              10  1314.917   2443.69  2995.818             200:10
weak_search                             168   159.618  1090.823  1295.656            200:168

counters:
  bulk_docs: 1400
  bulk_requests: 28
  setup_created_index: 1
  warmup_docs: 500

report: test/http-stress-report.json
```

## Failure and Recovery Boundaries

The core assumptions of the current implementation are: S3/OSS stores published Lucene commit files, etcd stores cluster state and manifest metadata, and each shard has only one write owner at a time. After a write commit, Lucene's `SnapshotDeletionPolicy` pins the local commit files; they are released only after the async upload completes and the manifest snapshot is published. As a result, the normal deletion policy will not clean up `segments_N` and related data files that are still being uploaded.

| Scenario | Current Behavior | Read/Write Impact | Verification |
| --- | --- | --- | --- |
| Shard owner temporarily unreachable | Master reassigns owner; write requests use owner term / allocation epoch as fence | Writes may return `409`/`503` during reassignment, recoverable after | etcd integration tests cover stale owner reroute |
| Master node exits | Surviving nodes with `MASTER` role re-elect master | Management writes may return `503` during election; existing owner local reads/writes are minimally affected | etcd integration tests cover coordinated node writes after master failover |
| S3/OSS transient failure | Manifest files remain `DIRTY`/`UPLOADING`; background maintenance retries pending uploads for current owner | `weak` can read owner local commit; `strong`/remote reads requiring remote snapshots may return `503` or skip unavailable generations | `/_snapshot_status`, `/:index/_uploads` |
| Remote delete failure during index deletion | Delete tombstone written first, then maintenance retries deletion of local/remote data and metadata | Index in deleting state rejects new reads/writes, preventing partial deletion from being used | `/_indices` `delete_pending` |
| PIT or search pin protects old snapshots | Snapshot GC retains latest N and still-pinned generations | Old generations not GC'd during PIT; cleaned up by maintenance after expiry | PIT API, snapshot GC tests |
| Local cache corrupt or lost | Re-download from manifest snapshot corresponding objects | First read slower; returns `503` if remote object is missing | Remote cache stats and error responses |

## Current Limitations and Notes

- The project is still experimental and not recommended for direct production use.
- S3 is the durable source for committed files, but the latest commit may still exist only on the shard owner's local disk before async upload completes. Use the default `weak` read preference if you need to read this data.
- `number_of_replicas > 0` is not supported, and there are no primary/replica failover semantics.
- Multi-node requires etcd; the in-memory metadata mode without etcd cannot be shared across processes.
- `--conf` supports YAML/JSON configuration files; command-line arguments override config file values.
- Windows, Linux, and macOS all use Java NIO Path for local path handling. Deleting open Lucene files on Windows may require waiting; the code already retries recursive deletions.
