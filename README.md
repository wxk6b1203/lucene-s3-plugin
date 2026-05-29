# Lucene S3

Lucene S3 是一个实验性的分布式搜索服务。它使用 Lucene 做本地写入和查询执行，使用 S3 或 S3 兼容对象存储作为持久化数据源，使用 etcd 保存集群元数据、节点心跳、master lease 和 shard routing。

当前实现已经移除了副本和 primary shard 语义：S3 是提交后数据的持久化来源，每个 shard 只保留一个当前写入 owner。读请求可以在多个节点上协调执行，写请求会被路由或转发到对应 shard owner。

## 架构设计

```text
Client
  |
  v
任意 HTTP 节点, 自动具备 COORDINATING 能力
  |
  +-- 集群元数据变更: 转发到持有 master lease 的节点
  |      |
  |      v
  |    etcd: cluster state version, node heartbeat, master lease,
  |          index settings, mappings, ILM policy, shard routing
  |
  +-- 写入请求: 按 routing 或文档 ID 计算 shard, 转发到 shard owner
  |      |
  |      v
  |    Lucene IndexWriter 写本地 WAL 目录
  |      |
  |      v
  |    commit 后由 ManifestManager 异步上传不可变 Lucene 文件到 S3
  |
  +-- 查询请求: 为每个 shard 选择 owner local 或 remote snapshot 读路径
         |
         v
       Lucene IndexSearcher + S3CachingDirectory
```

核心组件：

- `server`: Vert.x HTTP API、集群协调、跨节点转发、ILM/PIT 后台维护。
- `core`: Lucene 本地 shard 服务、shard routing、cluster state、mapping、search planner、S3 caching directory、manifest 管理。
- `utility`: 通用工具。

### 元数据和集群状态

- 多节点模式使用 etcd。`--etcd-endpoints` 非空时启用 etcd cluster state repository 和 master lease。
- 单节点开发模式可以不配置 etcd，此时使用内存元数据，只适合本地调试。
- Redis 不再作为元数据存储依赖。
- cluster state 使用递增 version，路由变更通过原子 update 写入。
- 节点通过 heartbeat 参与发现；master eligible 节点竞争 master lease。
- shard routing 记录 `nodeId`、`ownerTerm`、`allocationEpoch` 和状态。转发写请求时会携带 owner fence，目标节点会校验当前 routing，避免旧 owner 继续写入。

### 存储模型

每个逻辑 shard 会映射到一个物理 Lucene index 名：

```text
<index>__shard_<shardNumber>
```

本地目录默认在 `--data-path` 下：

```text
<data-path>/_wal/<index>__shard_<n>/_data       # 当前 owner 的本地 Lucene 写目录
<data-path>/_shared/<index>__shard_<n>/_data    # remote snapshot 下载缓存
<data-path>/_shared/<index>__shard_<n>/_temp    # 下载临时文件
<data-path>/remote-objects                      # 未配置 S3 时的本地对象存储 fallback
```

远端对象 key 形如：

```text
<index>__shard_<n>/_data/<lucene-file>.<crc32-hex>.<size>
```

Lucene 文件只有在具体 `segments_N` commit 可见后才被当作可发布文件。ManifestManager 会先上传数据文件，再上传 committed segment 文件，并通过文件状态区分：

- `DIRTY`: 本地已 commit，但远端对象未完成。
- `UPLOADING`: 正在上传。
- `CLEAN`: 已上传，可作为 remote snapshot 读取。
- `PINNED`: 可读且被保留的远端文件。

远端对象 key 会携带文件名、CRC32 和大小，用于区分同名文件的不同内容版本。后台维护任务会周期性重试本节点 owned shard 的 `DIRTY`/`UPLOADING` 文件；如果上传任务失败，下一轮维护或下一次本地 commit 会继续发布未完成的本地 commit。

每次完整上传一个 Lucene commit 后，ManifestManager 会发布一个 commit snapshot generation。snapshot 保存该 commit 可见的远端文件集合和 `segments_N` 文件名，`strong` 读和 remote-only 读会绑定最新完整 snapshot，而不是直接读取仍在变化的 live manifest。PIT 会 pin 住打开时的 snapshot generation，关闭或过期后释放 pin；master 后台维护器会执行 snapshot GC，只清理未被最新保留窗口或 PIT pin 保护的 generation 及其独占对象。

### 读一致性

公开 API 的 `read_preference` 只暴露两个值：

- `weak`, 默认值。每个 shard 独立选择读路径：如果该 shard 还有 `DIRTY` 或 `UPLOADING` 文件，则读当前 owner 的本地已提交数据；如果 remote snapshot 已经干净，则分配给负载较低的数据节点读取 S3/cache。
- `strong`。协调节点会在规划本次查询时为每个 shard 固定一个远端 clean/pinned snapshot generation，分片执行阶段只读取这组 generation，因此同一次非 PIT 查询不会在执行过程中漂移到更新的 snapshot。若某个 shard 最新 commit 仍在上传中，strong 读会看到该 shard 较旧但完整上传的快照；若该 shard 还没有任何远端 snapshot，则该 shard 返回空结果。

这种模型允许一个 index 的不同 shard 同时处于不同状态：例如 3 个 shard 中 2 个已经上传到 S3，1 个还在 owner 本地上传中，`weak` 查询会混合读取远端和本地；`strong` 查询只读规划时选中的远端快照。`strong` 保证的是“本次查询的一组 shard snapshot 稳定”，不是跨 shard 的全局事务时间戳。

共享读缓存按远端 manifest 的大小和 checksum 校验后才会复用。若同一逻辑文件名的远端元数据已变更，旧缓存会被丢弃并重新从对象存储拉取，避免读到过期文件或覆盖 WAL 中的本地写入文件。

### 无副本语义

本项目不维护 Lucene 副本，也没有 primary/replica shard 概念：

- `number_of_replicas` 必须省略或为 `0`。
- shard owner 只是当前写入节点，不代表 primary。
- owner 由 cluster state 的 routing table 决定，不由本地目录是否存在决定。
- owner 掉线后，master 会根据存活 DATA 节点重新分派 shard owner；写请求到达 master 时也会按需触发一次 no-op-safe reroute，缩短等待周期 tick 的窗口。
- 已上传的提交数据从 S3 恢复；如果所有节点本地目录都丢失但远端 clean snapshot 完整，读请求可由任意 DATA 节点下载缓存并查询，新 owner 写入时会从 remote snapshot 懒恢复再继续提交。
- 未上传的 owner 本地最新 commit 仍然依赖原 owner 存活。
- 新 owner 如果没有可恢复的本地 WAL，会在后台维护中丢弃该 shard 的 `DIRTY`/`UPLOADING` manifest，回退到最后一个 clean snapshot，避免不可恢复的 pending 元数据长期阻塞 remote snapshot 调度。
- 读扩展主要来自 remote snapshot：干净 shard 可以由任意 DATA 节点下载缓存并查询。

## 环境要求

- JDK 25。
- Gradle Wrapper。
- 多节点模式需要 etcd。
- 生产或多节点持久化需要 S3 或 S3 兼容对象存储。未配置 `--s3-bucket` 时，只会使用 `data/remote-objects` 做本地 fallback。

Windows PowerShell 示例：

```powershell
$env:JAVA_HOME = "C:\Users\wxk6b\Documents\Helper\jdk-25.0.2\"
$env:Path = "$env:JAVA_HOME\bin;$env:Path"
.\gradlew.bat test --stacktrace
```

Linux/macOS 示例：

```bash
export JAVA_HOME=/path/to/jdk-25
export PATH="$JAVA_HOME/bin:$PATH"
./gradlew test --stacktrace
```

## 启动方式

### 单节点本地开发

不配置 etcd 和 S3 时，服务会使用内存 cluster state 和本地对象存储 fallback：

```powershell
.\gradlew.bat :server:run --args="server --http-port 9200 --data-path data/node-1"
```

Linux/macOS：

```bash
./gradlew :server:run --args='server --http-port 9200 --data-path data/node-1'
```

### 多节点集群

先启动 etcd，然后分别启动多个服务进程。下面示例中 9200 是 master eligible + data + coordinating 节点，9201 是 data + coordinating 节点。

```bash
etcd --listen-client-urls http://127.0.0.1:2379 \
  --advertise-client-urls http://127.0.0.1:2379
```

Windows PowerShell：

```powershell
.\gradlew.bat :server:run --args="server --node-id n1 --node-name n1 --http-port 9200 --host 127.0.0.1 --roles MASTER,DATA,COORDINATING --etcd-endpoints http://127.0.0.1:2379 --data-path data/n1 --s3-bucket lucene-s3-dev --s3-region us-east-1 --s3-endpoint http://127.0.0.1:9000 --s3-access-key minioadmin --s3-secret-key minioadmin"
```

另一个终端：

```powershell
.\gradlew.bat :server:run --args="server --node-id n2 --node-name n2 --http-port 9201 --host 127.0.0.1 --roles DATA,COORDINATING --etcd-endpoints http://127.0.0.1:2379 --data-path data/n2 --s3-bucket lucene-s3-dev --s3-region us-east-1 --s3-endpoint http://127.0.0.1:9000 --s3-access-key minioadmin --s3-secret-key minioadmin"
```

Linux/macOS 只需要把 `.\gradlew.bat` 换成 `./gradlew`，并使用 shell 引号：

```bash
./gradlew :server:run --args='server --node-id n1 --node-name n1 --http-port 9200 --host 127.0.0.1 --roles MASTER,DATA,COORDINATING --etcd-endpoints http://127.0.0.1:2379 --data-path data/n1 --s3-bucket lucene-s3-dev --s3-region us-east-1 --s3-endpoint http://127.0.0.1:9000 --s3-access-key minioadmin --s3-secret-key minioadmin'
```

构建发行包：

```bash
./gradlew :server:distZip
```

Windows 使用：

```powershell
.\gradlew.bat :server:distZip
```

发行包输出到 `server/build/distributions/server-1.0-SNAPSHOT.zip`。Windows 启动脚本 `bin/server.bat` 使用 `lib/server-1.0-SNAPSHOT-classpath.jar` 作为 pathing jar，避免把所有依赖 jar 展开到命令行导致 classpath 过长。

如果需要直接展开到本地目录运行：

```bash
./gradlew :server:installDist
```

## 配置参数

服务支持通过 `--conf` 读取 YAML 或 JSON 配置文件；命令行显式传入的参数会覆盖配置文件中的同名配置。

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `--http-port`, `-p` | `9200` | HTTP API 监听端口。 |
| `--cluster-name` | `lucene-s3` | 集群名称。 |
| `--node-id` | 随机 UUID | 稳定节点 ID。多节点重启建议显式指定。 |
| `--node-name` | `local-node` | 节点显示名。 |
| `--host` | `127.0.0.1` | 对外发布的 host，跨节点转发会使用它。 |
| `--roles` | `MASTER,DATA,COORDINATING` | 逗号分隔角色，可选 `MASTER`,`DATA`,`INGEST`,`COORDINATING`。服务会自动补充 coordinating 能力。 |
| `--metrics-port` | `0` | Prometheus metrics 独立 HTTP 端口。`0` 表示关闭。 |
| `--http-forward-timeout` | `10` | 节点间内部 HTTP 转发超时时间，单位秒。 |
| `--etcd-endpoints` | 空 | etcd endpoint，例如 `http://127.0.0.1:2379`。为空时使用单节点内存元数据。 |
| `--etcd-namespace` | `lucene-s3/cluster` | etcd 中保存集群状态和 manifest 元数据的 namespace。 |
| `--etcd-timeout` | `10` | etcd 启动和元数据操作超时时间，单位秒。启动阶段超过该时间会失败退出。 |
| `--data-path` | `data` | 本地 WAL、共享缓存和本地 fallback 对象存储目录。 |
| `--cache-max-bytes` | `0` | 本地 remote snapshot 缓存容量上限。`0` 表示不按容量主动淘汰。 |
| `--cache-cleanup-interval` | `60` | 本地 remote snapshot 缓存后台清理间隔，单位秒。 |
| `--analyzer-plugin-path` | 空 | 第三方 Lucene Analyzer 插件目录或 jar 文件路径。目录下的 jar 或 class 目录会被独立 classloader 加载。 |
| `--s3-bucket` | 空 | S3 bucket。为空时启用本地 `remote-objects` fallback。 |
| `--s3-region` | 空 | S3 region。未指定时走 AWS SDK 默认配置。 |
| `--s3-protocol` | `https` | 当 `--s3-endpoint` 未包含 scheme 时使用的协议，可选 `http` 或 `https`。若 endpoint 已写 `http://` 或 `https://`，以 endpoint 为准。 |
| `--s3-endpoint` | 空 | S3 兼容服务 endpoint，例如 `oss-cn-shanghai.aliyuncs.com` 或 `http://127.0.0.1:9000`。 |
| `--s3-chunked-encoding` | `false` | 是否启用 AWS SDK S3 chunked encoding。默认关闭以兼容 OSS/MinIO 等 S3-compatible 服务；需要时可用 `--s3-chunked-encoding` 打开，或用 `--no-s3-chunked-encoding` 显式关闭。 |
| `--s3-content-md5` | `false` | 是否启用 legacy `Content-MD5` header。阿里云 OSS 等 S3 兼容服务如果要求 `DeleteObjects` 带 `Content-MD5`，可打开该选项。 |
| `--s3-access-key` | 空 | S3 access key。未指定时走 AWS SDK 默认凭证链。 |
| `--s3-secret-key` | 空 | S3 secret key。未指定时走 AWS SDK 默认凭证链。 |
| `--commit-every-request` | `true` | 是否每次成功写请求都执行 Lucene commit 并触发远端发布。压测写入时可设为 `false`，改由文档数或时间阈值触发。 |
| `--commit-interval` | `1000` | 关闭每请求 commit 后的延迟 commit 周期，单位毫秒；仅在 `--commit-every-request=false` 且未达到文档数阈值时生效。 |
| `--commit-after-docs` | `0` | 关闭每请求 commit 后，累计多少个变更文档触发一次 commit。`0` 表示不按文档数触发。 |
| `--refresh-policy` | `immediate` | 本地弱读 searcher refresh 策略。`immediate` 表示写请求返回前 refresh；`interval` 表示由后台维护按 `--refresh-interval` refresh。 |
| `--refresh-interval` | `1000` | `refresh-policy=interval` 时的本地 searcher refresh 周期，单位毫秒。refresh 只影响本地可搜索性，不等同于 Lucene commit 或远端 snapshot 发布。 |
| `--upload-wait-strategy` | `async` | Lucene commit 后的远端上传等待策略。`async` 表示本地 commit 后返回；`wait_for_upload` 表示等待本次 commit 文件上传并发布 clean snapshot 后返回。若关闭每请求 commit，上传等待只会发生在实际 commit 触发时。 |
| `--upload-wait-timeout` | `30` | `wait_for_upload` 等待上传和 snapshot 发布的超时时间，单位秒。 |
| `--snapshot-retain-latest` | `2` | 每个 shard 至少保留的最新 commit snapshot generation 数。PIT pin 住的 generation 会额外保留。 |

YAML 示例：

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

JSON 也支持同样的分组结构；平铺字段如 `httpPort`、`nodeId`、`s3Bucket` 也可以使用。
真实 AK/SK 建议通过环境变量或本地私有配置传入；`config/local.*`、`config/*.local.yaml`、`config/*.local.json` 和 `.env*` 已加入 `.gitignore`。

```powershell
.\gradlew.bat :server:run --args="server --conf config/server.yaml --http-port 9201"
```

### 日志

服务默认同时输出到 stdout 和本地滚动文件：

- `LUCENE_S3_LOG_LEVEL`: 默认 `INFO`。
- `LUCENE_S3_LOG_DIR`: 默认 `logs`。
- `LUCENE_S3_LOG_MAX_FILE_SIZE`: 默认 `100MB`。
- `LUCENE_S3_LOG_MAX_HISTORY`: 默认 `14`。
- `LUCENE_S3_LOG_TOTAL_SIZE_CAP`: 默认 `2GB`。

## HTTP API

PowerShell 如果 `curl` 是别名，建议使用 `curl.exe`。示例均假设服务在 `http://127.0.0.1:9200`。

### 集群状态

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

`/_cluster/health` 汇总 master、节点数、active shards、pending/stuck uploads 和 cluster state version。`/_shards` 展示每个 shard 的 owner、owner term、routing allocation epoch、远端 snapshot generation 和 pending uploads。`/_indices` 展示索引级状态、shard 汇总、mapping 字段数、ILM 绑定和删除 tombstone 状态；`/:index` 返回单个索引详情和 mappings。`/_nodes/stats` 当前返回本节点的 PIT 数、remote cache 命中率、缓存清理状态和 S3 操作/错误计数。

如果启动时设置了 `--metrics-port 9300`，Prometheus 可抓取独立端口上的指标，避免 metrics scrape 被主 HTTP API 的 Lucene/IO 请求阻塞：

```bash
curl http://127.0.0.1:9300/metrics
```

当前暴露 HTTP 请求数/总耗时、HTTP 阶段耗时、节点间内部 HTTP 耗时、活跃请求、shard 状态、pending/stuck uploads、PIT 数、本地 remote cache 命中，以及 S3 操作/错误/耗时累计计数。

查看某个 routing 会写到哪个节点：

```bash
curl "http://127.0.0.1:9200/books/_write_route?routing=user-1"
```

### 创建索引

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

`settings.number_of_shards` 默认 `1`。`number_of_replicas` 必须省略或为 `0`；本项目没有 primary/replica 语义，已提交数据从 S3 恢复。兼容旧的顶层写法 `number_of_shards`、`number_of_replicas`，也兼容 `settings.index.number_of_shards` 这类 ES 风格嵌套/点号 key。

支持的 mapping 类型：

- `keyword`: 精确匹配，默认开启 doc values，可排序和聚合。
- `text`: 默认使用 `standard` 分词，默认不开启 doc values，可配置 `analyzer` 和 `search_analyzer`。
- `long`, `integer`, `double`, `float`: 数值查询、排序、聚合。
- `boolean`: 布尔查询、排序、聚合。
- `dense_vector`: 向量检索字段，需要 `dimension` 或 `dims`，可选 `similarity`。
- `byte_vector`: byte 向量检索字段，需要 `dimension` 或 `dims`，可选 `similarity`。

字段参数：

- `type`: 字段类型，默认 `keyword`。
- `dimension` 或 `dims`: 向量维度。
- `similarity`: `cosine`, `dot_product`, `euclidean`, `maximum_inner_product`。
- `indexed` 或 `index`: 是否建索引，默认 `true`。
- `stored` 或 `store`: 是否 stored，默认 `true`。
- `doc_values` 或 `docValues`: 是否开启 doc values；keyword、数字、boolean 默认开启。
- `analyzer`: `text` 字段的索引 analyzer，默认 `standard`。
- `search_analyzer` 或 `searchAnalyzer`: `text` 字段的查询 analyzer，默认跟 `analyzer` 一致。

内置 analyzer 名称包括 `standard`, `keyword`, `whitespace`, `simple`, `stop`, `english`。第三方 analyzer 可以用 `class:<fully-qualified-class-name>` 指定，只要对应 jar 在运行时 classpath 中即可；也可以通过 `--analyzer-plugin-path` / `server.analyzer.pluginPath` 指向插件目录，目录下的 jar 或 class 目录会被加载。系统也预留了 `ik`, `ik_smart`, `ik_max_word`, `pinyin` 别名。示例：

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

追加 mapping：

```bash
curl -X PUT http://127.0.0.1:9200/books/_mapping \
  -H "content-type: application/json" \
  -d '{
    "properties": {
      "category": {"type": "keyword"}
    }
  }'
```

已经存在的字段不能用不同定义覆盖。

查看 mapping：

```bash
curl http://127.0.0.1:9200/books/_mapping
```

删除索引：

```bash
curl -X DELETE http://127.0.0.1:9200/books
```

删除索引会先在 cluster state 中标记删除中的 tombstone，再删除本地 shard 数据、远端对象和 manifest metadata，全部成功后才最终移除 index metadata。若中途失败，后台维护任务会继续重试这个 pending delete。

### 写入文档

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

也可以让服务生成文档 ID：

```bash
curl -X POST "http://127.0.0.1:9200/books/_doc?routing=user-1" \
  -H "content-type: application/json" \
  -d '{"title":"Lucene Basics","author":"alice","price":19.9}'
```

写请求可以发到任意节点。协调节点会按 routing 或文档 ID 选择 shard owner 并转发。

删除单个文档：

```bash
curl -X DELETE "http://127.0.0.1:9200/books/_doc/doc-1?routing=user-1"
```

批量写入使用 NDJSON，支持 `index`、`create` 和 `delete` action。`POST /_bulk` 需要每个 action 指定 `_index`，`POST /<index>/_bulk` 可以省略 `_index`：

```bash
curl -X POST http://127.0.0.1:9200/_bulk \
  -H "content-type: application/x-ndjson" \
  --data-binary $'{"index":{"_index":"books","_id":"doc-2","routing":"user-1"}}\n{"title":"Bulk Lucene","author":"alice","price":29.9}\n{"index":{"_index":"books"}}\n{"title":"Auto ID","author":"bob","price":9.9}\n{"delete":{"_index":"books","_id":"doc-1","routing":"user-1"}}\n'
```

协调节点会先为无 `_id` 的 item 生成稳定 ID，再按目标 shard owner 分组转发到内部 shard bulk API；`create` action 是 create-only，遇到已有 `_id` 会返回该 item 的错误。

### 搜索

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
      "by_author": {"terms": {"field": "author", "size": 10}},
      "avg_price": {"avg": {"field": "price"}}
    }
  }'
```

支持的 query：

- `match_all`
- `ids.values`
- `term`
- `match`
- `range`
- `bool.filter`
- `bool.must`
- `bool.should`
- `bool.must_not`

支持的聚合：

- `terms`
- `min`
- `max`
- `sum`
- `avg`
- `value_count`

排序支持 `_score`、`_id` 和开启 doc values 的 mapping 字段。大结果集排序会下推到 Lucene 原生 Sort，避免先全量取回再内存排序。

### search_after

`search_after` 必须和 `sort` 一起使用，值数量需要和 sort 字段数量一致。响应 hit 中的 `sortValues` 可作为下一页输入。

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

打开 PIT：

```bash
curl -X POST "http://127.0.0.1:9200/books/_pit?keep_alive=1m&read_preference=strong"
```

使用 PIT 查询：

```bash
curl -X POST http://127.0.0.1:9200/books/_search \
  -H "content-type: application/json" \
  -d '{
    "pit": {"id": "<pit-id>"},
    "query": {"match_all": {}},
    "size": 10
  }'
```

关闭 PIT：

```bash
curl -X DELETE http://127.0.0.1:9200/_pit \
  -H "content-type: application/json" \
  -d '{"id": "<pit-id>"}'
```

`keep_alive` 支持 `ms`, `s`, `m`, `h`, `d`，不带单位时按毫秒处理。后台维护器会主动关闭过期 coordinating PIT 和各 shard PIT。

### 向量检索

独立 `_knn_search`：

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

也可以在 `_search` 里携带 `knn`：

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

分布式向量查询会在 shard 侧扩大候选集，然后由协调节点做全局合并。

### update_by_query 和 delete_by_query

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

这两个接口会按 shard 分布式执行，并在对应 shard owner 上提交 Lucene 变更。

### 上传状态与自愈

查看所有 index 的异步上传状态：

```bash
curl http://127.0.0.1:9200/_uploads
curl http://127.0.0.1:9200/_snapshot_status
```

查看单个 index：

```bash
curl http://127.0.0.1:9200/books/_uploads
```

响应会按 shard 返回 owner、pending 文件数、状态计数、最新 remote snapshot generation，以及超过 1 分钟的 pending 是否被标记为 `stuck`。后台维护器会持续重试当前 owner 上的 pending 上传；也可以手动触发一次：

```bash
curl -X POST http://127.0.0.1:9200/books/_uploads/_retry -d '{}'
```

### ILM

创建 lifecycle policy：

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

绑定到索引：

```bash
curl -X PUT http://127.0.0.1:9200/books/_ilm/policy/delete-old \
  -H "content-type: application/json" \
  -d '{}'
```

当前后台执行器支持：

- `warm` phase：到期后由当前 shard owner 对本地 shard 执行一次 `forceMerge(1)` 并提交。
- `delete` phase：到期后删除 index metadata、本地 shard 数据、远端对象和 manifest metadata。

`min_age` 支持 `ms`, `s`, `m`, `h`, `d` 或毫秒数。`hot`、`cold`、`frozen` phase 当前可配置但不执行额外动作。

### 调试接口

```bash
curl -X POST http://127.0.0.1:9200/books/_search_plan \
  -H "content-type: application/json" \
  -d '{"query":{"match_all":{}}}'
```

`/_internal/...` 路由只用于节点之间转发，不建议客户端直接调用。

## 响应形态

搜索响应：

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

写入响应：

```json
{
  "indexName": "books",
  "shardId": {"indexName": "books", "shardNumber": 0},
  "id": "doc-1",
  "result": "created",
  "committed": true
}
```

错误响应：

```json
{
  "error": "index not found: books",
  "type": "IllegalArgumentException",
  "status": 404
}
```

常见状态码：

- `400`: 请求参数、mapping、query、sort、bulk 格式等客户端输入错误。
- `404`: index、mapping、PIT 等资源不存在或 PIT 已过期。
- `409`: index/document/mapping 已存在、写入 fence 过期、shard 当前不可写等状态冲突。
- `502`: 远端 shard RPC 或 S3-compatible 服务返回错误。
- `503`: 当前节点不是 master、master 不可用、无可用 DATA 节点、远端 commit snapshot 暂不可读、etcd/S3 客户端不可用等临时后端不可用。
- `500`: 本地 IO 或未分类内部错误。

## 压力测试

压力测试默认不随普通测试执行，需要显式打开：

```powershell
$env:LUCENE_S3_STRESS="true"
$env:LUCENE_S3_STRESS_DOCS="1000"
$env:LUCENE_S3_STRESS_BULK_SIZE="50"
$env:LUCENE_S3_STRESS_WRITE_THREADS="4"
$env:LUCENE_S3_STRESS_SEARCH_THREADS="4"
$env:LUCENE_S3_STRESS_SECONDS="8"
.\gradlew.bat :server:test --tests com.github.wxk6b1203.http.HttpApiServerStressTest --rerun-tasks --stacktrace
```

测试会启动单节点服务，混合执行 `_bulk`、`_search`、`_knn_search` 和观测接口请求，并把 JSON 报告写到 `server/build/reports/stress/http-api-stress.json`。可选环境变量还包括 `LUCENE_S3_STRESS_SHARDS`、`LUCENE_S3_STRESS_CATEGORIES`、`LUCENE_S3_STRESS_CACHE_MAX_BYTES` 和 `LUCENE_S3_STRESS_METRICS_PORT`。

也可以使用 `test/http_stress.py` 对已经启动的 HTTP 服务执行外部压测。下面是一组在 macOS M1 Pro 10C、32GB 内存机器上的本地样例结果，仅用于观察当前混合工作负载下的接口耗时形态；结果会受机器配置、S3/OSS endpoint、etcd、JVM 预热、索引状态和压测参数影响，不应视为通用性能基准。

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

## 故障和恢复边界

当前实现的核心假设是：S3/OSS 保存已经发布的 Lucene commit 文件，etcd 保存集群状态和 manifest 元数据，每个 shard 同一时间只有一个写 owner。写入提交后会用 Lucene `SnapshotDeletionPolicy` pin 住本地 commit 文件，异步上传完成并发布 manifest snapshot 后才释放；因此正常删除策略不会再清掉正在上传的 `segments_N` 和相关数据文件。

| 场景 | 当前行为 | 读写影响 | 验证入口 |
| --- | --- | --- | --- |
| shard owner 短暂不可达 | master 重新分配 owner，写请求通过 owner term/allocation epoch 做 fence | 重分配期间写入可能返回 `409`/`503`，恢复后可继续写 | etcd 集成测试覆盖 stale owner reroute |
| master 节点退出 | 具备 `MASTER` role 的存活节点重新竞选 master | 竞选期间管理类写入可能返回 `503`，已有 owner 的本地读写尽量不受影响 | etcd 集成测试覆盖 master failover 后协调节点写入 |
| S3/OSS 暂时失败 | manifest 文件保持 `DIRTY`/`UPLOADING`，后台维护任务重试当前 owner 的 pending uploads | `weak` 可读 owner 本地 commit；需要远端 snapshot 的 `strong`/remote 读可能返回 `503` 或跳过不可用 generation | `/_snapshot_status`、`/:index/_uploads` |
| 删除索引时远端删除失败 | 先写入 delete tombstone，再由维护任务重试删除本地/远端数据和 metadata | 索引处于 deleting 状态时拒绝新读写，避免删除一半又被继续使用 | `/_indices` 的 `delete_pending` |
| PIT 或 search pin 保护旧 snapshot | snapshot GC 会保留最新 N 个和仍被 pin 的 generation | PIT 期间旧 generation 不会被 GC；过期后维护任务清理 | PIT API、snapshot GC 测试 |
| 本地缓存损坏或丢失 | 重新从 manifest snapshot 对应对象下载 | 首次读取变慢；远端对象缺失时返回 `503` | remote cache 统计和错误响应 |

## 当前限制和注意事项

- 项目仍处于实验阶段，不建议直接用于生产。
- S3 是提交文件的持久化来源，但最新 commit 在异步上传完成前仍可能只存在于 shard owner 本地。需要读到这部分数据时使用默认 `weak`。
- 不支持 `number_of_replicas > 0`，也没有 primary/replica failover 语义。
- 多节点必须使用 etcd；不配置 etcd 的内存元数据模式不能跨进程共享。
- `--conf` 支持 YAML/JSON 配置文件；命令行参数会覆盖配置文件。
- Windows/Linux/macOS 均通过 Java NIO Path 处理本地路径。Windows 上删除打开中的 Lucene 文件可能需要等待，代码中已经对递归删除做了重试。
