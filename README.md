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
- `generator`: 辅助模块。

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
<index>__shard_<n>/_data/<lucene-file>
```

Lucene 文件只有在具体 `segments_N` commit 可见后才被当作可发布文件。ManifestManager 会先上传数据文件，再上传 committed segment 文件，并通过文件状态区分：

- `DIRTY`: 本地已 commit，但远端对象未完成。
- `UPLOADING`: 正在上传。
- `CLEAN`: 已上传，可作为 remote snapshot 读取。
- `PINNED`: 可读且被保留的远端文件。

### 读一致性

公开 API 的 `read_preference` 只暴露两个值：

- `weak`, 默认值。每个 shard 独立选择读路径：如果该 shard 还有 `DIRTY` 或 `UPLOADING` 文件，则读当前 owner 的本地已提交数据；如果 remote snapshot 已经干净，则分配给负载较低的数据节点读取 S3/cache。
- `strong`。只读取远端 clean/pinned snapshot，因此不会依赖当前 owner 本地状态。若最新 commit 仍在上传中，strong 读可能看到较旧但已经完整上传的快照。

这种模型允许一个 index 的不同 shard 同时处于不同状态：例如 3 个 shard 中 2 个已经上传到 S3，1 个还在 owner 本地上传中，`weak` 查询会混合读取远端和本地；`strong` 查询只读远端快照。

### 无副本语义

本项目不维护 Lucene 副本，也没有 primary/replica shard 概念：

- `number_of_replicas` 必须省略或为 `0`。
- shard owner 只是当前写入节点，不代表 primary。
- owner 掉线后，master 会根据存活 DATA 节点重新分派 shard owner。
- 已上传的提交数据从 S3 恢复；未上传的 owner 本地最新 commit 仍然依赖原 owner 存活。
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
./gradlew :server:installDist
```

Windows 使用：

```powershell
.\gradlew.bat :server:installDist
```

## 配置参数

当前配置以 CLI 参数为主。`--conf` 参数目前只会被记录到日志，尚未解析配置文件。

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `--http-port`, `-p` | `9200` | HTTP API 监听端口。 |
| `--cluster-name` | `lucene-s3` | 集群名称。 |
| `--node-id` | 随机 UUID | 稳定节点 ID。多节点重启建议显式指定。 |
| `--node-name` | `local-node` | 节点显示名。 |
| `--host` | `127.0.0.1` | 对外发布的 host，跨节点转发会使用它。 |
| `--roles` | `MASTER,DATA,COORDINATING` | 逗号分隔角色，可选 `MASTER`,`DATA`,`INGEST`,`COORDINATING`。服务会自动补充 coordinating 能力。 |
| `--etcd-endpoints` | 空 | etcd endpoint，例如 `http://127.0.0.1:2379`。为空时使用单节点内存元数据。 |
| `--etcd-namespace` | `lucene-s3/cluster` | etcd 中保存集群状态和 manifest 元数据的 namespace。 |
| `--data-path` | `data` | 本地 WAL、共享缓存和本地 fallback 对象存储目录。 |
| `--s3-bucket` | 空 | S3 bucket。为空时启用本地 `remote-objects` fallback。 |
| `--s3-region` | 空 | S3 region。未指定时走 AWS SDK 默认配置。 |
| `--s3-endpoint` | 空 | S3 兼容服务 endpoint，例如 MinIO。 |
| `--s3-access-key` | 空 | S3 access key。未指定时走 AWS SDK 默认凭证链。 |
| `--s3-secret-key` | 空 | S3 secret key。未指定时走 AWS SDK 默认凭证链。 |

## HTTP API

PowerShell 如果 `curl` 是别名，建议使用 `curl.exe`。示例均假设服务在 `http://127.0.0.1:9200`。

### 集群状态

```bash
curl http://127.0.0.1:9200/_cluster/state
curl http://127.0.0.1:9200/_nodes
```

查看某个 routing 会写到哪个节点：

```bash
curl "http://127.0.0.1:9200/books/_write_route?routing=user-1"
```

### 创建索引

```bash
curl -X PUT http://127.0.0.1:9200/books \
  -H "content-type: application/json" \
  -d '{
    "number_of_shards": 3,
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

支持的 mapping 类型：

- `keyword`: 精确匹配，默认开启 doc values，可排序和聚合。
- `text`: 使用 `StandardAnalyzer` 分词，默认不开启 doc values。
- `long`, `integer`, `double`, `float`: 数值查询、排序、聚合。
- `boolean`: 布尔查询、排序、聚合。
- `dense_vector`: 向量检索字段，需要 `dimension` 或 `dims`，可选 `similarity`。

字段参数：

- `type`: 字段类型，默认 `keyword`。
- `dimension` 或 `dims`: 向量维度。
- `similarity`: `cosine`, `dot_product`, `euclidean`, `maximum_inner_product`。
- `indexed` 或 `index`: 是否建索引，默认 `true`。
- `stored` 或 `store`: 是否 stored，默认 `true`。
- `doc_values` 或 `docValues`: 是否开启 doc values；keyword、数字、boolean 默认开启。

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

当前后台执行器主要实现 delete phase：到期后删除 index metadata、本地 shard 数据、远端对象和 manifest metadata。`min_age` 支持 `ms`, `s`, `m`, `h`, `d` 或毫秒数。

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

## 当前限制和注意事项

- 项目仍处于实验阶段，不建议直接用于生产。
- S3 是提交文件的持久化来源，但最新 commit 在异步上传完成前仍可能只存在于 shard owner 本地。需要读到这部分数据时使用默认 `weak`。
- 不支持 `number_of_replicas > 0`，也没有 primary/replica failover 语义。
- 多节点必须使用 etcd；不配置 etcd 的内存元数据模式不能跨进程共享。
- `--conf` 目前未实现配置文件解析。
- Windows/Linux/macOS 均通过 Java NIO Path 处理本地路径。Windows 上删除打开中的 Lucene 文件可能需要等待，代码中已经对递归删除做了重试。
