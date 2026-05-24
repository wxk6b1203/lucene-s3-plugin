# HTTP Stress Test

`http_stress.py` 是一个无第三方依赖的 HTTP 压测脚本，用于对已启动的 Lucene S3 服务混合执行写入、读取、向量查询和观测接口请求。

脚本默认不会删除已有索引，只会尝试创建 `stress_books`。如果索引已存在，会继续复用。需要清理重建时显式传入 `--reset-index`。

如果已有同名索引不是本文档里的压测 mapping，例如缺少 `embedding` 向量字段，读取侧的 `_knn_search` 可能会返回错误。这种情况下建议使用 `--reset-index` 重建压测索引。

## 启动服务

单节点本地开发示例：

```bash
./gradlew :server:run --args='server --http-port 9200 --data-path data/node-1'
```

如果要使用本地配置：

```bash
./gradlew :server:run --args='server --conf config/config.local.yml'
```

## 运行压测

轻量 smoke 压测：

```bash
python3 test/http_stress.py \
  --base-url http://127.0.0.1:9200 \
  --duration 10 \
  --warmup-docs 100 \
  --write-workers 1 \
  --read-workers 2 \
  --knn-workers 1 \
  --observe-workers 1
```

较高并发混合压测：

```bash
python3 test/http_stress.py \
  --base-url http://127.0.0.1:9200 \
  --index stress_books \
  --duration 60 \
  --shards 3 \
  --warmup-docs 2000 \
  --bulk-size 100 \
  --write-workers 4 \
  --read-workers 8 \
  --knn-workers 2 \
  --observe-workers 1 \
  --report test/http-stress-report.json
```

清理并重建压测索引：

```bash
python3 test/http_stress.py --reset-index --duration 30
```

如果希望出现 HTTP 非 2xx 或网络错误时返回非 0 exit code：

```bash
python3 test/http_stress.py --duration 30 --fail-on-errors
```

## 覆盖的接口

写入：

- `PUT /<index>`
- `POST /_bulk`

读取：

- `POST /<index>/_search?read_preference=weak`
- `POST /<index>/_search?read_preference=strong`
- `POST /<index>/_search`，包含聚合和 `search_after`
- `POST /<index>/_knn_search`

观测：

- `GET /_cluster/health`
- `GET /_indices`
- `GET /_shards`
- `GET /_nodes/stats`
- `GET /_snapshot_status`
- `GET /<index>/_uploads`

## 常用参数

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `--base-url` | `http://127.0.0.1:9200` | 服务地址。 |
| `--index` | `stress_books` | 压测索引名。 |
| `--duration` | `30` | 压测持续时间，单位秒。 |
| `--shards` | `3` | 创建索引时的 shard 数。索引已存在时不会修改。 |
| `--warmup-docs` | `500` | 正式压测前预写入的文档数。 |
| `--bulk-size` | `50` | 每个 `_bulk` 请求写入的文档数。 |
| `--write-workers` | `2` | 并发写入线程数。 |
| `--read-workers` | `4` | 并发普通搜索线程数。 |
| `--knn-workers` | `1` | 并发向量查询线程数。 |
| `--observe-workers` | `1` | 并发观测接口线程数。 |
| `--search-size` | `10` | 搜索请求的 size/k。 |
| `--timeout` | `15` | 单个 HTTP 请求超时时间，单位秒。 |
| `--reset-index` | `false` | 删除并重建压测索引。 |
| `--fail-on-errors` | `false` | 检测到错误时以非 0 状态退出。 |
| `--report` | `test/http-stress-report.json` | JSON 报告输出路径。 |

## 输出

运行结束后会在终端打印每类操作的请求数、平均耗时、p95、p99 和状态码分布，并把完整 JSON 报告写到 `--report` 指定路径。

报告示例结构：

```json
{
  "base_url": "http://127.0.0.1:9200",
  "index": "stress_books",
  "duration_seconds": 30,
  "options": {},
  "metrics": {
    "operations": {
      "bulk_write": {
        "count": 120,
        "statuses": {"200": 120},
        "latency_ms": {"avg": 42.1, "p95": 88.4, "p99": 130.2}
      }
    },
    "counters": {
      "bulk_docs": 6000
    }
  }
}
```

## 注意事项

- 脚本压测的是外部 HTTP API，需要服务先启动。
- 默认写入数据是确定性构造的测试文档，字段覆盖 text、keyword、数字、boolean 和 dense vector。
- `--reset-index` 会删除同名索引及其本地/远端数据，只建议对压测索引使用。
- 如果服务启用了异步上传，写入 API 返回快并不代表远端 snapshot 已经 clean，可以配合 `/_uploads`、`/_snapshot_status` 和 Prometheus 指标一起看。
