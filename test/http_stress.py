#!/usr/bin/env python3
"""HTTP stress workload for Lucene S3.

The script intentionally uses only Python standard library modules so it can run
on a development machine without installing a benchmark framework first.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import statistics
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


DEFAULT_CATEGORIES = [
    "search",
    "storage",
    "lucene",
    "s3",
    "distributed",
    "vector",
    "observability",
]


@dataclass
class HttpResult:
    status: int
    elapsed_ms: float
    body: str
    error: str | None = None

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300


@dataclass
class OperationStats:
    latencies_ms: list[float] = field(default_factory=list)
    statuses: dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    bytes_received: int = 0

    def record(self, result: HttpResult, max_errors: int) -> None:
        self.latencies_ms.append(result.elapsed_ms)
        self.statuses[str(result.status)] = self.statuses.get(str(result.status), 0) + 1
        self.bytes_received += len(result.body.encode("utf-8"))
        if (not result.ok or result.error) and len(self.errors) < max_errors:
            message = result.error or result.body[:300]
            self.errors.append(f"status={result.status} {message}")


class Metrics:
    def __init__(self, max_errors: int) -> None:
        self._lock = threading.Lock()
        self._operations: dict[str, OperationStats] = {}
        self._counters: dict[str, int] = {}
        self._max_errors = max_errors

    def record(self, operation: str, result: HttpResult) -> None:
        with self._lock:
            stats = self._operations.setdefault(operation, OperationStats())
            stats.record(result, self._max_errors)

    def add(self, counter: str, value: int = 1) -> None:
        with self._lock:
            self._counters[counter] = self._counters.get(counter, 0) + value

    def report(self) -> dict[str, Any]:
        with self._lock:
            operations = {
                name: summarize_operation(stats)
                for name, stats in sorted(self._operations.items())
            }
            counters = dict(sorted(self._counters.items()))
        return {
            "operations": operations,
            "counters": counters,
        }


def summarize_operation(stats: OperationStats) -> dict[str, Any]:
    latencies = sorted(stats.latencies_ms)
    count = len(latencies)
    return {
        "count": count,
        "statuses": stats.statuses,
        "errors": stats.errors,
        "bytes_received": stats.bytes_received,
        "latency_ms": {
            "min": round(latencies[0], 3) if latencies else None,
            "avg": round(statistics.fmean(latencies), 3) if latencies else None,
            "p50": percentile(latencies, 50),
            "p90": percentile(latencies, 90),
            "p95": percentile(latencies, 95),
            "p99": percentile(latencies, 99),
            "max": round(latencies[-1], 3) if latencies else None,
        },
    }


def percentile(sorted_values: list[float], percentile_value: int) -> float | None:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return round(sorted_values[0], 3)
    rank = (len(sorted_values) - 1) * percentile_value / 100
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return round(sorted_values[int(rank)], 3)
    weight = rank - lower
    return round(sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight, 3)


def request(
    base_url: str,
    method: str,
    path: str,
    body: Any | None = None,
    content_type: str = "application/json",
    timeout: float = 10.0,
) -> HttpResult:
    url = base_url.rstrip("/") + path
    payload = None
    if body is not None:
        if content_type == "application/x-ndjson":
            payload = str(body).encode("utf-8")
        else:
            payload = json.dumps(body, separators=(",", ":")).encode("utf-8")
    headers = {"content-type": content_type}
    started = time.perf_counter()
    try:
        req = urllib.request.Request(url, data=payload, headers=headers, method=method)
        with urllib.request.urlopen(req, timeout=timeout) as response:
            response_body = response.read().decode("utf-8", errors="replace")
            elapsed_ms = (time.perf_counter() - started) * 1000
            return HttpResult(response.status, elapsed_ms, response_body)
    except urllib.error.HTTPError as exc:
        response_body = exc.read().decode("utf-8", errors="replace")
        elapsed_ms = (time.perf_counter() - started) * 1000
        return HttpResult(exc.code, elapsed_ms, response_body)
    except Exception as exc:  # noqa: BLE001 - benchmark should capture transport failures.
        elapsed_ms = (time.perf_counter() - started) * 1000
        return HttpResult(0, elapsed_ms, "", repr(exc))


def setup_index(args: argparse.Namespace, metrics: Metrics) -> None:
    if args.reset_index:
        delete = request(args.base_url, "DELETE", f"/{args.index}", timeout=args.timeout)
        if delete.status == 200:
            metrics.add("setup_deleted_index")
        elif delete.status == 404:
            metrics.add("setup_delete_index_not_found")
        else:
            metrics.record("setup_delete_index", delete)
            raise RuntimeError(f"delete index failed: status={delete.status} body={delete.body}")
        wait_for_index_absent(args)

    body = {
        "settings": {
            "number_of_shards": args.shards,
            "number_of_replicas": 0,
        },
        "mappings": {
            "properties": {
                "title": {"type": "text"},
                "description": {"type": "text"},
                "category": {"type": "keyword"},
                "tenant": {"type": "keyword"},
                "price": {"type": "double"},
                "pages": {"type": "long"},
                "available": {"type": "boolean"},
                "published_at": {"type": "long"},
                "embedding": {
                    "type": "dense_vector",
                    "dimension": 3,
                    "similarity": "cosine",
                },
            }
        },
    }
    create = request(args.base_url, "PUT", f"/{args.index}", body, timeout=args.timeout)
    if create.status == 200:
        metrics.add("setup_created_index")
    elif create.status == 409:
        metrics.add("setup_index_already_exists")
    else:
        metrics.record("setup_create_index", create)
        raise RuntimeError(f"create index failed: status={create.status} body={create.body}")

    if args.warmup_docs > 0:
        warmup_docs(args, metrics)


def wait_for_index_absent(args: argparse.Namespace) -> None:
    deadline = time.monotonic() + args.setup_timeout
    while time.monotonic() < deadline:
        result = request(args.base_url, "GET", f"/{args.index}", timeout=args.timeout)
        if result.status == 404:
            return
        time.sleep(0.2)


def warmup_docs(args: argparse.Namespace, metrics: Metrics) -> None:
    remaining = args.warmup_docs
    sequence = 0
    while remaining > 0:
        count = min(args.bulk_size, remaining)
        body, docs = bulk_body(args.index, "warmup", sequence, count, args.categories)
        result = request(
            args.base_url,
            "POST",
            "/_bulk",
            body,
            content_type="application/x-ndjson",
            timeout=args.timeout,
        )
        metrics.record("warmup_bulk", result)
        if not result.ok:
            raise RuntimeError(f"warmup bulk failed: status={result.status} body={result.body}")
        metrics.add("warmup_docs", docs)
        sequence += count
        remaining -= count


def bulk_body(
    index: str,
    writer_id: str,
    start: int,
    count: int,
    categories: list[str],
) -> tuple[str, int]:
    lines: list[str] = []
    for offset in range(count):
        sequence = start + offset
        doc_id = f"{writer_id}-{sequence}"
        category = categories[sequence % len(categories)]
        routing = f"tenant-{sequence % 128}"
        action = {"index": {"_index": index, "_id": doc_id, "routing": routing}}
        source = document_source(sequence, writer_id, category)
        lines.append(json.dumps(action, separators=(",", ":")))
        lines.append(json.dumps(source, separators=(",", ":")))
    return "\n".join(lines) + "\n", count


def document_source(sequence: int, writer_id: str, category: str) -> dict[str, Any]:
    return {
        "title": f"Lucene S3 stress document {sequence}",
        "description": (
            f"deterministic benchmark payload for {category} writer {writer_id} "
            f"sequence {sequence}"
        ),
        "category": category,
        "tenant": f"tenant-{sequence % 128}",
        "price": round(10 + (sequence % 1000) * 0.37, 2),
        "pages": 50 + sequence % 900,
        "available": sequence % 3 != 0,
        "published_at": 20260101 + sequence % 500,
        "embedding": vector(sequence),
    }


def vector(sequence: int) -> list[float]:
    return [
        round(((sequence * 31) % 1000) / 1000, 4),
        round(((sequence * 17 + 13) % 1000) / 1000, 4),
        round(((sequence * 7 + 29) % 1000) / 1000, 4),
    ]


def writer_worker(args: argparse.Namespace, worker: int, stop_at: float, metrics: Metrics) -> None:
    sequence = worker * 1_000_000_000
    writer_id = f"writer-{worker}"
    while time.monotonic() < stop_at:
        body, docs = bulk_body(args.index, writer_id, sequence, args.bulk_size, args.categories)
        result = request(
            args.base_url,
            "POST",
            "/_bulk",
            body,
            content_type="application/x-ndjson",
            timeout=args.timeout,
        )
        metrics.record("bulk_write", result)
        if result.ok:
            metrics.add("bulk_requests")
            metrics.add("bulk_docs", docs)
            record_bulk_item_errors(result, metrics)
        sequence += docs


def record_bulk_item_errors(result: HttpResult, metrics: Metrics) -> None:
    try:
        payload = json.loads(result.body)
    except json.JSONDecodeError:
        metrics.add("bulk_response_parse_errors")
        return
    if payload.get("errors") is True:
        metrics.add("bulk_responses_with_item_errors")
        for item in payload.get("items", []):
            action = next(iter(item.values()))
            status = int(action.get("status", 0))
            if status >= 300:
                metrics.add(f"bulk_item_status_{status}")


def search_worker(args: argparse.Namespace, worker: int, stop_at: float, metrics: Metrics) -> None:
    rng = random.Random(args.seed + worker)
    operations = [
        weak_search,
        strong_search,
        aggregation_search,
        search_after,
    ]
    while time.monotonic() < stop_at:
        operation = rng.choice(operations)
        operation(args, rng, metrics)


def knn_worker(args: argparse.Namespace, worker: int, stop_at: float, metrics: Metrics) -> None:
    rng = random.Random(args.seed + 10_000 + worker)
    while time.monotonic() < stop_at:
        result = request(
            args.base_url,
            "POST",
            f"/{args.index}/_knn_search",
            {
                "knn": {
                    "field": "embedding",
                    "query_vector": vector(rng.randint(0, 1_000_000)),
                    "k": args.search_size,
                    "num_candidates": max(args.search_size * 5, 50),
                    "filter": {"term": {"category": rng.choice(args.categories)}},
                },
                "read_preference": "weak",
            },
            timeout=args.timeout,
        )
        metrics.record("knn_search", result)


def observe_worker(args: argparse.Namespace, worker: int, stop_at: float, metrics: Metrics) -> None:
    paths = [
        "/_cluster/health",
        "/_indices",
        "/_shards",
        "/_nodes/stats",
        "/_snapshot_status",
        f"/{args.index}/_uploads",
    ]
    position = worker % len(paths)
    while time.monotonic() < stop_at:
        path = paths[position % len(paths)]
        result = request(args.base_url, "GET", path, timeout=args.timeout)
        metrics.record(f"observe {path}", result)
        position += 1
        time.sleep(args.observe_interval)


def weak_search(args: argparse.Namespace, rng: random.Random, metrics: Metrics) -> None:
    result = request(
        args.base_url,
        "POST",
        f"/{args.index}/_search?read_preference=weak",
        {
            "query": {"term": {"category": rng.choice(args.categories)}},
            "size": args.search_size,
            "sort": [
                {"price": {"order": "asc"}},
                {"_id": {"order": "asc"}},
            ],
        },
        timeout=args.timeout,
    )
    metrics.record("weak_search", result)


def strong_search(args: argparse.Namespace, rng: random.Random, metrics: Metrics) -> None:
    result = request(
        args.base_url,
        "POST",
        f"/{args.index}/_search?read_preference=strong",
        {
            "query": {"match": {"description": "deterministic"}},
            "size": args.search_size,
        },
        timeout=args.timeout,
    )
    metrics.record("strong_search", result)


def aggregation_search(args: argparse.Namespace, rng: random.Random, metrics: Metrics) -> None:
    max_price = 20 + rng.randint(0, 300)
    result = request(
        args.base_url,
        "POST",
        f"/{args.index}/_search",
        {
            "query": {
                "bool": {
                    "filter": [
                        {"range": {"price": {"lte": max_price}}},
                        {"term": {"available": True}},
                    ]
                }
            },
            "size": 0,
            "aggs": {
                "by_category": {"terms": {"field": "category", "size": 10}},
                "avg_price": {"avg": {"field": "price"}},
            },
        },
        timeout=args.timeout,
    )
    metrics.record("aggregation_search", result)


def search_after(args: argparse.Namespace, rng: random.Random, metrics: Metrics) -> None:
    price = round(10 + rng.randint(0, 500) * 0.37, 2)
    result = request(
        args.base_url,
        "POST",
        f"/{args.index}/_search",
        {
            "query": {"match_all": {}},
            "size": args.search_size,
            "sort": [
                {"price": {"order": "asc"}},
                {"_id": {"order": "asc"}},
            ],
            "search_after": [price, ""],
        },
        timeout=args.timeout,
    )
    metrics.record("search_after", result)


def run_workload(args: argparse.Namespace, metrics: Metrics) -> None:
    stop_at = time.monotonic() + args.duration
    tasks = []
    with ThreadPoolExecutor(max_workers=args.total_workers) as executor:
        for worker in range(args.write_workers):
            tasks.append(executor.submit(writer_worker, args, worker, stop_at, metrics))
        for worker in range(args.read_workers):
            tasks.append(executor.submit(search_worker, args, worker, stop_at, metrics))
        for worker in range(args.knn_workers):
            tasks.append(executor.submit(knn_worker, args, worker, stop_at, metrics))
        for worker in range(args.observe_workers):
            tasks.append(executor.submit(observe_worker, args, worker, stop_at, metrics))
        for task in as_completed(tasks, timeout=args.duration + args.timeout + 5):
            task.result()


def print_summary(report: dict[str, Any]) -> None:
    print("\nHTTP stress summary")
    print(f"base_url: {report['base_url']}")
    print(f"index:    {report['index']}")
    print(f"duration: {report['duration_seconds']}s")
    print("")
    print(f"{'operation':34} {'count':>8} {'avg':>9} {'p95':>9} {'p99':>9} {'statuses':>18}")
    print("-" * 94)
    for name, stats in report["metrics"]["operations"].items():
        latency = stats["latency_ms"]
        statuses = ",".join(f"{k}:{v}" for k, v in sorted(stats["statuses"].items()))
        print(
            f"{name[:34]:34} "
            f"{stats['count']:8d} "
            f"{value_or_dash(latency['avg']):>9} "
            f"{value_or_dash(latency['p95']):>9} "
            f"{value_or_dash(latency['p99']):>9} "
            f"{statuses:>18}"
        )
    counters = report["metrics"]["counters"]
    if counters:
        print("\ncounters:")
        for name, value in counters.items():
            print(f"  {name}: {value}")


def value_or_dash(value: Any) -> str:
    return "-" if value is None else str(value)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lucene S3 HTTP stress workload")
    parser.add_argument("--base-url", default="http://127.0.0.1:9200")
    parser.add_argument("--index", default="stress_books")
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--shards", type=int, default=3)
    parser.add_argument("--bulk-size", type=int, default=50)
    parser.add_argument("--write-workers", type=int, default=2)
    parser.add_argument("--read-workers", type=int, default=4)
    parser.add_argument("--knn-workers", type=int, default=1)
    parser.add_argument("--observe-workers", type=int, default=1)
    parser.add_argument("--search-size", type=int, default=10)
    parser.add_argument("--warmup-docs", type=int, default=500)
    parser.add_argument("--reset-index", action="store_true")
    parser.add_argument("--setup-timeout", type=float, default=30.0)
    parser.add_argument("--timeout", type=float, default=15.0)
    parser.add_argument("--observe-interval", type=float, default=1.0)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--max-errors", type=int, default=20)
    parser.add_argument("--fail-on-errors", action="store_true")
    parser.add_argument("--report", default="test/http-stress-report.json")
    parser.add_argument(
        "--categories",
        default=",".join(DEFAULT_CATEGORIES),
        help="Comma-separated category values used by writers and searches.",
    )
    args = parser.parse_args()
    args.categories = [item.strip() for item in args.categories.split(",") if item.strip()]
    if not args.categories:
        raise SystemExit("--categories must not be empty")
    args.total_workers = (
        args.write_workers
        + args.read_workers
        + args.knn_workers
        + args.observe_workers
    )
    if args.total_workers <= 0:
        raise SystemExit("at least one worker is required")
    return args


def has_errors(report: dict[str, Any]) -> bool:
    for stats in report["metrics"]["operations"].values():
        for status, count in stats["statuses"].items():
            if status == "0" or int(status) >= 300:
                if count > 0:
                    return True
        if stats["errors"]:
            return True
    return False


def main() -> int:
    args = parse_args()
    metrics = Metrics(max_errors=args.max_errors)
    started = time.time()

    setup_index(args, metrics)
    run_workload(args, metrics)

    report = {
        "base_url": args.base_url,
        "index": args.index,
        "duration_seconds": args.duration,
        "started_at_epoch_seconds": started,
        "finished_at_epoch_seconds": time.time(),
        "options": {
            "shards": args.shards,
            "bulk_size": args.bulk_size,
            "write_workers": args.write_workers,
            "read_workers": args.read_workers,
            "knn_workers": args.knn_workers,
            "observe_workers": args.observe_workers,
            "search_size": args.search_size,
            "warmup_docs": args.warmup_docs,
            "reset_index": args.reset_index,
            "categories": args.categories,
        },
        "metrics": metrics.report(),
    }
    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print_summary(report)
    print(f"\nreport: {report_path}")

    if args.fail_on_errors and has_errors(report):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
