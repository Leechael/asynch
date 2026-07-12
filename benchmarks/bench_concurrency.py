"""Compare pool-level coroutine and thread concurrency without claiming multiplexing."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from time import perf_counter

from asynch.pool import Pool
from benchmarks.bench_pool_contention import (
    csv_ints,
    default_dsn,
    distribution,
    percentile,
    server_snapshot,
)


async def asynch_round(dsn: str, workers: int, iterations: int) -> dict[str, object]:
    start = asyncio.Event()
    all_acquired = asyncio.Event()
    acquired = 0
    acquired_lock = asyncio.Lock()

    async with Pool(minsize=workers, maxsize=workers, dsn=dsn) as pool:

        async def worker() -> list[float]:
            nonlocal acquired
            async with pool.connection() as connection:
                async with acquired_lock:
                    acquired += 1
                    if acquired == workers:
                        all_acquired.set()
                await start.wait()
                latencies: list[float] = []
                for _ in range(iterations):
                    started = perf_counter()
                    result = await connection._connection.execute("SELECT 1")
                    latencies.append(perf_counter() - started)
                    if result != [(1,)]:
                        raise RuntimeError(f"SELECT 1 returned {result!r}, not [(1,)]")
                return latencies

        tasks = [asyncio.create_task(worker()) for _ in range(workers)]
        await all_acquired.wait()
        started = perf_counter()
        start.set()
        samples = await asyncio.gather(*tasks)
        elapsed = perf_counter() - started

    latencies = [latency for worker_samples in samples for latency in worker_samples]
    return {
        "operations": workers * iterations,
        "elapsed_s": elapsed,
        "qps": workers * iterations / elapsed,
        "latency": distribution(latencies),
    }


def sync_round(dsn: str, workers: int, iterations: int) -> dict[str, object]:
    ready = threading.Event()
    start = threading.Event()
    ready_count = 0
    ready_lock = threading.Lock()

    def worker() -> list[float]:
        nonlocal ready_count
        from clickhouse_driver import Client

        client = Client.from_url(dsn)
        try:
            if client.execute("SELECT 1") != [(1,)]:
                raise RuntimeError("synchronous warmup SELECT 1 returned an unexpected result")
            with ready_lock:
                ready_count += 1
                if ready_count == workers:
                    ready.set()
            start.wait()
            latencies: list[float] = []
            for _ in range(iterations):
                started = perf_counter()
                result = client.execute("SELECT 1")
                latencies.append(perf_counter() - started)
                if result != [(1,)]:
                    raise RuntimeError(f"SELECT 1 returned {result!r}, not [(1,)]")
            return latencies
        finally:
            client.disconnect()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(worker) for _ in range(workers)]
        if not ready.wait(timeout=30):
            raise RuntimeError("synchronous workers did not establish exclusive connections")
        started = perf_counter()
        start.set()
        samples = [future.result() for future in futures]
        elapsed = perf_counter() - started

    latencies = [latency for worker_samples in samples for latency in worker_samples]
    return {
        "operations": workers * iterations,
        "elapsed_s": elapsed,
        "qps": workers * iterations / elapsed,
        "latency": distribution(latencies),
    }


async def run(args: argparse.Namespace) -> dict[str, object]:
    snapshot = await server_snapshot(args.dsn)
    configurations: list[dict[str, object]] = []
    modes = (("asynch_pool", asynch_round), ("sync_thread_executor", sync_round))
    for workers in args.workers:
        measurements = []
        for name, round_fn in modes:
            if name == "asynch_pool":
                await round_fn(args.dsn, workers, args.iterations)
            else:
                await asyncio.to_thread(round_fn, args.dsn, workers, args.iterations)
            results = []
            for round_number in range(1, args.rounds + 1):
                if name == "asynch_pool":
                    result = await round_fn(args.dsn, workers, args.iterations)
                else:
                    result = await asyncio.to_thread(round_fn, args.dsn, workers, args.iterations)
                results.append(dict(result, round=round_number))
            qps = [float(result["qps"]) for result in results]
            combined_latency = [
                value / 1000 for result in results for value in result["latency"]["raw_ms"]
            ]
            measurements.append(
                {
                    "mode": name,
                    "rounds": results,
                    "raw_qps": qps,
                    "qps_p50": percentile(qps, 50),
                    "qps_p90": percentile(qps, 90),
                    "qps_p99": percentile(qps, 99),
                    "qps_max": max(qps),
                    "query_latency": distribution(combined_latency),
                }
            )
        configurations.append({"workers": workers, "measurements": measurements})
    return {
        "benchmark": "pool_level_concurrency",
        "environment": {
            **snapshot,
            "python": sys.version.split()[0],
            "workers": args.workers,
            "iterations": args.iterations,
            "rounds": args.rounds,
        },
        "scope": "Pool-level coroutine-versus-thread overhead; not protocol concurrency.",
        "configurations": configurations,
    }


def emit(result: dict[str, object], as_json: bool) -> None:
    if as_json:
        print(json.dumps(result, sort_keys=True))  # noqa: T201
        return
    environment = result["environment"]
    print("WP01 pool-level concurrency benchmark")  # noqa: T201
    print(  # noqa: T201
        "environment: ClickHouse={version} revision={revision}; Python={python}; "
        "workers={workers}; iterations={iterations}; rounds={rounds} (+1 warmup)".format(
            **environment
        )
    )
    print(f"scope: {result['scope']}")  # noqa: T201
    for configuration in result["configurations"]:
        for measurement in configuration["measurements"]:
            latency = measurement["query_latency"]
            print(  # noqa: T201
                "workers={workers} mode={mode} qps p50={qps_p50:.3f} p90={qps_p90:.3f} "
                "p99={qps_p99:.3f} max={qps_max:.3f} raw={raw_qps}; query-latency "
                "p50={p50_ms:.3f}ms p90={p90_ms:.3f}ms p99={p99_ms:.3f}ms max={max_ms:.3f}ms".format(
                    **configuration, **measurement, **latency
                )
            )
            print(f" query_latency_raw_ms={latency['raw_ms']}")  # noqa: T201


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default=default_dsn())
    parser.add_argument("--workers", type=csv_ints, default=[1, 10, 50, 100])
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args()
    if args.rounds < 5:
        parser.error("--rounds must be at least 5")
    if args.iterations < 1:
        parser.error("--iterations must be positive")
    return args


def main() -> None:
    args = parse_args()
    try:
        emit(asyncio.run(run(args)), args.as_json)
    except Exception as error:
        print(f"benchmark error: {error}", file=sys.stderr)  # noqa: T201
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
