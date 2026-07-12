"""Measure Pool acquisition contention without adding driver instrumentation."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections.abc import Iterable
from time import perf_counter

from asynch.connection import Connection
from asynch.pool import Pool
from asynch.proto import constants


def default_dsn() -> str:
    return os.environ.get(
        "CLICKHOUSE_DSN",
        "clickhouse://{user}:{password}@{host}:{port}/{database}".format(
            user=os.environ.get("CLICKHOUSE_USER", constants.DEFAULT_USER),
            password=os.environ.get("CLICKHOUSE_PASSWORD", constants.DEFAULT_PASSWORD),
            host=os.environ.get("CLICKHOUSE_HOST", constants.DEFAULT_HOST),
            port=os.environ.get("CLICKHOUSE_PORT", constants.DEFAULT_PORT),
            database=os.environ.get("CLICKHOUSE_DB", constants.DEFAULT_DATABASE),
        ),
    )


def percentile(values: Iterable[float], percent: int) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("cannot calculate a percentile for no values")
    index = max(0, min(len(ordered) - 1, round((percent / 100) * (len(ordered) - 1))))
    return ordered[index]


def distribution(values: list[float]) -> dict[str, object]:
    return {
        "count": len(values),
        "p50_ms": percentile(values, 50) * 1000,
        "p90_ms": percentile(values, 90) * 1000,
        "p99_ms": percentile(values, 99) * 1000,
        "max_ms": max(values) * 1000,
        "raw_ms": [value * 1000 for value in values],
    }


def csv_ints(value: str) -> list[int]:
    try:
        values = [int(part) for part in value.split(",")]
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected a comma-separated list of integers") from error
    if not values or any(item < 1 for item in values):
        raise argparse.ArgumentTypeError("all values must be positive")
    return values


async def server_snapshot(dsn: str) -> dict[str, object]:
    async with Connection(dsn=dsn) as connection:
        async with connection.cursor() as cursor:
            await cursor.execute("SELECT version(), revision()")
            version, revision = await cursor.fetchone()
    return {"version": version, "revision": revision}


async def one_round(dsn: str, workers: int, maxsize: int, iterations: int) -> dict[str, object]:
    start = asyncio.Event()

    async with Pool(minsize=maxsize, maxsize=maxsize, dsn=dsn) as pool:

        async def worker() -> tuple[list[float], list[float]]:
            acquire_waits: list[float] = []
            cycle_times: list[float] = []
            await start.wait()
            for _ in range(iterations):
                cycle_started = perf_counter()
                async with pool.connection() as connection:
                    acquire_waits.append(perf_counter() - cycle_started)
                    async with connection.cursor() as cursor:
                        await cursor.execute("SELECT 1")
                        answer = await cursor.fetchone()
                    if answer != (1,):
                        raise RuntimeError(f"SELECT 1 returned {answer!r}, not (1,)")
                cycle_times.append(perf_counter() - cycle_started)
            return acquire_waits, cycle_times

        tasks = [asyncio.create_task(worker()) for _ in range(workers)]
        started = perf_counter()
        start.set()
        samples = await asyncio.gather(*tasks)
        elapsed = perf_counter() - started

    acquire_waits = [wait for worker_waits, _ in samples for wait in worker_waits]
    cycle_times = [duration for _, worker_cycles in samples for duration in worker_cycles]
    operations = workers * iterations
    return {
        "operations": operations,
        "elapsed_s": elapsed,
        "ops_per_sec": operations / elapsed,
        "acquire_wait": distribution(acquire_waits),
        "worker_cycle": distribution(cycle_times),
    }


def classify_lock_bottleneck(configurations: list[dict[str, object]], rtt_ms: float) -> str:
    if rtt_ms <= 0:
        return "not_assessed_without_injected_rtt"
    high_contention = [
        item
        for item in configurations
        if int(item["workers"]) >= max(int(size) for size in item["maxsizes_compared"])
    ]
    if not high_contention:
        return "insufficient_high_contention_samples"
    all_within_factor = all(
        0.5 <= float(item["maxsize_ratio_to_theory"]) <= 2 for item in high_contention
    )
    no_maxsize_gain = all(bool(item["maxsize_gain_within_10_percent"]) for item in high_contention)
    if all_within_factor and no_maxsize_gain:
        return "established"
    return "not_established"


def format_distribution(name: str, stats: dict[str, object]) -> list[str]:
    return [
        "  {name}: p50={p50_ms:.3f}ms p90={p90_ms:.3f}ms p99={p99_ms:.3f}ms "
        "max={max_ms:.3f}ms count={count}".format(name=name, **stats),
        "  {name}_raw_ms={raw_ms}".format(name=name, **stats),
    ]


def emit(result: dict[str, object], as_json: bool) -> None:
    if as_json:
        print(json.dumps(result, sort_keys=True))  # noqa: T201
        return
    environment = result["environment"]
    print("WP01 Pool contention benchmark")  # noqa: T201
    print(  # noqa: T201
        "environment: ClickHouse={version} revision={revision}; Python={python}; "
        "injected_rtt_ms={injected_rtt_ms:g}; workers={workers}; maxsizes={maxsizes}; "
        "iterations={iterations}; rounds={rounds} (+1 warmup)".format(**environment)
    )
    print("criterion: maxsize gain <=10% and measured throughput within [0.5x, 2x] of 1/(2*RTT)")  # noqa: T201
    for configuration in result["configurations"]:
        print(  # noqa: T201
            "workers={workers} maxsize={maxsize} ops_per_sec raw={raw_ops_per_sec} "
            "p50={ops_p50:.3f} p90={ops_p90:.3f} p99={ops_p99:.3f} max={ops_max:.3f} "
            "theory={theoretical_ops_per_sec} ratio={ratio_to_theory}".format(**configuration)
        )
        for round_result in configuration["rounds"]:
            print(  # noqa: T201
                " round={round} elapsed_s={elapsed_s:.6f} ops_per_sec={ops_per_sec:.3f}".format(
                    **round_result
                )
            )
            for line in format_distribution("acquire_wait", round_result["acquire_wait"]):
                print(line)  # noqa: T201
            for line in format_distribution("worker_cycle", round_result["worker_cycle"]):
                print(line)  # noqa: T201
    print(f"verdict: lock_inside_rtt_bottleneck={result['verdict']}")  # noqa: T201


async def run(args: argparse.Namespace) -> dict[str, object]:
    snapshot = await server_snapshot(args.dsn)
    configurations: list[dict[str, object]] = []
    theoretical = None if args.rtt_ms == 0 else 1000 / (2 * args.rtt_ms)
    for workers in args.workers:
        per_maxsize: list[dict[str, object]] = []
        for maxsize in args.maxsizes:
            await one_round(args.dsn, workers, maxsize, args.iterations)
            rounds = [
                await one_round(args.dsn, workers, maxsize, args.iterations)
                for _ in range(args.rounds)
            ]
            ops = [float(round_result["ops_per_sec"]) for round_result in rounds]
            per_maxsize.append(
                {
                    "workers": workers,
                    "maxsize": maxsize,
                    "rounds": [
                        dict(round_result, round=index + 1)
                        for index, round_result in enumerate(rounds)
                    ],
                    "raw_ops_per_sec": ops,
                    "ops_p50": percentile(ops, 50),
                    "ops_p90": percentile(ops, 90),
                    "ops_p99": percentile(ops, 99),
                    "ops_max": max(ops),
                    "theoretical_ops_per_sec": theoretical,
                    "ratio_to_theory": None
                    if theoretical is None
                    else percentile(ops, 50) / theoretical,
                }
            )
        by_size = {int(item["maxsize"]): item for item in per_maxsize}
        smallest, largest = min(args.maxsizes), max(args.maxsizes)
        gain = float(by_size[largest]["ops_p50"]) / float(by_size[smallest]["ops_p50"])
        for item in per_maxsize:
            item["maxsizes_compared"] = args.maxsizes
            item["maxsize_gain_within_10_percent"] = gain <= 1.10
            item["maxsize_ratio_to_theory"] = item["ratio_to_theory"]
            item["maxsize_p50_gain"] = gain
            configurations.append(item)
    return {
        "benchmark": "pool_contention",
        "environment": {
            **snapshot,
            "python": sys.version.split()[0],
            "injected_rtt_ms": args.rtt_ms,
            "workers": args.workers,
            "maxsizes": args.maxsizes,
            "iterations": args.iterations,
            "rounds": args.rounds,
        },
        "configurations": configurations,
        "verdict": classify_lock_bottleneck(configurations, args.rtt_ms),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default=default_dsn())
    parser.add_argument("--workers", type=csv_ints, default=[1, 8, 32, 128])
    parser.add_argument("--maxsizes", type=csv_ints, default=[8, 32])
    parser.add_argument(
        "--iterations", type=int, default=5, help="SELECT 1 loops per worker per round"
    )
    parser.add_argument("--rounds", type=int, default=5, help="measured rounds after one warmup")
    parser.add_argument(
        "--rtt-ms",
        type=float,
        required=True,
        help="configured round-trip latency for this proxy run; use 2 * proxy --delay-ms",
    )
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args()
    if args.rounds < 5:
        parser.error("--rounds must be at least 5")
    if args.iterations < 1:
        parser.error("--iterations must be positive")
    if args.rtt_ms < 0:
        parser.error("--rtt-ms must be non-negative")
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
