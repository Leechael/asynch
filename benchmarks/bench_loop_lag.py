"""Measure event-loop lag while decoding ClickHouse result sets."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections.abc import Awaitable, Callable, Iterable
from contextlib import suppress
from time import perf_counter
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from asynch.connection import Connection
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
        raise ValueError("cannot calculate a percentile for no samples")
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


def update_compression(dsn: str, compression: str) -> str:
    parsed = urlsplit(dsn)
    query = [(key, value) for key, value in parse_qsl(parsed.query) if key != "compression"]
    if compression != "off":
        query.append(("compression", compression))
    return urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, urlencode(query), parsed.fragment)
    )


def validate_identifier(value: str) -> str:
    if not value.replace("_", "").isalnum() or value[0].isdigit():
        raise argparse.ArgumentTypeError("table prefix must be an SQL identifier")
    return value


def csv_strings(value: str) -> list[str]:
    values = value.split(",")
    if not values or any(not item for item in values):
        raise argparse.ArgumentTypeError("expected a non-empty comma-separated list")
    return values


def csv_ints(value: str) -> list[int]:
    try:
        values = [int(item) for item in value.split(",")]
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected a comma-separated list of integers") from error
    if not values or any(item < 1 for item in values):
        raise argparse.ArgumentTypeError("all values must be positive")
    return values


async def execute(connection: Connection, query: str) -> None:
    async with connection.cursor() as cursor:
        await cursor.execute(query)


async def server_snapshot(dsn: str) -> dict[str, object]:
    async with Connection(dsn=dsn) as connection:
        async with connection.cursor() as cursor:
            await cursor.execute("SELECT version(), revision()")
            version, revision = await cursor.fetchone()
    return {"version": version, "revision": revision}


async def prepare_tables(args: argparse.Namespace) -> tuple[str, str]:
    int_table = f"{args.table_prefix}_int64"
    string_table = f"{args.table_prefix}_string"
    async with Connection(dsn=args.dsn) as connection:
        for table in (int_table, string_table):
            await execute(connection, f"DROP TABLE IF EXISTS {table}")
        await execute(  # noqa: S608
            connection,
            f"""
            CREATE TABLE {int_table}
            (
                c0 Int64, c1 Int64, c2 Int64, c3 Int64,
                c4 Int64, c5 Int64, c6 Int64, c7 Int64
            ) ENGINE = MergeTree ORDER BY tuple()
            """,
        )
        int_insert_query = f"""
            INSERT INTO {int_table}
            SELECT
                toInt64(number), toInt64(number + 1), toInt64(number + 2), toInt64(number + 3),
                toInt64(number + 4), toInt64(number + 5), toInt64(number + 6), toInt64(number + 7)
            FROM numbers({args.rows})
            """  # noqa: S608
        await execute(connection, int_insert_query)
        await execute(
            connection,
            f"""
            CREATE TABLE {string_table}
            (s0 String, s1 String, s2 String, s3 String)
            ENGINE = MergeTree ORDER BY tuple()
            """,
        )
        payload = "concat(toString(number + {seed}), repeat('x', 32 - length(toString(number + {seed}))))".format(
            seed=args.seed
        )
        await execute(
            connection,
            f"INSERT INTO {string_table} SELECT {payload}, {payload}, {payload}, {payload} "  # noqa: S608
            f"FROM numbers({args.rows})",
        )
    return int_table, string_table


async def cleanup_tables(dsn: str, tables: Iterable[str]) -> None:
    async with Connection(dsn=dsn) as connection:
        for table in tables:
            await execute(connection, f"DROP TABLE IF EXISTS {table}")


async def heartbeat(stop: asyncio.Event, interval: float, samples: list[float]) -> None:
    loop = asyncio.get_running_loop()
    last = loop.time()
    while not stop.is_set():
        await asyncio.sleep(interval)
        now = loop.time()
        samples.append(now - last - interval)
        last = now


async def measure_with_heartbeat(
    operation: Callable[[], Awaitable[int]], interval: float
) -> tuple[float, list[float], int]:
    stop = asyncio.Event()
    lags: list[float] = []
    task = asyncio.create_task(heartbeat(stop, interval, lags))
    await asyncio.sleep(interval * 2)
    lags.clear()
    started = perf_counter()
    try:
        rows = await operation()
    finally:
        elapsed = perf_counter() - started
        stop.set()
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
    if not lags:
        raise RuntimeError("heartbeat collected no samples")
    return elapsed, lags, rows


async def idle_baseline(seconds: float, interval: float) -> list[float]:
    stop = asyncio.Event()
    lags: list[float] = []
    task = asyncio.create_task(heartbeat(stop, interval, lags))
    await asyncio.sleep(seconds)
    stop.set()
    await task
    if not lags:
        raise RuntimeError("idle baseline collected no samples")
    return lags


async def asynch_read(dsn: str, table: str, streaming: bool, max_block_size: int) -> int:
    async with Connection(dsn=dsn) as connection:
        async with connection.cursor() as cursor:
            if streaming:
                cursor.set_stream_results(True, max_block_size)
            await cursor.execute(f"SELECT * FROM {table}")  # noqa: S608
            if streaming:
                count = 0
                async for _row in cursor:
                    count += 1
                return count
            return len(await cursor.fetchall())


def sync_read(dsn: str, table: str, max_block_size: int) -> int:
    from clickhouse_driver import Client

    client = Client.from_url(dsn)
    try:
        query = f"SELECT * FROM {table}"  # noqa: S608
        rows = client.execute(query, settings={"max_block_size": max_block_size})
        return len(rows)
    finally:
        client.disconnect()


def scenario_specs() -> dict[str, tuple[str, str, bool]]:
    return {
        "asynch-stream-int64": ("asynch", "int64", True),
        "asynch-stream-string": ("asynch", "string", True),
        "asynch-fetchall-int64": ("asynch", "int64", False),
        "asynch-fetchall-string": ("asynch", "string", False),
        "sync-executor-int64": ("sync", "int64", False),
        "sync-executor-string": ("sync", "string", False),
    }


def classify_loop_blocking(baseline: dict[str, object], foreground: dict[str, object]) -> str:
    baseline_p99 = float(baseline["p99_ms"])
    foreground_p99 = float(foreground["p99_ms"])
    if foreground_p99 >= 10 * baseline_p99 and foreground_p99 >= 10:
        return "established"
    return "not_established"


async def run(args: argparse.Namespace) -> dict[str, object]:
    specs = scenario_specs()
    unknown = set(args.scenarios) - set(specs)
    if unknown:
        raise ValueError(f"unknown scenarios: {', '.join(sorted(unknown))}")
    snapshot = await server_snapshot(args.dsn)
    tables = (
        f"{args.table_prefix}_int64",
        f"{args.table_prefix}_string",
    )
    results: list[dict[str, object]] = []
    try:
        await prepare_tables(args)
        by_shape = {"int64": tables[0], "string": tables[1]}
        baseline_lags = await idle_baseline(args.baseline_seconds, args.heartbeat_ms / 1000)
        baseline = distribution(baseline_lags)
        for compression in args.compression:
            dsn = update_compression(args.dsn, compression)
            for max_block_size in args.max_block_sizes:
                for scenario in args.scenarios:
                    driver, shape, streaming = specs[scenario]
                    table = by_shape[shape]
                    awaitable: Callable[[], Awaitable[int]]
                    if driver == "asynch":

                        def awaitable(
                            dsn: str = dsn,
                            table: str = table,
                            streaming: bool = streaming,
                            max_block_size: int = max_block_size,
                        ) -> Awaitable[int]:
                            return asynch_read(dsn, table, streaming, max_block_size)
                    else:
                        loop = asyncio.get_running_loop()

                        def awaitable(
                            dsn: str = dsn,
                            table: str = table,
                            max_block_size: int = max_block_size,
                        ) -> Awaitable[int]:
                            return loop.run_in_executor(None, sync_read, dsn, table, max_block_size)

                    await measure_with_heartbeat(awaitable, args.heartbeat_ms / 1000)
                    rounds: list[dict[str, object]] = []
                    for round_number in range(1, args.rounds + 1):
                        elapsed, lags, row_count = await measure_with_heartbeat(
                            awaitable, args.heartbeat_ms / 1000
                        )
                        if row_count != args.rows:
                            raise RuntimeError(
                                f"{scenario} returned {row_count} rows, expected {args.rows}"
                            )
                        rounds.append(
                            {
                                "round": round_number,
                                "elapsed_s": elapsed,
                                "rows": row_count,
                                "throughput_rows_per_sec": row_count / elapsed,
                                "lag": distribution(lags),
                            }
                        )
                    combined_lags = [
                        item / 1000
                        for round_result in rounds
                        for item in round_result["lag"]["raw_ms"]
                    ]
                    foreground = distribution(combined_lags)
                    results.append(
                        {
                            "scenario": scenario,
                            "driver": driver,
                            "shape": shape,
                            "streaming": streaming,
                            "compression": compression,
                            "max_block_size": max_block_size,
                            "rounds": rounds,
                            "foreground_lag": foreground,
                            "loop_blocking": classify_loop_blocking(baseline, foreground),
                        }
                    )
    finally:
        if not args.keep_tables:
            await cleanup_tables(args.dsn, tables)
    return {
        "benchmark": "loop_lag",
        "environment": {
            **snapshot,
            "python": sys.version.split()[0],
            "rows": args.rows,
            "seed": args.seed,
            "compression": args.compression,
            "max_block_sizes": args.max_block_sizes,
            "heartbeat_ms": args.heartbeat_ms,
            "rounds": args.rounds,
        },
        "idle_baseline_lag": baseline,
        "results": results,
        "criterion": "p99 >= 10x idle p99 and p99 >= 10ms",
    }


def emit(result: dict[str, object], as_json: bool) -> None:
    if as_json:
        print(json.dumps(result, sort_keys=True))  # noqa: T201
        return
    environment = result["environment"]
    print("WP01 event-loop lag benchmark")  # noqa: T201
    print(  # noqa: T201
        "environment: ClickHouse={version} revision={revision}; Python={python}; rows={rows}; "
        "seed={seed}; compression={compression}; max_block_sizes={max_block_sizes}; "
        "heartbeat_ms={heartbeat_ms:g}; rounds={rounds} (+1 warmup per scenario)".format(
            **environment
        )
    )
    print(f"criterion: {result['criterion']}")  # noqa: T201
    print(f"idle_lag={result['idle_baseline_lag']}")  # noqa: T201
    for item in result["results"]:
        foreground = item["foreground_lag"]
        print(  # noqa: T201
            "scenario={scenario} compression={compression} max_block_size={max_block_size} "
            "lag_p50={p50_ms:.3f}ms lag_p90={p90_ms:.3f}ms lag_p99={p99_ms:.3f}ms "
            "lag_max={max_ms:.3f}ms blocking={loop_blocking}".format(**item, **foreground)
        )
        print(f" foreground_lag_raw_ms={foreground['raw_ms']}")  # noqa: T201
        for round_result in item["rounds"]:
            print(  # noqa: T201
                " round={round} elapsed_s={elapsed_s:.6f} rows={rows} "
                "throughput_rows_per_sec={throughput_rows_per_sec:.3f} lag={lag}".format(
                    **round_result
                )
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default=default_dsn())
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--seed", type=int, default=20260712)
    parser.add_argument("--table-prefix", type=validate_identifier, default="wp01_bench")
    parser.add_argument("--compression", type=csv_strings, default=["off", "lz4"])
    parser.add_argument("--max-block-sizes", type=csv_ints, default=[65409, 8192])
    parser.add_argument("--heartbeat-ms", type=float, default=10.0)
    parser.add_argument("--baseline-seconds", type=float, default=2.0)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--keep-tables", action="store_true")
    parser.add_argument("--json", action="store_true", dest="as_json")
    parser.add_argument("--scenarios", type=csv_strings, default=list(scenario_specs()))
    args = parser.parse_args()
    if args.rows < 1:
        parser.error("--rows must be positive")
    if args.heartbeat_ms <= 0:
        parser.error("--heartbeat-ms must be positive")
    if args.baseline_seconds <= 0:
        parser.error("--baseline-seconds must be positive")
    if args.rounds < 5:
        parser.error("--rounds must be at least 5")
    if any(item not in {"off", "lz4", "zstd"} for item in args.compression):
        parser.error("--compression values must be off, lz4, or zstd")
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
