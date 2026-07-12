"""Measure single-query decoding throughput across three real driver modes."""

from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import sys
from collections.abc import Iterable
from pathlib import Path
from time import perf_counter

from asynch.connection import Connection
from benchmarks.bench_loop_lag import (
    cleanup_tables,
    csv_strings,
    default_dsn,
    percentile,
    server_snapshot,
    update_compression,
    validate_identifier,
)

PURE_DRIVER_PROBE = r"""
import importlib.machinery
import json
import clickhouse_driver.bufferedreader as bufferedreader
import clickhouse_driver.bufferedwriter as bufferedwriter
import clickhouse_driver.columns.largeint as largeint
import clickhouse_driver.varint as varint

modules = (bufferedreader, bufferedwriter, largeint, varint)
suffixes = importlib.machinery.EXTENSION_SUFFIXES
extensions = [module.__file__ for module in modules if module.__file__.endswith(tuple(suffixes))]
if extensions:
    raise SystemExit("not a pure-Python clickhouse-driver: " + ", ".join(extensions))
print(json.dumps({"pure_python": True}))
"""

PURE_DRIVER_QUERY = r"""
import importlib.machinery
import json
import sys
from time import perf_counter

from clickhouse_driver import Client
import clickhouse_driver.bufferedreader as bufferedreader
import clickhouse_driver.bufferedwriter as bufferedwriter
import clickhouse_driver.columns.largeint as largeint
import clickhouse_driver.varint as varint

suffixes = importlib.machinery.EXTENSION_SUFFIXES
modules = (bufferedreader, bufferedwriter, largeint, varint)
extensions = [module.__file__ for module in modules if module.__file__.endswith(tuple(suffixes))]
if extensions:
    raise SystemExit("not a pure-Python clickhouse-driver: " + ", ".join(extensions))

dsn, query = sys.argv[1:]
client = Client.from_url(dsn)
try:
    client.execute("SELECT 1")
    started = perf_counter()
    rows = client.execute(query)
    print(json.dumps({"elapsed_s": perf_counter() - started, "rows": len(rows)}))
finally:
    client.disconnect()
"""


def distribution(values: Iterable[float]) -> dict[str, object]:
    samples = list(values)
    return {
        "count": len(samples),
        "p50": percentile(samples, 50),
        "p90": percentile(samples, 90),
        "p99": percentile(samples, 99),
        "max": max(samples),
        "raw": samples,
    }


async def execute(connection: Connection, query: str) -> None:
    async with connection.cursor() as cursor:
        await cursor.execute(query)


async def prepare_tables(args: argparse.Namespace) -> dict[str, str]:
    tables = {
        "int64": f"{args.table_prefix}_throughput_int64",
        "string": f"{args.table_prefix}_throughput_string",
        "nullable": f"{args.table_prefix}_throughput_nullable",
        "lowcardinality": f"{args.table_prefix}_throughput_lowcardinality",
    }
    async with Connection(dsn=args.dsn) as connection:
        for table in tables.values():
            await execute(connection, f"DROP TABLE IF EXISTS {table}")
        await execute(
            connection,
            f"""
            CREATE TABLE {tables["int64"]}
            (c0 Int64, c1 Int64, c2 Int64, c3 Int64, c4 Int64, c5 Int64, c6 Int64, c7 Int64)
            ENGINE = MergeTree ORDER BY tuple()
            """,
        )
        int_insert = f"""
            INSERT INTO {tables["int64"]}
            SELECT
                toInt64(number), toInt64(number + 1), toInt64(number + 2), toInt64(number + 3),
                toInt64(number + 4), toInt64(number + 5), toInt64(number + 6), toInt64(number + 7)
            FROM numbers({args.rows})
            """  # noqa: S608
        await execute(connection, int_insert)
        payload = "concat(toString(number + {seed}), repeat('x', 32 - length(toString(number + {seed}))))".format(
            seed=args.seed
        )
        await execute(
            connection,
            f"""
            CREATE TABLE {tables["string"]}
            (s0 String, s1 String, s2 String, s3 String)
            ENGINE = MergeTree ORDER BY tuple()
            """,
        )
        string_insert = f"""
            INSERT INTO {tables["string"]} SELECT {payload}, {payload}, {payload}, {payload}
            FROM numbers({args.rows})
            """  # noqa: S608
        await execute(connection, string_insert)
        await execute(
            connection,
            f"""
            CREATE TABLE {tables["nullable"]}
            (n0 Nullable(Int64), n1 Nullable(String), n2 Nullable(Int64), n3 Nullable(String))
            ENGINE = MergeTree ORDER BY tuple()
            """,
        )
        nullable_insert = f"""
            INSERT INTO {tables["nullable"]}
            SELECT
                if(number % 5 = 0, NULL, toInt64(number)),
                if(number % 5 = 0, NULL, {payload}),
                if(number % 7 = 0, NULL, toInt64(number + 1)),
                if(number % 7 = 0, NULL, {payload})
            FROM numbers({args.rows})
            """  # noqa: S608
        await execute(connection, nullable_insert)
        await execute(
            connection,
            f"""
            CREATE TABLE {tables["lowcardinality"]}
            (k0 LowCardinality(String), k1 LowCardinality(String), k2 LowCardinality(String))
            ENGINE = MergeTree ORDER BY tuple()
            """,
        )
        lowcardinality_insert = f"""
            INSERT INTO {tables["lowcardinality"]}
            SELECT
                concat('group-', toString(number % 16)),
                concat('bucket-', toString(number % 64)),
                concat('kind-', toString(number % 4))
            FROM numbers({args.rows})
            """  # noqa: S608
        await execute(connection, lowcardinality_insert)
    return tables


async def asynch_query(dsn: str, query: str) -> tuple[int, float]:
    async with Connection(dsn=dsn) as connection:
        await connection._connection.execute("SELECT 1")
        started = perf_counter()
        rows = await connection._connection.execute(query)
        return len(rows), perf_counter() - started


def cython_query(dsn: str, query: str) -> tuple[int, float]:
    from clickhouse_driver import Client

    client = Client.from_url(dsn)
    try:
        client.execute("SELECT 1")
        started = perf_counter()
        rows = client.execute(query)
        return len(rows), perf_counter() - started
    finally:
        client.disconnect()


def run_pure_python(executable: Path, dsn: str, query: str) -> tuple[int, float]:
    completed = subprocess.run(  # noqa: S603 -- explicit interpreter is verified before use
        [str(executable), "-c", PURE_DRIVER_QUERY, dsn, query],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(completed.stdout)
    return int(payload["rows"]), float(payload["elapsed_s"])


def check_pure_python(executable: Path) -> None:
    if not executable.is_file():
        raise ValueError(f"--pure-python-python does not point to a file: {executable}")
    completed = subprocess.run(  # noqa: S603 -- probe the explicit interpreter
        [str(executable), "-c", PURE_DRIVER_PROBE],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode:
        message = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(
            "pure-Python comparison is unavailable: provide an interpreter with a real "
            f"pure-Python clickhouse-driver fallback ({message})"
        )


async def run(args: argparse.Namespace) -> dict[str, object]:
    check_pure_python(args.pure_python_python)
    snapshot = await server_snapshot(args.dsn)
    tables = await prepare_tables(args)
    results: list[dict[str, object]] = []
    drivers = ("asynch", "clickhouse_driver_cython", "clickhouse_driver_pure_python")
    try:
        for compression in args.compression:
            dsn = update_compression(args.dsn, compression)
            for shape, table in tables.items():
                query = f"SELECT * FROM {table}"  # noqa: S608
                for driver in drivers:
                    if driver == "asynch":
                        await asynch_query(dsn, query)
                    elif driver == "clickhouse_driver_cython":
                        await asyncio.to_thread(cython_query, dsn, query)
                    else:
                        await asyncio.to_thread(
                            run_pure_python, args.pure_python_python, dsn, query
                        )
                    rounds = []
                    for round_number in range(1, args.rounds + 1):
                        if driver == "asynch":
                            rows, elapsed = await asynch_query(dsn, query)
                        elif driver == "clickhouse_driver_cython":
                            rows, elapsed = await asyncio.to_thread(cython_query, dsn, query)
                        else:
                            rows, elapsed = await asyncio.to_thread(
                                run_pure_python, args.pure_python_python, dsn, query
                            )
                        if rows != args.rows:
                            raise RuntimeError(
                                f"{driver} {shape} returned {rows} rows, expected {args.rows}"
                            )
                        rounds.append(
                            {
                                "round": round_number,
                                "rows": rows,
                                "elapsed_s": elapsed,
                                "rows_per_sec": rows / elapsed,
                            }
                        )
                    throughputs = [round_result["rows_per_sec"] for round_result in rounds]
                    elapsed = [round_result["elapsed_s"] for round_result in rounds]
                    results.append(
                        {
                            "driver": driver,
                            "shape": shape,
                            "compression": compression,
                            "rounds": rounds,
                            "throughput_rows_per_sec": distribution(throughputs),
                            "elapsed_s": distribution(elapsed),
                        }
                    )
    finally:
        if not args.keep_tables:
            await cleanup_tables(args.dsn, tuple(tables.values()))
    return {
        "benchmark": "single_query_throughput",
        "environment": {
            **snapshot,
            "python": sys.version.split()[0],
            "rows": args.rows,
            "seed": args.seed,
            "compression": args.compression,
            "rounds": args.rounds,
            "pure_python_python": str(args.pure_python_python),
        },
        "results": results,
    }


def emit(result: dict[str, object], as_json: bool) -> None:
    if as_json:
        print(json.dumps(result, sort_keys=True))  # noqa: T201
        return
    environment = result["environment"]
    print("WP01 single-query throughput benchmark")  # noqa: T201
    print(  # noqa: T201
        "environment: ClickHouse={version} revision={revision}; Python={python}; rows={rows}; "
        "seed={seed}; compression={compression}; rounds={rounds} (+1 warmup); "
        "pure_python_python={pure_python_python}".format(**environment)
    )
    for item in result["results"]:
        throughput = item["throughput_rows_per_sec"]
        elapsed = item["elapsed_s"]
        print(  # noqa: T201
            "driver={driver} shape={shape} compression={compression} rows/sec "
            "p50={p50:.3f} p90={p90:.3f} p99={p99:.3f} max={max:.3f} raw={raw}; "
            "elapsed_s p50={elapsed_p50:.6f} p90={elapsed_p90:.6f} "
            "p99={elapsed_p99:.6f} max={elapsed_max:.6f}".format(
                **item,
                **throughput,
                elapsed_p50=elapsed["p50"],
                elapsed_p90=elapsed["p90"],
                elapsed_p99=elapsed["p99"],
                elapsed_max=elapsed["max"],
            )
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default=default_dsn())
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--seed", type=int, default=20260712)
    parser.add_argument("--table-prefix", type=validate_identifier, default="wp01_bench")
    parser.add_argument("--compression", type=csv_strings, default=["off", "lz4"])
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--pure-python-python", type=Path, required=True)
    parser.add_argument("--keep-tables", action="store_true")
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args()
    if args.rows < 1:
        parser.error("--rows must be positive")
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
