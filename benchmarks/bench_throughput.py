"""Measure single-query decoding throughput across real driver modes."""

from __future__ import annotations

import argparse
import asyncio
import json
import pickle
import subprocess
import sys
import tempfile
from collections.abc import Iterable
from pathlib import Path
from time import perf_counter
from typing import Optional

from asynch.connection import Connection
from benchmarks.bench_loop_lag import (
    cleanup_tables,
    csv_strings,
    default_dsn,
    execute,
    percentile,
    server_snapshot,
    update_compression,
    validate_identifier,
)

PURE_DRIVER_PROBE = r"""
import importlib.machinery
import json
import subprocess
import sys
from importlib.metadata import distribution
from pathlib import Path
from urllib.parse import unquote, urlparse
import clickhouse_driver.bufferedreader as bufferedreader
import clickhouse_driver.bufferedwriter as bufferedwriter
import clickhouse_driver.columns.largeint as largeint
import clickhouse_driver.varint as varint

modules = (bufferedreader, bufferedwriter, largeint, varint)
suffixes = importlib.machinery.EXTENSION_SUFFIXES
paths = [Path(module.__file__).resolve() for module in modules]
extensions = [str(path) for path in paths if path.name.endswith(tuple(suffixes))]
if extensions:
    raise SystemExit("not a pure-Python clickhouse-driver: " + ", ".join(extensions))
direct_url_text = distribution("clickhouse-driver").read_text("direct_url.json")
if not direct_url_text:
    raise SystemExit("pure-Python comparison must be installed from a local source checkout")
direct_url = json.loads(direct_url_text)
source_url = urlparse(direct_url["url"])
if source_url.scheme != "file":
    raise SystemExit("pure-Python comparison must be installed from a local source checkout")
source_path = Path(unquote(source_url.path)).resolve()
revision = subprocess.run(
    ["git", "-C", str(source_path), "rev-parse", "HEAD"],
    check=True,
    capture_output=True,
    text=True,
).stdout.strip()
expected_revision = sys.argv[1]
if not revision.startswith(expected_revision):
    raise SystemExit(f"pure-Python revision {revision} does not match {expected_revision}")
if any(source_path not in path.parents for path in paths):
    raise SystemExit("pure-Python modules were not imported from the source checkout")
print(json.dumps({
    "pure_python_module_paths": [str(path) for path in paths],
    "pure_python_source_direct_url": direct_url,
    "pure_python_source_revision": revision,
}))
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

PURE_DRIVER_ROWS = r"""
import json
import pickle
import sys

from clickhouse_driver import Client

dsn, query, output_path = sys.argv[1:]
client = Client.from_url(dsn)
try:
    client.execute("SELECT 1")
    rows = client.execute(query)
    with open(output_path, "wb") as output:
        pickle.dump(rows, output, protocol=pickle.HIGHEST_PROTOCOL)
    print(json.dumps({"rows": len(rows)}))
finally:
    client.disconnect()
"""

PURE_DRIVER_REVISION = "49afa09"

SOURCE_DRIVER_PROBE = r"""
import importlib.machinery
import json
import subprocess
import sys
from importlib.metadata import distribution
from pathlib import Path
from urllib.parse import unquote, urlparse

import clickhouse_driver.bufferedreader as bufferedreader
import clickhouse_driver.bufferedwriter as bufferedwriter
import clickhouse_driver.columns.largeint as largeint
import clickhouse_driver.varint as varint

modules = (bufferedreader, bufferedwriter, largeint, varint)
suffixes = importlib.machinery.EXTENSION_SUFFIXES
extensions = [module.__file__ for module in modules if module.__file__.endswith(tuple(suffixes))]
if len(extensions) != 4:
    raise SystemExit("source comparison must expose all four clickhouse-driver extension modules")
direct_url_text = distribution("clickhouse-driver").read_text("direct_url.json")
if not direct_url_text:
    raise SystemExit("source comparison must be installed from a local source checkout")
direct_url = json.loads(direct_url_text)
parsed_url = urlparse(direct_url["url"])
if parsed_url.scheme != "file":
    raise SystemExit("source comparison must be installed from a local source checkout")
source_path = Path(unquote(parsed_url.path))
completed = subprocess.run(
    ["git", "-C", str(source_path), "rev-parse", "HEAD"],
    check=False,
    capture_output=True,
    text=True,
)
if completed.returncode:
    raise SystemExit("source comparison metadata does not point to a Git checkout")
revision = completed.stdout.strip()
expected_revision = sys.argv[1]
if not revision.startswith(expected_revision):
    raise SystemExit(
        f"source comparison revision {revision} does not match expected {expected_revision}"
    )
print(json.dumps({
    "source_c_extensions": extensions,
    "source_direct_url": direct_url,
    "source_revision": revision,
}))
"""

SOURCE_DRIVER_QUERY = r"""
import json
import sys
from time import perf_counter

from clickhouse_driver import Client

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


def table_names(args: argparse.Namespace) -> dict[str, str]:
    return {
        "int64": f"{args.table_prefix}_throughput_int64",
        "string": f"{args.table_prefix}_throughput_string",
        "nullable": f"{args.table_prefix}_throughput_nullable",
        "lowcardinality": f"{args.table_prefix}_throughput_lowcardinality",
    }


async def prepare_tables(args: argparse.Namespace) -> dict[str, str]:
    tables = table_names(args)
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


def cython_rows(dsn: str, query: str) -> list[object]:
    from clickhouse_driver import Client

    client = Client.from_url(dsn)
    try:
        client.execute("SELECT 1")
        return client.execute(query)
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


def pure_python_rows(executable: Path, dsn: str, query: str) -> list[object]:
    with tempfile.NamedTemporaryFile(suffix=".pickle", delete=False) as output:
        output_path = Path(output.name)
    try:
        completed = subprocess.run(  # noqa: S603 -- explicit interpreter is verified before use
            [str(executable), "-c", PURE_DRIVER_ROWS, dsn, query, str(output_path)],
            check=True,
            capture_output=True,
            text=True,
        )
        payload = json.loads(completed.stdout)
        with output_path.open("rb") as output:
            rows = pickle.load(  # noqa: S301 -- data came from the verified local interpreter
                output
            )
        if len(rows) != int(payload["rows"]):
            raise RuntimeError("pure-Python comparison row serialization count mismatch")
        return rows
    finally:
        output_path.unlink(missing_ok=True)


def check_pure_python(executable: Path) -> dict[str, object]:
    if not executable.is_file():
        raise ValueError(f"--pure-python-python does not point to a file: {executable}")
    completed = subprocess.run(  # noqa: S603 -- probe the explicit interpreter
        [str(executable), "-c", PURE_DRIVER_PROBE, PURE_DRIVER_REVISION],
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
    return json.loads(completed.stdout)


def check_source_driver(executable: Path, expected_revision: str) -> dict[str, object]:
    if not executable.is_file():
        raise ValueError(f"--source-driver-python does not point to a file: {executable}")
    completed = subprocess.run(  # noqa: S603 -- probe the explicit interpreter
        [str(executable), "-c", SOURCE_DRIVER_PROBE, expected_revision],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode:
        message = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(f"source clickhouse-driver comparison is unavailable: {message}")
    return json.loads(completed.stdout)


def run_source_driver(executable: Path, dsn: str, query: str) -> tuple[int, float]:
    completed = subprocess.run(  # noqa: S603 -- explicit interpreter is verified before use
        [str(executable), "-c", SOURCE_DRIVER_QUERY, dsn, query],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(completed.stdout)
    return int(payload["rows"]), float(payload["elapsed_s"])


async def verify_pure_python_results(
    args: argparse.Namespace, tables: dict[str, str]
) -> list[dict[str, object]]:
    """Compare every wheel/pure-Python result row before timing either mode."""
    order_by = {
        "int64": "c0",
        "string": "s0",
        "nullable": "n0, n1, n2, n3",
        "lowcardinality": "k0, k1, k2",
    }
    checks = []
    for compression in args.compression:
        dsn = update_compression(args.dsn, compression)
        for shape, table in tables.items():
            # MergeTree's unordered block delivery can differ between two
            # correct connections; sort only this correctness query. Timed
            # queries below deliberately remain the original unsorted form.
            query = f"SELECT * FROM {table} ORDER BY {order_by[shape]}"  # noqa: S608
            wheel_rows = await asyncio.to_thread(cython_rows, dsn, query)
            pure_rows = await asyncio.to_thread(
                pure_python_rows, args.pure_python_python, dsn, query
            )
            if wheel_rows != pure_rows:
                mismatch = next(
                    (
                        (index, wheel_row, pure_row)
                        for index, (wheel_row, pure_row) in enumerate(zip(wheel_rows, pure_rows))
                        if wheel_row != pure_row
                    ),
                    None,
                )
                if mismatch is None:
                    detail = f"row counts differ: wheel={len(wheel_rows)} pure={len(pure_rows)}"
                else:
                    index, wheel_row, pure_row = mismatch
                    detail = (
                        f"first mismatch at row {index}: wheel={wheel_row!r} "
                        f"({type(wheel_row)!r}) pure={pure_row!r} ({type(pure_row)!r})"
                    )
                raise RuntimeError(
                    "pure-Python correctness gate failed: "
                    f"wheel and pure-Python rows differ for {shape} compression={compression}; {detail}"
                )
            checks.append(
                {
                    "shape": shape,
                    "compression": compression,
                    "rows": len(wheel_rows),
                    "passed": True,
                }
            )
    return checks


def retention_ratios(results: list[dict[str, object]]) -> list[dict[str, object]]:
    by_shape = {(item["shape"], item["compression"], item["driver"]): item for item in results}
    ratios = []
    for shape, compression, driver in by_shape:
        if driver != "asynch":
            continue
        key = (shape, compression)
        required = [
            (*key, "asynch"),
            (*key, "clickhouse_driver_cython"),
            (*key, "clickhouse_driver_pure_python"),
        ]
        if not all(item in by_shape for item in required):
            continue
        asynch = by_shape[required[0]]["throughput_rows_per_sec"]["p50"]
        wheel = by_shape[required[1]]["throughput_rows_per_sec"]["p50"]
        pure_python = by_shape[required[2]]["throughput_rows_per_sec"]["p50"]
        decython = pure_python / wheel
        async_retention = asynch / pure_python
        direct = asynch / wheel
        identity = decython * async_retention
        ratios.append(
            {
                "shape": shape,
                "compression": compression,
                "r_decython": decython,
                "r_async": async_retention,
                "asynch_over_wheel": direct,
                "identity_product": identity,
                "identity_absolute_delta": abs(identity - direct),
            }
        )
    return sorted(ratios, key=lambda item: (item["shape"], item["compression"]))


async def run(args: argparse.Namespace) -> dict[str, object]:
    pure_python_provenance = None
    if args.pure_python_python:
        pure_python_provenance = check_pure_python(args.pure_python_python)
    source_provenance = None
    if args.source_driver_python:
        source_provenance = check_source_driver(
            args.source_driver_python, args.source_driver_revision
        )
    snapshot = await server_snapshot(args.dsn)
    tables = table_names(args)
    results: list[dict[str, object]] = []
    correctness_gate: list[dict[str, object]] = []
    drivers = ["asynch", "clickhouse_driver_cython"]
    if args.source_driver_python:
        drivers.append("clickhouse_driver_source_c_extension")
    if args.pure_python_python:
        drivers.append("clickhouse_driver_pure_python")
    try:
        await prepare_tables(args)
        if args.pure_python_python:
            correctness_gate = await verify_pure_python_results(args, tables)
        for compression in args.compression:
            dsn = update_compression(args.dsn, compression)
            for shape, table in tables.items():
                query = f"SELECT * FROM {table}"  # noqa: S608
                for driver in drivers:
                    if driver == "asynch":
                        await asynch_query(dsn, query)
                    elif driver == "clickhouse_driver_cython":
                        await asyncio.to_thread(cython_query, dsn, query)
                    elif driver == "clickhouse_driver_source_c_extension":
                        await asyncio.to_thread(
                            run_source_driver, args.source_driver_python, dsn, query
                        )
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
                        elif driver == "clickhouse_driver_source_c_extension":
                            rows, elapsed = await asyncio.to_thread(
                                run_source_driver, args.source_driver_python, dsn, query
                            )
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
    ratio_table = retention_ratios(results)
    return {
        "benchmark": "single_query_throughput",
        "environment": {
            **snapshot,
            "python": sys.version.split()[0],
            "rows": args.rows,
            "seed": args.seed,
            "compression": args.compression,
            "rounds": args.rounds,
            "pure_python_python": str(args.pure_python_python) if args.pure_python_python else None,
            "pure_python_provenance": pure_python_provenance,
            "source_driver_python": str(args.source_driver_python)
            if args.source_driver_python
            else None,
            "source_driver_provenance": source_provenance,
        },
        "correctness_gate": correctness_gate,
        "results": results,
        "retention_ratios": ratio_table,
    }


def text_report(result: dict[str, object]) -> str:
    lines = []
    environment = result["environment"]
    lines.append("Single-query throughput benchmark")
    lines.append(
        "environment: ClickHouse={version} revision={revision}; Python={python}; rows={rows}; "
        "seed={seed}; compression={compression}; rounds={rounds} (+1 warmup); "
        "source_driver_python={source_driver_python}; "
        "pure_python_python={pure_python_python}".format(**environment)
    )
    if result["correctness_gate"]:
        lines.append(
            "pure-Python correctness gate: "
            + ", ".join(
                "{shape}/{compression} rows={rows} passed={passed}".format(**item)
                for item in result["correctness_gate"]
            )
        )
    )
    for item in result["results"]:
        throughput = item["throughput_rows_per_sec"]
        elapsed = item["elapsed_s"]
        lines.append(
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
    for ratio in result["retention_ratios"]:
        lines.append(
            "shape={shape} compression={compression} R_decython={r_decython:.6f} "
            "R_async={r_async:.6f} asynch/wheel={asynch_over_wheel:.6f} "
            "identity_delta={identity_absolute_delta:.12f}".format(**ratio)
        )
    return "\n".join(lines) + "\n"


def emit(result: dict[str, object], as_json: bool, text_output: Optional[Path]) -> None:
    report = text_report(result)
    if text_output:
        text_output.write_text(report, encoding="utf-8")
    if as_json:
        print(json.dumps(result, sort_keys=True))  # noqa: T201
    else:
        print(report, end="")  # noqa: T201


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default=default_dsn())
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--seed", type=int, default=20260712)
    parser.add_argument("--table-prefix", type=validate_identifier, default="wp01_bench")
    parser.add_argument("--compression", type=csv_strings, default=["off", "lz4"])
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--pure-python-python", type=Path)
    parser.add_argument("--source-driver-python", type=Path)
    parser.add_argument("--source-driver-revision")
    parser.add_argument("--keep-tables", action="store_true")
    parser.add_argument("--json", action="store_true", dest="as_json")
    parser.add_argument("--text-output", type=Path)
    args = parser.parse_args()
    if not args.pure_python_python and not args.source_driver_python:
        parser.error("one of --pure-python-python or --source-driver-python is required")
    if args.source_driver_python and not args.source_driver_revision:
        parser.error("--source-driver-revision is required with --source-driver-python")
    if args.source_driver_revision and not args.source_driver_python:
        parser.error("--source-driver-revision requires --source-driver-python")
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
        emit(asyncio.run(run(args)), args.as_json, args.text_output)
    except Exception as error:
        print(f"benchmark error: {error}", file=sys.stderr)  # noqa: T201
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
