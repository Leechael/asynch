import argparse
import asyncio
import gc
import os
import subprocess
import sys
import tracemalloc
from dataclasses import dataclass
from time import perf_counter

from asynch.connection import Connection
from asynch.proto import constants


DEFAULT_DSN = (
    f"clickhouse://{os.environ.get('CLICKHOUSE_USER', constants.DEFAULT_USER)}:"
    f"{os.environ.get('CLICKHOUSE_PASSWORD', constants.DEFAULT_PASSWORD)}"
    f"@{os.environ.get('CLICKHOUSE_HOST', constants.DEFAULT_HOST)}:"
    f"{os.environ.get('CLICKHOUSE_PORT', constants.DEFAULT_PORT)}"
    f"/{os.environ.get('CLICKHOUSE_DB', constants.DEFAULT_DATABASE)}"
)


@dataclass
class Sample:
    cycle: int
    rss_mib: float
    py_current_mib: float
    py_peak_mib: float
    elapsed_s: float


def current_rss_mib() -> float:
    output = subprocess.check_output(
        ["ps", "-o", "rss=", "-p", str(os.getpid())],
        text=True,
    )
    return int(output.strip()) / 1024


def sample(cycle: int, started_at: float) -> Sample:
    gc.collect()
    current, peak = tracemalloc.get_traced_memory()
    return Sample(
        cycle=cycle,
        rss_mib=current_rss_mib(),
        py_current_mib=current / 1024 / 1024,
        py_peak_mib=peak / 1024 / 1024,
        elapsed_s=perf_counter() - started_at,
    )


async def execute(conn: Connection, query: str, args=None):
    return await conn._connection.execute(query, args=args)


async def prepare(conn: Connection, table: str):
    database, _, _ = table.partition(".")
    if database and database != table:
        await execute(conn, f"CREATE DATABASE IF NOT EXISTS {database}")

    await execute(conn, f"DROP TABLE IF EXISTS {table}")
    await execute(
        conn,
        f"""
        CREATE TABLE {table}
        (
            id UInt64,
            payload String
        )
        ENGINE = Null
        """,
    )


async def run_insert(conn: Connection, table: str, cycle: int, batch_size: int):
    data = [(cycle * batch_size + i, f"payload-{i % 1024}") for i in range(batch_size)]
    await execute(conn, f"INSERT INTO {table} (id, payload) VALUES", data)


async def run_buffered_select(conn: Connection, rows: int):
    result = await execute(
        conn,
        f"SELECT number, toString(number) FROM numbers({rows})",
    )
    if len(result) != rows:
        raise RuntimeError(f"Buffered SELECT returned {len(result)} rows, expected {rows}")


async def run_streaming_select(conn: Connection, rows: int):
    result = await conn._connection.execute_iter(
        f"SELECT number, toString(number) FROM numbers({rows})"
    )
    count = 0
    async for _row in result:
        count += 1

    if count != rows:
        raise RuntimeError(f"Streaming SELECT returned {count} rows, expected {rows}")


def print_sample(row: Sample, baseline: Sample | None):
    rss_delta = 0.0 if baseline is None else row.rss_mib - baseline.rss_mib
    py_delta = 0.0 if baseline is None else row.py_current_mib - baseline.py_current_mib
    print(
        f"{row.cycle:>5} "
        f"{row.elapsed_s:>9.2f}s "
        f"rss={row.rss_mib:>9.2f} MiB "
        f"rss_delta={rss_delta:>8.2f} MiB "
        f"py_current={row.py_current_mib:>8.2f} MiB "
        f"py_delta={py_delta:>8.2f} MiB "
        f"py_peak={row.py_peak_mib:>8.2f} MiB",
        flush=True,
    )


async def run(args):
    tracemalloc.start()
    started_at = perf_counter()
    samples: list[Sample] = []

    async with Connection(dsn=args.dsn) as conn:
        await prepare(conn, args.table)
        initial = sample(0, started_at)
        print("cycle elapsed    rss/current deltas are measured after gc.collect()")
        print_sample(initial, None)

        baseline = None
        for cycle in range(1, args.cycles + 1):
            await run_insert(conn, args.table, cycle, args.batch_size)
            await run_buffered_select(conn, args.select_rows)
            await run_streaming_select(conn, args.stream_rows)

            row = sample(cycle, started_at)
            samples.append(row)
            if cycle == args.warmup_cycles:
                baseline = row
            print_sample(row, baseline)

            if args.sleep:
                await asyncio.sleep(args.sleep)

        if not args.keep_table:
            await execute(conn, f"DROP TABLE IF EXISTS {args.table}")

    if not samples:
        return

    baseline = baseline or samples[0]
    final = samples[-1]
    rss_growth = final.rss_mib - baseline.rss_mib
    py_growth = final.py_current_mib - baseline.py_current_mib
    print()
    print(f"baseline_cycle={baseline.cycle}")
    print(f"final_cycle={final.cycle}")
    print(f"rss_growth_after_warmup={rss_growth:.2f} MiB")
    print(f"python_heap_growth_after_warmup={py_growth:.2f} MiB")
    print(
        "Interpretation: a small plateau is normal; repeated positive growth "
        "across longer runs is the signal to investigate."
    )

    if args.fail_on_rss_growth_mib is not None and rss_growth > args.fail_on_rss_growth_mib:
        raise SystemExit(
            "RSS growth exceeded threshold: "
            f"{rss_growth:.2f} MiB > {args.fail_on_rss_growth_mib:.2f} MiB"
        )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Manual asynch memory-growth smoke test. Not intended for regular CI."
    )
    parser.add_argument("--dsn", default=os.environ.get("CLICKHOUSE_DSN", DEFAULT_DSN))
    parser.add_argument("--table", default="test.asynch_memory_watch")
    parser.add_argument("--cycles", type=int, default=50)
    parser.add_argument("--warmup-cycles", type=int, default=5)
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--select-rows", type=int, default=10000)
    parser.add_argument("--stream-rows", type=int, default=10000)
    parser.add_argument("--sleep", type=float, default=0.0)
    parser.add_argument("--keep-table", action="store_true")
    parser.add_argument("--fail-on-rss-growth-mib", type=float)
    return parser.parse_args()


if __name__ == "__main__":
    try:
        asyncio.run(run(parse_args()))
    except KeyboardInterrupt:
        sys.exit(130)
