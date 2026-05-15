import argparse
import asyncio
import gc
import os
import random
import subprocess
import sys
import tracemalloc
from collections import Counter
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
    operation: int
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


def sample(operation: int, started_at: float) -> Sample:
    gc.collect()
    current, peak = tracemalloc.get_traced_memory()
    return Sample(
        operation=operation,
        rss_mib=current_rss_mib(),
        py_current_mib=current / 1024 / 1024,
        py_peak_mib=peak / 1024 / 1024,
        elapsed_s=perf_counter() - started_at,
    )


def print_sample(row: Sample, baseline: Sample | None, outcomes: Counter):
    rss_delta = 0.0 if baseline is None else row.rss_mib - baseline.rss_mib
    py_delta = 0.0 if baseline is None else row.py_current_mib - baseline.py_current_mib
    top_outcomes = ", ".join(f"{name}={count}" for name, count in outcomes.most_common(6))
    print(
        f"{row.operation:>5} "
        f"{row.elapsed_s:>9.2f}s "
        f"rss={row.rss_mib:>9.2f} MiB "
        f"rss_delta={rss_delta:>8.2f} MiB "
        f"py_current={row.py_current_mib:>8.2f} MiB "
        f"py_delta={py_delta:>8.2f} MiB "
        f"py_peak={row.py_peak_mib:>8.2f} MiB "
        f"outcomes=[{top_outcomes}]",
        flush=True,
    )


async def execute(conn: Connection, query: str, args=None):
    return await conn._connection.execute(query, args=args)


async def prepare(dsn: str, table: str):
    async with Connection(dsn=dsn) as conn:
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


async def cleanup(dsn: str, table: str):
    async with Connection(dsn=dsn) as conn:
        await execute(conn, f"DROP TABLE IF EXISTS {table}")


async def normal_select(dsn: str, args) -> str:
    async with Connection(dsn=dsn) as conn:
        rows = await execute(
            conn,
            f"SELECT number, toString(number) FROM numbers({args.select_rows})",
        )
        if len(rows) != args.select_rows:
            return "normal_select_bad_count"
    return "normal_select_ok"


async def normal_insert(dsn: str, args) -> str:
    data = [(i, f"payload-{i % 1024}") for i in range(args.batch_size)]
    async with Connection(dsn=dsn) as conn:
        await execute(conn, f"INSERT INTO {args.table} (id, payload) VALUES", data)
    return "normal_insert_ok"


async def full_stream(dsn: str, args) -> str:
    async with Connection(dsn=dsn) as conn:
        result = await conn._connection.execute_iter(
            f"SELECT number, toString(number) FROM numbers({args.stream_rows})"
        )
        count = 0
        async for _row in result:
            count += 1
        if count != args.stream_rows:
            return "full_stream_bad_count"
    return "full_stream_ok"


async def server_exception(dsn: str, _args) -> str:
    async with Connection(dsn=dsn) as conn:
        try:
            await execute(conn, "SELECT definitely_missing_column FROM system.one")
        except Exception:
            return "server_exception_expected"
    return "server_exception_missing"


async def malformed_insert(dsn: str, args) -> str:
    async with Connection(dsn=dsn) as conn:
        try:
            await execute(
                conn,
                f"INSERT INTO {args.table} (definitely_missing_column) VALUES",
                [(1,)],
            )
        except Exception:
            return "malformed_insert_expected"
    return "malformed_insert_missing"


async def abandon_stream(dsn: str, args) -> str:
    conn = Connection(dsn=dsn)
    await conn.connect()
    try:
        result = await conn._connection.execute_iter(
            f"SELECT number, sleepEachRow({args.sleep_each_row}) FROM numbers({args.chaos_rows})"
        )
        consumed = 0
        try:
            async for _row in result:
                consumed += 1
                if consumed >= args.early_rows:
                    break
        except Exception:
            await conn._connection.disconnect()
            return "abandon_stream_exception"
        await conn._connection.disconnect()
        return "abandon_stream_disconnect"
    finally:
        await conn.close()


async def cancel_query_task(dsn: str, args) -> str:
    conn = Connection(dsn=dsn)
    await conn.connect()
    task = asyncio.create_task(
        conn._connection.execute(
            f"SELECT number, sleepEachRow({args.sleep_each_row}) FROM numbers({args.chaos_rows})"
        )
    )
    try:
        await asyncio.sleep(args.cancel_after)
        if task.done():
            try:
                await task
            except Exception:
                return "cancel_query_exception_before_cancel"
            else:
                return "cancel_query_finished_before_cancel"

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            return "cancel_query_cancelled"
        except Exception:
            return "cancel_query_exception_after_cancel"
        return "cancel_query_no_exception"
    finally:
        await conn._connection.disconnect()
        await conn.close()


async def disconnect_during_query(dsn: str, args) -> str:
    conn = Connection(dsn=dsn)
    await conn.connect()
    task = asyncio.create_task(
        conn._connection.execute(
            f"SELECT number, sleepEachRow({args.sleep_each_row}) FROM numbers({args.chaos_rows})"
        )
    )
    try:
        await asyncio.sleep(args.cancel_after)
        await conn._connection.disconnect()
        try:
            await task
        except Exception:
            return "disconnect_during_query_exception"
        return "disconnect_during_query_finished"
    finally:
        if not task.done():
            task.cancel()
            try:
                await task
            except BaseException:
                pass
        await conn.close()


OPERATIONS = {
    "normal_select": normal_select,
    "normal_insert": normal_insert,
    "full_stream": full_stream,
    "server_exception": server_exception,
    "malformed_insert": malformed_insert,
    "abandon_stream": abandon_stream,
    "cancel_query_task": cancel_query_task,
    "disconnect_during_query": disconnect_during_query,
}


async def run(args):
    rng = random.Random(args.seed)
    operation_names = list(OPERATIONS)
    weights = [24, 24, 18, 8, 8, 8, 5, 5]
    outcomes: Counter = Counter()
    samples: list[Sample] = []

    tracemalloc.start()
    started_at = perf_counter()
    await prepare(args.dsn, args.table)

    try:
        initial = sample(0, started_at)
        print("operation elapsed    RSS/Python heap sampled after gc.collect()")
        print_sample(initial, None, outcomes)

        baseline = None
        for operation in range(1, args.operations + 1):
            name = rng.choices(operation_names, weights=weights, k=1)[0]
            try:
                outcome = await asyncio.wait_for(
                    OPERATIONS[name](args.dsn, args),
                    timeout=args.operation_timeout,
                )
            except Exception as exc:
                outcome = f"{name}_unexpected_{type(exc).__name__}"
            outcomes[outcome] += 1

            if operation == args.warmup_operations:
                baseline = sample(operation, started_at)
                samples.append(baseline)
                print_sample(baseline, baseline, outcomes)
            elif operation % args.report_every == 0 or operation == args.operations:
                row = sample(operation, started_at)
                samples.append(row)
                print_sample(row, baseline, outcomes)
    finally:
        if not args.keep_table:
            await cleanup(args.dsn, args.table)

    if not samples:
        return

    baseline = baseline or samples[0]
    final = samples[-1]
    rss_growth = final.rss_mib - baseline.rss_mib
    py_growth = final.py_current_mib - baseline.py_current_mib
    print()
    print(f"seed={args.seed}")
    print(f"baseline_operation={baseline.operation}")
    print(f"final_operation={final.operation}")
    print(f"rss_growth_after_warmup={rss_growth:.2f} MiB")
    print(f"python_heap_growth_after_warmup={py_growth:.2f} MiB")
    print("outcomes:")
    for name, count in outcomes.most_common():
        print(f"  {name}: {count}")
    print(
        "Interpretation: expected exceptions are part of the workload; "
        "investigate unexpected outcomes or steady post-warmup memory growth."
    )

    if args.fail_on_rss_growth_mib is not None and rss_growth > args.fail_on_rss_growth_mib:
        raise SystemExit(
            "RSS growth exceeded threshold: "
            f"{rss_growth:.2f} MiB > {args.fail_on_rss_growth_mib:.2f} MiB"
        )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Manual chaos memory-growth test. Not intended for regular CI."
    )
    parser.add_argument("--dsn", default=os.environ.get("CLICKHOUSE_DSN", DEFAULT_DSN))
    parser.add_argument("--table", default="test.asynch_chaos_memory_watch")
    parser.add_argument("--operations", type=int, default=300)
    parser.add_argument("--warmup-operations", type=int, default=30)
    parser.add_argument("--report-every", type=int, default=25)
    parser.add_argument("--seed", type=int, default=20260514)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--select-rows", type=int, default=5000)
    parser.add_argument("--stream-rows", type=int, default=5000)
    parser.add_argument("--chaos-rows", type=int, default=50000)
    parser.add_argument("--early-rows", type=int, default=50)
    parser.add_argument("--sleep-each-row", type=float, default=0.0001)
    parser.add_argument("--cancel-after", type=float, default=0.02)
    parser.add_argument("--operation-timeout", type=float, default=5.0)
    parser.add_argument("--keep-table", action="store_true")
    parser.add_argument("--fail-on-rss-growth-mib", type=float)
    return parser.parse_args()


if __name__ == "__main__":
    try:
        asyncio.run(run(parse_args()))
    except KeyboardInterrupt:
        sys.exit(130)
