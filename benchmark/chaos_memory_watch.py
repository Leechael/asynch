from __future__ import annotations

import argparse
import asyncio
import gc
import json
import os
import random
import subprocess
import sys
import tracemalloc
import weakref
from collections import Counter
from dataclasses import asdict, dataclass
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
    fd_count: int
    pending_tasks: int
    elapsed_s: float


def current_rss_mib() -> float:
    output = subprocess.check_output(
        ["ps", "-o", "rss=", "-p", str(os.getpid())],
        text=True,
    )
    return int(output.strip()) / 1024


def current_fd_count() -> int:
    try:
        return len(os.listdir("/proc/self/fd"))
    except OSError:
        return -1


def pending_task_count() -> int:
    current = asyncio.current_task()
    return sum(1 for task in asyncio.all_tasks() if task is not current and not task.done())


def write_json_report(path: str, report: dict) -> None:
    with open(path, "w", encoding="utf-8") as report_file:
        json.dump(report, report_file, indent=2)
        report_file.write("\n")


def sample(operation: int, started_at: float) -> Sample:
    gc.collect()
    current, peak = tracemalloc.get_traced_memory()
    return Sample(
        operation=operation,
        rss_mib=current_rss_mib(),
        py_current_mib=current / 1024 / 1024,
        py_peak_mib=peak / 1024 / 1024,
        fd_count=current_fd_count(),
        pending_tasks=pending_task_count(),
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
        f"fds={row.fd_count:>4} tasks={row.pending_tasks:>3} "
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


def run_docker(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", *args],
        check=check,
        capture_output=True,
        text=True,
    )


async def wait_for_clickhouse(container_id: str, timeout: float) -> None:
    deadline = perf_counter() + timeout
    while perf_counter() < deadline:
        result = await asyncio.to_thread(
            run_docker,
            "exec",
            container_id,
            "clickhouse-client",
            "--query",
            "SELECT 1",
            check=False,
        )
        if result.returncode == 0:
            return
        await asyncio.sleep(0.5)
    raise TimeoutError(f"ClickHouse did not become ready within {timeout:.1f}s")


async def wait_for_query_start(conn: Connection, timeout: float) -> None:
    deadline = perf_counter() + timeout
    while perf_counter() < deadline:
        if conn.is_query_executing:
            await asyncio.sleep(0.1)
            return
        await asyncio.sleep(0.01)
    raise TimeoutError(f"query did not start within {timeout:.1f}s")


async def server_kill_during_query(dsn: str, args) -> str:
    container_id = args.clickhouse_container_id
    if not container_id:
        raise RuntimeError("--clickhouse-container-id is required for server kill cycles")

    conn = Connection(dsn=dsn)
    task = None
    killed = False
    try:
        await conn.connect()
        proto = conn._connection
        resource_refs = [
            weakref.ref(resource)
            for resource in (
                proto.reader,
                proto.writer,
                proto.reader.reader,
                proto.writer.writer,
            )
        ]
        task = asyncio.create_task(proto.execute("SELECT sleep(30)"))
        await wait_for_query_start(conn, args.query_start_timeout)

        killed = True
        await asyncio.to_thread(run_docker, "kill", "--signal", "KILL", container_id)
        try:
            await asyncio.wait_for(task, timeout=args.query_failure_timeout)
        except asyncio.TimeoutError as exc:
            raise RuntimeError(
                f"query did not fail within {args.query_failure_timeout:.1f}s"
            ) from exc
        except Exception:
            pass
        else:
            raise RuntimeError("query completed successfully after ClickHouse was killed")

        state = {
            "connected": proto.connected,
            "is_query_executing": proto.is_query_executing,
            "reader": proto.reader,
            "writer": proto.writer,
            "block_reader": proto.block_reader,
            "block_reader_raw": proto.block_reader_raw,
            "block_writer": proto.block_writer,
            "server_info": proto.server_info,
            "last_query": proto.last_query,
        }
        dirty = {name: value for name, value in state.items() if value not in (None, False)}
        if dirty:
            raise RuntimeError(f"driver state was not cleared after connection loss: {dirty}")

        task = None
        await asyncio.sleep(0)
        gc.collect()
        await asyncio.sleep(0)
        leaked_resources = sum(reference() is not None for reference in resource_refs)
        if leaked_resources:
            raise RuntimeError(f"{leaked_resources} retired connection resources were retained")
    finally:
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except BaseException:
                pass
        if killed:
            await asyncio.to_thread(run_docker, "start", container_id)
            await wait_for_clickhouse(container_id, args.server_restart_timeout)
        await conn.close()

    await conn.connect()
    try:
        rows = await execute(conn, "SELECT 1")
        if rows != [(1,)]:
            raise RuntimeError(f"recovery query returned {rows!r}")
    finally:
        await conn.close()
    return "server_kill_recovered"


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

EXPECTED_OUTCOMES = {
    "normal_select_ok",
    "normal_insert_ok",
    "full_stream_ok",
    "server_exception_expected",
    "malformed_insert_expected",
    "abandon_stream_exception",
    "abandon_stream_disconnect",
    "cancel_query_cancelled",
    "cancel_query_exception_after_cancel",
    "disconnect_during_query_exception",
    "server_kill_recovered",
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

        for cycle in range(1, args.server_kill_cycles + 1):
            operation = args.operations + cycle
            try:
                outcome = await asyncio.wait_for(
                    server_kill_during_query(args.dsn, args),
                    timeout=(
                        args.query_start_timeout
                        + args.query_failure_timeout
                        + args.server_restart_timeout
                        + args.operation_timeout
                    ),
                )
            except Exception as exc:
                outcome = f"server_kill_unexpected_{type(exc).__name__}"
            outcomes[outcome] += 1
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
    fd_growth = final.fd_count - baseline.fd_count if baseline.fd_count >= 0 else 0
    task_growth = final.pending_tasks - baseline.pending_tasks
    unexpected = {name: count for name, count in outcomes.items() if name not in EXPECTED_OUTCOMES}
    print()
    print(f"seed={args.seed}")
    print(f"baseline_operation={baseline.operation}")
    print(f"final_operation={final.operation}")
    print(f"rss_growth_after_warmup={rss_growth:.2f} MiB")
    print(f"python_heap_growth_after_warmup={py_growth:.2f} MiB")
    print(f"fd_growth_after_warmup={fd_growth}")
    print(f"pending_task_growth_after_warmup={task_growth}")
    print("outcomes:")
    for name, count in outcomes.most_common():
        print(f"  {name}: {count}")
    print(
        "Interpretation: expected exceptions are part of the workload; "
        "investigate unexpected outcomes or steady post-warmup memory growth."
    )

    report = {
        "seed": args.seed,
        "baseline_operation": baseline.operation,
        "final_operation": final.operation,
        "rss_growth_mib": rss_growth,
        "python_heap_growth_mib": py_growth,
        "fd_growth": fd_growth,
        "pending_task_growth": task_growth,
        "outcomes": dict(outcomes),
        "unexpected_outcomes": unexpected,
        "samples": [asdict(row) for row in samples],
    }
    if args.json_output:
        write_json_report(args.json_output, report)

    failures = []
    if args.fail_on_unexpected_outcomes and unexpected:
        failures.append(f"unexpected outcomes: {unexpected}")
    if args.fail_on_rss_growth_mib is not None and rss_growth > args.fail_on_rss_growth_mib:
        failures.append(
            f"RSS growth exceeded threshold: {rss_growth:.2f} MiB > "
            f"{args.fail_on_rss_growth_mib:.2f} MiB"
        )
    if (
        args.fail_on_python_heap_growth_mib is not None
        and py_growth > args.fail_on_python_heap_growth_mib
    ):
        failures.append(
            f"Python heap growth exceeded threshold: {py_growth:.2f} MiB > "
            f"{args.fail_on_python_heap_growth_mib:.2f} MiB"
        )
    if args.fail_on_fd_growth is not None and fd_growth > args.fail_on_fd_growth:
        failures.append(
            f"file descriptor growth exceeded threshold: {fd_growth} > {args.fail_on_fd_growth}"
        )
    if args.fail_on_task_growth is not None and task_growth > args.fail_on_task_growth:
        failures.append(
            f"pending task growth exceeded threshold: {task_growth} > {args.fail_on_task_growth}"
        )
    if failures:
        raise SystemExit("\n".join(failures))


def parse_args():
    parser = argparse.ArgumentParser(
        description="Chaos memory-growth test for manual and release validation."
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
    parser.add_argument(
        "--clickhouse-container-id",
        default=os.environ.get("CLICKHOUSE_CONTAINER_ID"),
    )
    parser.add_argument("--server-kill-cycles", type=int, default=0)
    parser.add_argument("--query-start-timeout", type=float, default=5.0)
    parser.add_argument("--query-failure-timeout", type=float, default=10.0)
    parser.add_argument("--server-restart-timeout", type=float, default=60.0)
    parser.add_argument("--keep-table", action="store_true")
    parser.add_argument("--json-output")
    parser.add_argument("--fail-on-unexpected-outcomes", action="store_true")
    parser.add_argument("--fail-on-rss-growth-mib", type=float)
    parser.add_argument("--fail-on-python-heap-growth-mib", type=float)
    parser.add_argument("--fail-on-fd-growth", type=int)
    parser.add_argument("--fail-on-task-growth", type=int)
    return parser.parse_args()


if __name__ == "__main__":
    try:
        asyncio.run(run(parse_args()))
    except KeyboardInterrupt:
        sys.exit(130)
