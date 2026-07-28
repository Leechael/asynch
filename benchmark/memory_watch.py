from __future__ import annotations

import argparse
import asyncio
import gc
import json
import os
import subprocess
import sys
import tracemalloc
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
    cycle: int
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


def write_markdown_report(path: str, report: dict) -> None:
    baseline = report["baseline"]
    final = report["final"]
    rows = (
        ("RSS", baseline["rss_mib"], final["rss_mib"], report["rss_growth_mib"], "MiB"),
        (
            "Python heap",
            baseline["py_current_mib"],
            final["py_current_mib"],
            report["python_heap_growth_mib"],
            "MiB",
        ),
        ("File descriptors", baseline["fd_count"], final["fd_count"], report["fd_growth"], ""),
        (
            "Pending tasks",
            baseline["pending_tasks"],
            final["pending_tasks"],
            report["pending_task_growth"],
            "",
        ),
    )
    lines = [
        "### Normal workload memory watch",
        "",
        f"Cycles: {report['baseline_cycle']} baseline, {report['final_cycle']} final",
        "",
        "| Metric | Baseline | Final | Growth |",
        "| --- | ---: | ---: | ---: |",
    ]
    for name, baseline_value, final_value, growth, unit in rows:
        suffix = f" {unit}" if unit else ""
        lines.append(
            f"| {name} | {baseline_value:.2f}{suffix} | {final_value:.2f}{suffix} | "
            f"{growth:+.2f}{suffix} |"
        )
    with open(path, "w", encoding="utf-8") as report_file:
        report_file.write("\n".join(lines) + "\n")


def sample(cycle: int, started_at: float) -> Sample:
    gc.collect()
    current, peak = tracemalloc.get_traced_memory()
    return Sample(
        cycle=cycle,
        rss_mib=current_rss_mib(),
        py_current_mib=current / 1024 / 1024,
        py_peak_mib=peak / 1024 / 1024,
        fd_count=current_fd_count(),
        pending_tasks=pending_task_count(),
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
        f"py_peak={row.py_peak_mib:>8.2f} MiB "
        f"fds={row.fd_count:>4} tasks={row.pending_tasks:>3}",
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
    fd_growth = final.fd_count - baseline.fd_count if baseline.fd_count >= 0 else 0
    task_growth = final.pending_tasks - baseline.pending_tasks
    print()
    print(f"baseline_cycle={baseline.cycle}")
    print(f"final_cycle={final.cycle}")
    print(f"rss_growth_after_warmup={rss_growth:.2f} MiB")
    print(f"python_heap_growth_after_warmup={py_growth:.2f} MiB")
    print(f"fd_growth_after_warmup={fd_growth}")
    print(f"pending_task_growth_after_warmup={task_growth}")
    print(
        "Interpretation: a small plateau is normal; repeated positive growth "
        "across longer runs is the signal to investigate."
    )

    report = {
        "baseline_cycle": baseline.cycle,
        "final_cycle": final.cycle,
        "rss_growth_mib": rss_growth,
        "python_heap_growth_mib": py_growth,
        "fd_growth": fd_growth,
        "pending_task_growth": task_growth,
        "baseline": asdict(baseline),
        "final": asdict(final),
        "samples": [asdict(row) for row in samples],
    }
    if args.json_output:
        write_json_report(args.json_output, report)
    if args.markdown_output:
        write_markdown_report(args.markdown_output, report)

    failures = []
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
        description="Asynch memory-growth smoke test for manual and release validation."
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
    parser.add_argument("--json-output")
    parser.add_argument("--markdown-output")
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
