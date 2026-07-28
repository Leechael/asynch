# Manual Benchmarks

These scripts are for local investigation and release checks. They are not part
of the regular pytest suite or CI gate.

## Memory Growth Watch

Run a repeated insert, buffered select, and streaming select workload while
sampling current process RSS and Python heap usage:

```bash
CLICKHOUSE_DSN='clickhouse://default:@127.0.0.1:19000/default?async_insert=0' \
python -m benchmark.memory_watch
```

Useful longer run:

```bash
CLICKHOUSE_DSN='clickhouse://default:@127.0.0.1:19000/default?async_insert=0' \
python -m benchmark.memory_watch \
  --cycles 300 \
  --batch-size 10000 \
  --select-rows 50000 \
  --stream-rows 50000
```

The script prints growth after warmup. A small plateau is normal; repeated
positive RSS or Python heap growth across longer runs is the signal to inspect
for leaks. For explicit local thresholds and a machine-readable report, add for example:

```bash
--json-output reports/memory-watch.json \
--markdown-output reports/memory-watch.md \
--fail-on-rss-growth-mib 128 \
--fail-on-python-heap-growth-mib 32 \
--fail-on-fd-growth 2 \
--fail-on-task-growth 0
```

## Chaos Memory Watch

Run a randomized workload that mixes normal operations with expected failures:
server exceptions, malformed inserts, abandoned streams, task cancellation, and
client-side disconnects while a query is in flight.

```bash
CLICKHOUSE_DSN='clickhouse://default:@127.0.0.1:19000/default?async_insert=0' \
python -m benchmark.chaos_memory_watch
```

Useful longer run:

```bash
CLICKHOUSE_DSN='clickhouse://default:@127.0.0.1:19000/default?async_insert=0' \
python -m benchmark.chaos_memory_watch \
  --operations 1000 \
  --report-every 50 \
  --chaos-rows 100000
```

The default seed is fixed for reproducibility. Change `--seed` to explore a
different operation order. Pass `--fail-on-unexpected-outcomes` to turn any
outcome outside the script's explicit expected set into a failure.

## Release validation

The `pypi` GitHub Actions workflow runs both memory watchers before publishing
and posts their Markdown summaries on the pull request associated with the
release commit. A repository owner, member, or collaborator can run the same
validation on an open same-repository pull request by posting this exact comment:

```text
!prelaunch
```

The command validates and builds the distributions but never publishes them.
The workflow can also be started with `workflow_dispatch`, which has the same
non-publishing behavior.

The release-only chaos run adds deterministic ClickHouse process-loss cycles.
It starts a long query, sends `SIGKILL` to the Actions service container, checks
that the driver cleared its wire and query state, restarts ClickHouse, and
verifies that the same Python process can reconnect. Run the same phase locally
against a disposable container with:

```bash
CLICKHOUSE_CONTAINER_ID=asynch-clickhouse \
CLICKHOUSE_DSN='clickhouse://default:@127.0.0.1:9000/default' \
python -m benchmark.chaos_memory_watch \
  --operations 300 \
  --server-kill-cycles 10 \
  --fail-on-unexpected-outcomes
```

Do not point `CLICKHOUSE_CONTAINER_ID` at a shared or production container.
