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
for leaks. For an explicit local threshold, add for example:

```bash
--fail-on-rss-growth-mib 128
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
different operation order.
