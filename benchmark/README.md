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
