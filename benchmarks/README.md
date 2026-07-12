# WP01 benchmark measurement line

These are manual, external-timing benchmarks for the `asynch` driver. They are
not a PR CI gate: a benchmark run needs an isolated ClickHouse instance and a
stable host. The scripts fail explicitly if that server cannot be reached; they
never substitute a partial result or an estimate.

Start a local ClickHouse matching the test-suite convention:

```bash
docker run --rm --name asynch-clickhouse \
  -e CLICKHOUSE_SKIP_USER_SETUP=1 -p 9000:9000 \
  clickhouse/clickhouse-server:latest
```

Install the repository's development dependencies (the comparison driver is
already in the `dev` group), then point the scripts at it:

```bash
poetry install --extras compression --with dev,test,lint
export CLICKHOUSE_DSN='clickhouse://default:@127.0.0.1:9000/default'
```

## Pool contention (D)

Run the latency proxy in a separate terminal. Its delay is one way, so a
`--delay-ms 5` proxy is reported to the benchmark as a 10 ms injected RTT.
The proxy delays the first byte of each TCP read burst in each direction and
does not parse or alter ClickHouse packets; this applies latency to the small
ping/query request-response exchanges measured by D.

```bash
python -m benchmarks._proxy --listen 19000 --target 127.0.0.1:9000 --delay-ms 5
CLICKHOUSE_DSN='clickhouse://default:@127.0.0.1:19000/default' \
  python -m benchmarks.bench_pool_contention --rtt-ms 10
```

The default D run covers workers `1,8,32,128`, pool max sizes `8,32`, one
warmup plus five measured rounds, and five `SELECT 1` loops per worker in every
round. Run the command separately for injected RTTs `0,1,5,20` ms (use the
direct server for `0`); use `--json` for a machine-readable report. Every
report includes the ClickHouse version/revision, Python version, parameters,
all raw acquisition/cycle samples, and p50/p90/p99/max.

The criterion is fixed before data collection: for high-contention settings,
if increasing `maxsize` improves median aggregate ops/sec by no more than 10%
and the median is within `[0.5x, 2x]` of `1 / (2 * injected_RTT)`, the
lock-inside-RTT bottleneck is **established**. Otherwise it is **not
established** (or not assessed with zero injected RTT). This describes pool
scheduling only, never protocol-level query concurrency.

## Event-loop lag (A)

```bash
python -m benchmarks.bench_loop_lag
```

The default A run creates deterministic 1,000,000-row wide-Int64 and
String-dense (four 32-byte strings) MergeTree tables, then removes them after a
successful run. It measures a 10 ms heartbeat while asynch streams and fetches
each shape, plus the real synchronous `clickhouse-driver` in
`loop.run_in_executor`. It runs compression off/lz4 and max block sizes
65409/8192, with one warmup and five measured rounds per configuration. Use
`--keep-tables` when investigating, or `--json` for a machine-readable report.

The comparison driver is intentionally reported with its own heartbeat data;
an executor does not imply a clean event loop because its Cython decoder may
hold the GIL. The fixed blocking criterion is: foreground lag p99 is at least
10 times the idle p99 and at least 10 ms. The report includes raw lag samples,
p50/p90/p99/max, foreground duration, and rows/sec for every round. A smaller
block size that does not materially improve the results supports the hypothesis
that buffered block decoding does not yield between rows; it is not a claim of
protocol concurrency.

## Pool-level concurrency (B)

```bash
python -m benchmarks.bench_concurrency \
  --rtt-ms 0 --workers 1,8,32,128 --iterations 5 --seed 20260712
```

Run B directly for 0 ms RTT. For 5 ms RTT, run `_proxy.py` with
`--delay-ms 2.5`, point `CLICKHOUSE_DSN` at the proxy, and pass `--rtt-ms 5`.
Each configuration uses one warmup plus five measured rounds.

This is deliberately a comparison of Pool-managed coroutines against
`clickhouse-driver` worker threads. Every worker owns one connection for an
entire timed round; the output reports aggregate QPS and per-query
p50/p90/p99/max with raw samples. B has no pass/fail verdict. Its output is the
pre-lock-refactor QPS-ratio baseline for asynch Pool relative to the official
synchronous driver plus threads; the same parameters will be rerun after the
lock refactor. It is not an experiment in single-connection concurrency or
protocol multiplexing.

## Single-query throughput (C / WP07 pure-Python control)

```bash
python -m benchmarks.bench_throughput \
  --source-driver-python /path/to/no-cython-source-build/bin/python \
  --source-driver-revision 49afa09
```

The source interpreter must retain `direct_url.json` metadata pointing to the
local Git checkout at the expected revision.

C compares asynch, the installed `clickhouse-driver` Cython extension build,
and a separately supplied `clickhouse-driver` source build made without a
Cython compiler. It covers wide Int64, String, Nullable, and LowCardinality
shapes with compression off/lz4. The latter remains a C-extension build and is
labeled `source_c_extension` in output; it is useful for the plan's
`--no-binary` control, but is never described as pure Python.

If a real pure-Python fallback becomes available, pass
`--pure-python-python` instead. Upstream 0.2.10 builds bundled C sources even
from `--no-binary` source installs, so the script refuses to mislabel that
build as pure Python.

WP07 provides that fallback as four reviewed mechanical translations at
`benchmarks/pure_python_driver/`. Build it in an isolated environment, then run
all four drivers in one invocation:

```bash
python benchmarks/pure_python_driver/build_env.py --output-dir /tmp/wp07-pure-python
python -m benchmarks.bench_throughput \
  --source-driver-python /path/to/no-cython-source-build/bin/python \
  --source-driver-revision 49afa09 \
  --pure-python-python /tmp/wp07-pure-python/venv/bin/python \
  --text-output benchmarks/results/DATE-wp07-throughput.txt \
  --json > benchmarks/results/DATE-wp07-throughput.json
```

With the pure-Python interpreter, C first compares every row returned by the
wheel and pure-Python drivers for Int64, String, Nullable, and LowCardinality
with compression off and lz4. This non-optional correctness gate finishes
before timing begins; a mismatch stops the run. The JSON records its eight
shape/compression checks, the four resolved `.py` module paths, and the pinned
checkout revision. The output also reports p50-based `R_decython =
pure_python / wheel` and `R_async = asynch / pure_python`, alongside the
published `asynch / wheel` ratio. Their product is an algebraic consistency
check for the published p50 table, not an independent validation; it therefore
cannot establish that any measured difference is within run-to-run noise.
Ratios are only interpreted within the same run, never against an earlier run.

The pure-Python control removes the four driver Cython modules only. It retains
the C-extension `lz4` and `clickhouse-cityhash` dependencies, matching the
asynch environment, so the ratios isolate driver decoding rather than claiming
an all-Python compression or hashing stack.

### WP07 first result (2026-07-12)

`results/2026-07-12-wp07-pure-python-throughput.{json,txt}` records the
successful CI run. Its full-row gate passed for every shape/compression mode.
String has `R_decython=0.206–0.208` and `R_async=0.214–0.217`; Nullable has
`0.407–0.412` and `0.282–0.287`, respectively. Both costs are material. String
is nearly balanced, with the Cython-removal cost slightly larger; Nullable's
async cost is larger. This is decision input, not a remediation choice.

## Results

Commit first-run reports under `benchmarks/results/` with an ISO date prefix.
Do not omit slow rounds, substitute means for percentiles, or report an
estimated comparison-driver result. `bench_concurrency.py` and
`bench_throughput.py` are intentionally deferred until D and A have data and a
conclusion; their wording must distinguish pool-level coroutine-vs-thread cost
from nonexistent single-connection protocol multiplexing.

When a local ClickHouse host is unavailable, manually dispatch the existing
`compat-nightly` workflow from the target branch with `wp01_benchmark=true`.
That switch runs only the D/A/B/C and WP07 measurement job—not a PR gate—and
uploads raw JSON/text reports, including the two B RTT controls, the
no-Cython source-build C control, and the pure-Python C control, as an
artifact.
Download that artifact, inspect every raw distribution and conclusion, then
commit the unchanged reports under
`benchmarks/results/` with their measurement date.
