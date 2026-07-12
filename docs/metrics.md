# Metrics and tuning

Metrics are disabled by default. The driver keeps no global registry and does
not depend on a metrics exporter. Each `Connection` owns the metrics for its
most recent query, and each `Pool` owns its acquisition counters.

## 1. Timing model and collection points

`Connection.last_query.client_timings` is `None` unless metrics are enabled.
When enabled, it contains these fields:

- `network_wait`: cumulative time spent waiting for socket reads while decoding
  result blocks.
- `decode`: result-block wall time less the `network_wait` observed during that
  block. This includes decompression, checksum verification, and column
  decoding.
- `decompress`: the decompression portion of `decode`.
- `max_block_decode`: the largest single block `decode` value.
- `ttfb`: time from completing `send_query()` to the first `DATA` packet.
- `blocks`, `rows`, `bytes_compressed`, and `bytes_raw`: result-block counters.

There is only one socket-wait probe: `BufferedReader._read_into_buffer`, around
`await StreamReader.read(...)`. The compressed reader gets bytes through its
`raw_reader`, which is that same plain `BufferedReader`; it does not read the
socket itself. This covers compressed and uncompressed responses without
counting the same wait twice.

For compressed blocks, `bytes_compressed` is the payload size declared by the
compression header (including its uncompressed-size field), and `bytes_raw` is
the uncompressed size declared by that header. For uncompressed blocks,
`bytes_raw` is the plain reader byte-count delta.

## 2. Reading `decode_share`

For `execute()` calls, calculate:

```text
decode_share = client_timings.decode / last_query.elapsed
```

A `decode_share` above about `0.5` together with a high
`max_block_decode` means Python-side decoding dominates this workload. Other
coroutines in the same process can be delayed during those blocks. A value
below about `0.2` is usually a server-waiting workload where asyncio is doing
useful work.

For `execute_iter()`, `elapsed` is intentionally not stored because the caller
controls the lifetime of the stream. Use
`decode / (network_wait + decode)` or measure wall time around the application's
own iteration instead.

The `0.5`, `0.2`, and a 10 ms loop-lag alert are initial experience-based
starting points, not fixed limits. Revise them with production data for the
specific query mix and latency target. `max_block_decode` is approximately the
largest one-turn event-loop blockage; compare its spikes with the loop-lag
heartbeat below.

## 3. Enabling metrics and debug logging

Pass `metrics=True` to a connection:

```python
from asynch import Connection

connection = Connection(metrics=True)
```

Or set `ASYNCH_METRICS` to `1`, `true`, or `on` (case-insensitive):

```shell
export ASYNCH_METRICS=true
```

An explicit `metrics=True` or `metrics=False` takes precedence over the
environment variable. `Pool(metrics=...)` applies the same resolution to the
pool's acquisition counters. It does not turn on per-query connection metrics;
set the environment variable or create direct connections with `metrics=True`
when those are needed.

Per-query summaries use only the `asynch.metrics` logger at `DEBUG` level:

```python
import logging

logging.basicConfig(level=logging.INFO)
logging.getLogger("asynch.metrics").setLevel(logging.DEBUG)
```

This leaves the rest of the `asynch.*` loggers at their existing levels.

## 4. Application loop-lag heartbeat

The driver does not run a background task. Run a heartbeat in the application
and export or log its result alongside query timings:

```python
import asyncio
from time import perf_counter


async def report_loop_lag(interval=0.1):
    next_tick = perf_counter() + interval
    while True:
        await asyncio.sleep(interval)
        now = perf_counter()
        lag = max(now - next_tick, 0.0)
        print(f"event_loop_lag_seconds={lag:.6f}")
        next_tick = now + interval


async def main():
    heartbeat = asyncio.create_task(report_loop_lag())
    try:
        await asyncio.sleep(60)
    finally:
        heartbeat.cancel()
        try:
            await heartbeat
        except asyncio.CancelledError:
            pass


asyncio.run(main())
```

A loop-lag spike near `max_block_decode` is evidence that local decoding, not
server execution, delayed neighbouring coroutines.

## 5. Prometheus export in the application

Install the exporter in the application, not the driver:

```shell
pip install prometheus-client
```

This complete example creates a table, runs a query, and exports the final
query and pool values:

```python
import asyncio

from prometheus_client import Gauge, start_http_server

from asynch import Connection
from asynch.pool import Pool

NETWORK_WAIT = Gauge("asynch_network_wait_seconds", "Network wait in the last query")
DECODE = Gauge("asynch_decode_seconds", "Decode time in the last query")
DECOMPRESS = Gauge("asynch_decompress_seconds", "Decompression time in the last query")
MAX_BLOCK_DECODE = Gauge("asynch_max_block_decode_seconds", "Largest block decode")
POOL_ACQUISITIONS = Gauge("asynch_pool_acquisitions", "Completed pool acquisitions")
POOL_ACQUIRE_WAIT = Gauge("asynch_pool_acquire_wait_seconds", "Total pool acquire wait")


async def main():
    start_http_server(8000)
    async with Connection(
        dsn="clickhouse://default:@127.0.0.1:9000/default",
        metrics=True,
    ) as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("DROP TABLE IF EXISTS metrics_example")
            await cursor.execute("CREATE TABLE metrics_example (n UInt64) ENGINE = Memory")
            await cursor.execute("INSERT INTO metrics_example VALUES", [(1,), (2,), (3,)])
            await cursor.execute("SELECT sum(n) FROM metrics_example")
            assert await cursor.fetchone() == (6,)

        timings = conn.last_query.client_timings
        NETWORK_WAIT.set(timings.network_wait)
        DECODE.set(timings.decode)
        DECOMPRESS.set(timings.decompress)
        MAX_BLOCK_DECODE.set(timings.max_block_decode)

    async with Pool(
        minsize=0,
        maxsize=1,
        dsn="clickhouse://default:@127.0.0.1:9000/default",
        metrics=True,
    ) as pool:
        async with pool.connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT 1")
                assert await cursor.fetchone() == (1,)
        POOL_ACQUISITIONS.set(pool.metrics.acquisitions)
        POOL_ACQUIRE_WAIT.set(pool.metrics.acquire_wait_total)


asyncio.run(main())
```

Use counters or histograms in the application when their aggregation semantics
fit the monitoring system. The values above are intentionally raw driver
measurements.

## 6. Cross-checking server measurements

Compare the client values with server-side facts before changing the driver:

1. Inspect `last_query.progress` and the server's `Progress` packet elapsed
   time where the server provides it.
2. Give production queries a stable `query_id` and find the same query in
   `system.query_log`.
3. Compare server execution and read rows/bytes with the client-side `rows`,
   `bytes_raw`, and wall time.
4. Investigate large differences before concluding that decode or network time
   is the cause; retries, proxies, queueing, and client-side row materializing
   can all change the totals.

## 7. Measurements to possible improvements

| Observation | Candidate next step |
| --- | --- |
| High loop lag and a large decompression share | Offload independently compressed blocks with `run_in_executor`. |
| A throughput benchmark shows per-value async work dominates | Add a synchronous buffered-reader fast path: a whole decompressed block is already in memory, so only refill needs `await`. |
| Both remain high on large scans | Move codec work to C/Cython or document the workload boundary. |
| High `acquire_wait_total` or `acquire_wait_max` | Investigate pool locking and acquisition contention. |

These are decision rules, not enabled features. Collect production measurements
before adding any of them.

## 8. Block and buffer tuning

`max_block_size` is a ClickHouse server setting that cursor streaming passes to
the server. It controls rows per returned block. Roughly:

```text
single event-loop blockage ~= rows per block * decode cost per row
```

The packet generator calls `await asyncio.sleep(0)` after each `DATA` block.
Without that call, awaiting already-buffered data does not reach the event loop:
several buffered blocks could decode back-to-back and starve other tasks. The
yield makes `max_block_size` a real fairness control. Larger blocks can improve
throughput, while smaller blocks reduce the worst delay seen by co-resident
coroutines.

`insert_block_size` controls how many rows the driver sends in each insert
block. It affects insert batching rather than result decoding.

`ASYNCH_BUFFER_SIZE` configures the socket stream buffer in bytes. It can also
be passed as `Connection(buffer_size=...)`; the explicit keyword wins. The
default is 1 MiB and values must be positive integers:

```shell
export ASYNCH_BUFFER_SIZE=262144
```

A large buffer can hold several result blocks. The block-boundary `sleep(0)`
prevents those already-buffered blocks from becoming one uninterrupted decode
run, but it does not make a single large block cheaper to decode. Tune buffer
size and `max_block_size` together with observed throughput and loop lag.

## 9. Pooling and session state

Pooling does not reset a native-protocol session between borrowers. Session
settings and `TIMEZONE_UPDATE` session timezone changes can therefore leak to
the next borrower. This is a protocol property, not a metric failure.

Borrowers must restore the session state they change, or discard and reconnect
a connection that is no longer safe to reuse. Applications using SQLAlchemy
should remember that its dialect constructs `Connection` directly; its pool is
owned by SQLAlchemy, not by `asynch.Pool`.
