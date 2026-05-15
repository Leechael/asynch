# asynch

![license](https://img.shields.io/github/license/Leechael/asynch)
![ci](https://github.com/Leechael/asynch/actions/workflows/ci.yml/badge.svg)

`asynch` is an asyncio ClickHouse driver that speaks the native TCP protocol.
It keeps the public import/package name from the original project, but this
repository is now a maintained fork by **Leechael**.

The upstream project at `long2ice/asynch` has been quiet for a while. This fork
is used as the default GitHub branch because it carries protocol, DB-API, type
system, and test-suite updates needed by modern ClickHouse and downstream async
SQLAlchemy integrations.

## Why This Fork

The original project already provided a useful async native driver. This fork
continues that work with a practical compatibility goal:

- Keep the native TCP driver usable with recent ClickHouse protocol revisions.
- Preserve the existing async API and package name where possible.
- Align the DB-API surface with the parts of PEP 249 that are independent of
  sync vs async execution.
- Keep behavior close to `clickhouse-driver` and compatible with
  `clickhouse-sqlalchemy` async adapter work.
- Test against both older CI ClickHouse images and newer real ClickHouse
  instances.

## What Changed From Upstream

This branch contains substantial changes compared with the original project:

- Native protocol support has been advanced through revision `54483`, including
  newer handshake/addendum fields, query/client info fields, profile/progress
  packets, server logs, compressed log/profile-event packets, parallel replica
  protocol fields, and out-of-order aggregation bucket metadata.
- Query parameter handling now covers both local substitution and ClickHouse
  server-side parameters when the server revision supports them.
- Date and DateTime escaping has been tightened, including DateTime64
  fractional precision preservation.
- The DB-API compatibility surface now exposes `apilevel`, `threadsafety`,
  `paramstyle`, exception hierarchy, type constructors, type objects,
  `description`, `rowcount`, and no-result-set fetch behavior.
- ClickHouse type coverage has been expanded for newer or less common families
  such as `BFloat16`, `Time`, `Time64`, `DateTime32`, `QBit`, `JSON`,
  `Dynamic`, `Variant`, `Geometry`, `AggregateFunction`,
  `SimpleAggregateFunction`, newer interval families, aliases, and conversion
  edge cases.
- Pooling, reconnection, insert-drain behavior, profile events, compressed
  reads, and real-world operator/substitution cases have broader tests.
- The test suite has been updated for Python 3.9 through 3.14, recent
  dependency versions, and ClickHouse server capability differences.

## Installation

Until this fork is published under its own release channel, install it directly
from GitHub:

```shell
pip install "asynch @ git+https://github.com/Leechael/asynch.git"
```

With optional cityhash support for ClickHouse compression:

```shell
pip install "asynch[compression] @ git+https://github.com/Leechael/asynch.git"
```

The import name remains unchanged:

```python
from asynch import Connection
```

If you install `asynch` from PyPI without a Git URL, you may get the original
upstream package instead of this maintained fork.

## Quick Start

Create a connection with a DSN:

```python
from asynch import Connection


async def main():
    async with Connection(
        dsn="clickhouse://default:@127.0.0.1:9000/default",
    ) as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT 1")
            assert await cursor.fetchone() == (1,)
```

Or pass connection parameters explicitly:

```python
from asynch import Connection


async def main():
    async with Connection(
        host="127.0.0.1",
        port=9000,
        user="default",
        password="",
        database="default",
    ) as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT version()")
            print(await cursor.fetchone())
```

## Cursor Usage

Fetch all rows:

```python
async with conn.cursor() as cursor:
    await cursor.execute("SELECT number FROM numbers(3)")
    rows = await cursor.fetchall()
    assert rows == [(0,), (1,), (2,)]
```

Use a dict cursor:

```python
from asynch.cursors import DictCursor


async with conn.cursor(cursor=DictCursor) as cursor:
    await cursor.execute("SELECT 1 AS value")
    rows = await cursor.fetchall()
    assert rows == [{"value": 1}]
```

Insert rows:

```python
async with conn.cursor() as cursor:
    await cursor.execute(
        "INSERT INTO test.events (id, name) VALUES",
        [(1, "alpha"), (2, "beta")],
    )
    assert cursor.rowcount == 2
```

Set per-query ClickHouse settings:

```python
async with conn.cursor() as cursor:
    cursor.set_settings({"max_threads": 2})
    await cursor.execute("SELECT count() FROM system.numbers LIMIT 10")
```

## Parameters

Local parameter substitution uses DB-API-style `pyformat` placeholders:

```python
async with conn.cursor() as cursor:
    await cursor.execute(
        "SELECT %(value)s, %(name)s",
        {"value": 42, "name": "Ada"},
    )
    assert await cursor.fetchone() == (42, "Ada")
```

Server-side ClickHouse parameters are supported when the negotiated server
revision supports `DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS`:

```python
async with conn.cursor() as cursor:
    cursor.set_settings({"server_side_params": True})
    await cursor.execute(
        "SELECT {value:Int32}, {text:String}",
        {"value": 42, "text": "Ada"},
    )
    assert await cursor.fetchone() == (42, "Ada")
```

Older ClickHouse servers do not support server-side parameters. In that case,
use local `pyformat` substitution.

## DB-API Compatibility

This is still an async driver, so `execute()`, `fetchone()`, `fetchall()`,
`commit()`, and similar operations are awaited. The fork nevertheless aligns
with PEP 249 where the concepts do not depend on sync execution:

- `asynch.apilevel == "2.0"`
- `asynch.threadsafety == 1`
- `asynch.paramstyle == "pyformat"`
- PEP 249 exception classes are exported.
- `Date`, `Time`, `Timestamp`, `DateFromTicks`, `TimeFromTicks`,
  `TimestampFromTicks`, and `Binary` are exported.
- Cursor `description` follows the DB-API 7-item column tuple shape.
- Cursor `rowcount` is maintained for selects, inserts, and no-result cases.
- Fetching from a cursor without a result set raises `ProgrammingError`.

ClickHouse is not a transactional database in the usual DB-API sense. In this
fork, `commit()` is a no-op for compatibility and `rollback()` raises
`NotSupportedError`.

## Pooling

```python
from asynch.pool import Pool


async def main():
    async with Pool(minsize=1, maxsize=4) as pool:
        async with pool.connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT 1")
                assert await cursor.fetchone() == (1,)
```

Manual lifecycle is also supported:

```python
pool = Pool(minsize=1, maxsize=4)
await pool.startup()
try:
    async with pool.connection() as conn:
        ...
finally:
    await pool.shutdown()
```

## ClickHouse Server Notes

ClickHouse features vary by server version and by feature flags. This fork tries
to negotiate protocol features and skip unsupported behavior in tests, but your
application should still account for server capability differences.

Examples:

- `server_side_params` requires newer protocol support.
- `JSON`, `Dynamic`, `Variant`, `QBit`, and some interval/type aliases depend on
  ClickHouse version and experimental settings.
- Some servers expose a type family but still reject it in table columns.
- OpenTelemetry server log formatting differs across ClickHouse versions.

### Async Inserts And Read-After-Write

`asynch` uses ClickHouse's native protocol. Insert visibility is controlled by
the ClickHouse server settings.

If the server has `async_insert=1` and `wait_for_async_insert=0`, an `INSERT`
may return after ClickHouse accepts data into the async insert buffer, before
the rows are visible to a following `SELECT`:

```python
async with conn.cursor() as cursor:
    await cursor.execute("INSERT INTO test.events (id) VALUES", [(1,)])
    await cursor.execute("SELECT id FROM test.events WHERE id = 1")
    rows = await cursor.fetchall()
```

For read-after-write behavior, enable `wait_for_async_insert`:

```python
async with conn.cursor() as cursor:
    cursor.set_settings({"wait_for_async_insert": 1})
    await cursor.execute("INSERT INTO test.events (id) VALUES", [(1,)])
```

This is ClickHouse server behavior, not Python asyncio behavior.

## Development

Install dependencies with Poetry:

```shell
poetry install --extras compression --no-root --with lint,test
```

Run the CI checks locally:

```shell
make ci
```

To test against a specific ClickHouse instance:

```shell
CLICKHOUSE_DSN="clickhouse://default:@192.168.50.4:9000/default" make ci
```

Formatting and linting:

```shell
make check
make lint
```

There are also manual memory-watch tools under `benchmark/` for long-running
connection/query stress checks.

## Maintainer

This maintained fork is authored and maintained by **Leechael**.

The original project was created by `long2ice` and reused ideas and behavior
from `clickhouse-driver`.

## License

Apache-2.0. See [LICENSE](./LICENSE).
