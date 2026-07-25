# Datetime parameters

How a bound `datetime` reaches ClickHouse, why a sub-second value sometimes gets
rejected, and what to do about it.

## Two paths, only one of which knows your schema

A bound parameter reaches the server one of two ways.

**Data inserts** — you pass a *sequence* of rows:

```python
await cursor.executemany(
    "INSERT INTO events (id, ts) VALUES (%(id)s, %(ts)s)",
    [{"id": 1, "ts": datetime.now()}],
)
```

These go through the native block protocol. The server sends back a sample block
carrying each column's exact type, and the driver encodes the value against that
type. A `DateTime` column gets whole seconds, a `DateTime64(3)` gets
milliseconds, a `DateTime64(6)` gets microseconds. Nothing is parsed as text, so
nothing can be misread. Through SQLAlchemy, `Core insert()` and the ORM always
land here.

**Ordinary queries** — you pass a *mapping*:

```python
await cursor.execute(
    "INSERT INTO events (id, ts) VALUES (%(id)s, %(ts)s)",
    {"id": 1, "ts": datetime.now()},
)
```

Here the statement is assembled as text before anything is sent, so the driver
has no column type to work from. It can only choose how to spell the value; the
server decides what that spelling means.

At this API the boundary is the *type* of `args`: any list, tuple or generator
is a data insert, whatever its length, and a mapping is not.

Through SQLAlchemy the boundary moves, because SQLAlchemy decides for itself
when to call `executemany`. `Core insert()` and the ORM always reach the block
path. A `text()` statement reaches it only with more than one row: SQLAlchemy
unwraps a single-element list into one mapping, so it lands on the textual path
along with a plain dict.

## What the driver spells

For an **aware** value the driver sends the instant itself, as microseconds
since the epoch:

```sql
SELECT ... WHERE ts >= fromUnixTimestamp64Micro(1785000825123456)
ALTER TABLE events UPDATE ts = fromUnixTimestamp64Micro(1785000825123456) WHERE id = 1
```

A wall time would lose the instant in two ways, and this spelling loses it in
neither.

A filter compares at full precision instead of silently widening to the
column's resolution. And the value keeps meaning the moment it meant: a wall
time carries no zone, so a column with one of its own reads it locally and
lands somewhere else, and during a fall-back hour two different instants read
the same on the clock — `2025-11-02 05:30Z` and `06:30Z` are both `01:30` in
New York.

Neither depends on a sub-second part being present, so an aware whole second
takes the same route:

```sql
SELECT ... WHERE ts >= fromUnixTimestamp64Micro(1785000825000000)
```

A **naive** value keeps the plain string. It denotes a wall time, not an
instant, and the zone it belongs to is the target column's. The block writer
localises it that way too (`columns/datetimecolumn.py`). A typed literal carries
no zone and would be read in the server's, which moves the instant whenever the
column carries a zone of its own — a `DateTime64(6, 'America/Los_Angeles')` row
would stop matching the very value it was written with. Only the server can
resolve a naive value against the column, so the driver leaves that to it.

The practical consequence: bind aware datetimes if you want sub-second filters
to be exact. `datetime.now(timezone.utc)` and `pendulum.now("UTC")` already
are.

Where the rows arrive as a payload the driver keeps the plain string, because
ClickHouse parses a payload with the input format rather than the expression
evaluator, and servers before 25.4 reject a typed literal there:

```sql
INSERT INTO events (id, ts) VALUES (1, '2026-07-25 17:33:45.123456')
INSERT INTO events (id, ts) FORMAT Values (1, '2026-07-25 17:33:45.123456')
```

`FORMAT Values` reaches the same parser under another spelling, and every other
format takes its data in a syntax of its own, where a SQL function call would
mean nothing at all.

So the typed spelling applies when both of these hold: the value is aware, and
the statement feeds its rows from a query rather than a payload. Anything else
keeps the plain string.

Turn the behaviour off with `typed_datetime_literals=False` on the connection, or
by setting `ASYNCH_TYPED_DATETIME_LITERALS=off`. It is on by default.

## date_time_input_format is yours, not the driver's

Whether the server accepts `'2026-07-25 17:33:45.123456'` for a second precision
column is governed by [`date_time_input_format`][setting]. The driver never sets
or overrides it.

- `basic` (the ClickHouse default) accepts only `YYYY-MM-DD hh:mm:ss` and unix
  timestamps. A sub-second part is a parse error.
- `best_effort` accepts ISO 8601, RFC 1123, and sub-second parts, truncating to
  whatever the column can hold.

It applies to text-format parsing only, which is why it changes what a `VALUES`
section accepts and does not change how a `WHERE` comparison is evaluated.

Configure it wherever suits you — per query, on the connection, per session with
`SET`, or in a server-side profile:

```python
conn = Connection(dsn=dsn, settings={"date_time_input_format": "best_effort"})
```

## Recommended

- Insert through a data insert when you can. It carries full precision, needs no
  setting, and behaves the same on every server version.
- Bind aware datetimes. A naive value cannot be typed, so its sub-second part is
  at the mercy of the server's parsing.
- If you insert sub-second datetimes through the textual path against a
  `DateTime` column, set `date_time_input_format='best_effort'`.
- On ClickHouse 25.4 and newer none of this is needed for inserts: the default
  `basic` parser already accepts a sub-second part and truncates it.

## Errors you may see

### `Code: 6` — `ServerCannotParseTextError`

```
Code: 6.
DB::Exception: Cannot parse string '2026-07-25 17:33:45.123456' as DateTime
```

The server could not read the text as the target column's type. With a datetime,
this is `date_time_input_format='basic'` meeting a sub-second value on a
`DateTime` column. Set `best_effort`, drop the fraction before binding, or use a
data insert.

### `Code: 53` — `ServerTypeMismatchError`

```
Code: 53.
DB::Exception: Type mismatch in IN or VALUES section. Expected: DateTime. Got: Decimal64
```

The value's type did not match the column. For datetimes this means a typed
literal reached a `VALUES` section on a server older than 25.4. The driver avoids
this by itself; you can hit it by writing `toDateTime64(...)` into `VALUES` by
hand.

Both classes subclass `ServerException`, so `except ServerException` keeps
catching them, and both carry their remedy on the `hint` attribute.

## One case the driver cannot rescue

A sub-second datetime bound into a **`Nullable(DateTime)`** column through the
textual path can be stored as `NULL` instead of raising. Nothing in the response
marks it, so no caller can detect it, and no spelling avoids it: inside a
`VALUES` section the driver has to use a bare string.

This is what the CI matrix measured, not a general rule about ClickHouse:

| Server | `date_time_input_format` | Result |
| --- | --- | --- |
| 24.3-lts, 25.3-lts | `basic` | stored as `NULL` |
| 24.3-lts, 25.3-lts | `best_effort` | stored, truncated to the second |
| latest | either | stored, truncated to the second |

The same shape appears for `ALTER TABLE ... UPDATE` on those servers, where
`best_effort` does not help because a mutation evaluates its `SET` as an
expression rather than through the input format.

Other server builds, other settings and other nullable temporal types were not
measured. The cases above live in `tests/test_datetime_param_matrix.py`; a cell
that hits this outcome skips with `documented hazard: value dropped to NULL`, so
the CI log for any given server shows which combinations it applies to.

Avoid it by setting `best_effort`, by inserting through a data insert, or by
upgrading the server.

[setting]: https://clickhouse.com/docs/en/operations/settings/formats#date_time_input_format
