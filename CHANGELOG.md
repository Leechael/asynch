# ChangeLog

## 0.4

### Unreleased

- Stop a session timezone announced by the server from outliving the query it
  arrived with. ClickHouse announces `session_timezone` only on a statement
  that feeds its rows through `input()`, and never announces its absence, so
  the value the driver recorded on the connection went on deciding how every
  later result was read. After one such insert carrying
  `session_timezone='Asia/Kolkata'`, a plain `SELECT toDateTime(0)` over the
  same connection came back as `1970-01-01 05:30` while the server still called
  that row `1970-01-01 00:00`, and a pooled connection handed the shift to
  whoever borrowed it next. The value is now cleared as each query starts,
  which is what `clickhouse-driver` has always done and what this driver was
  missing.

### 0.4.0rc4

- Mark a connection closed even when the wire cleanup fails. `Connection.close()`
  now clears `_opened` and sets `_closed` in a `finally` block, so a
  `disconnect()` that raises on an already broken socket no longer leaves the
  object reporting itself as open. This is the path taken by a caller that
  disposes its connections after a network error, such as a SQLAlchemy engine
  whose dialect answers `is_disconnect()`: before, the raising cleanup left a
  connection that still described itself as connected while nothing was on the
  other end of it.
- Reset the wire state even when the socket refuses to close. The protocol level
  `Connection.disconnect()` now calls `reset_state()` and clears `connected` in a
  `finally` block. A `writer.close()` that raised anything other than
  `ConnectionError` used to leave the previous session buffered state and the
  `connected` flag in place, and because `connect()` disconnects first whenever
  it still believes it is connected, the same failing close ran again on every
  reconnect attempt. A process that hit this once could not open another
  connection until it was restarted.
- Gate the release on those two paths. The publish workflow now runs the test
  suite and both memory watches, including one that kills the server mid
  workload, against a live ClickHouse server before it builds and uploads, so a
  tag that regresses connection cleanup or leaks sockets, tasks, or memory
  across reconnects does not reach PyPI. Commenting `!prelaunch` on a pull
  request runs the same checks and reports the results there.

### 0.4.0rc3

- Send an aware `datetime` parameter as the instant it denotes,
  `fromUnixTimestamp64Micro(...)`, rather than as a wall-time string, unless the
  statement carries its rows as a payload (`VALUES`, `FORMAT`), where servers
  before 25.4 refuse a typed literal. A filter now compares at full precision
  instead of widening to the column's resolution, a column carrying its own
  timezone no longer shifts the value, and a daylight saving fall-back hour no
  longer makes two instants look alike. A naive value keeps the plain string:
  its zone belongs to the target column, and only the server can resolve that.
  Turn the spelling off with `typed_datetime_literals=False` or
  `ASYNCH_TYPED_DATETIME_LITERALS=off`.
- Give the two server error codes this surfaces their own `ServerException`
  subclasses, `ServerCannotParseTextError` (Code 6) and `ServerTypeMismatchError`
  (Code 53), each carrying its remedy. Both remain `ServerException`, so
  existing handlers are unaffected.
- Document the whole path, including what `date_time_input_format` governs and
  the one case no spelling can rescue, in `docs/datetime-parameters.md`.

## 0.3

### 0.3.1

- Fix params substitution for select queries. By @dmkulazhenko in #141.

### 0.3.0

- Update the `Connection` and `Pool` classes API. By @stankudrow in #130:
  - remove the deprecated `connected` property from the `Connection` class
  - fix type hinting for `Cursor` class as incoming parameter for the connection `cursor` method
  - make the connection `close` async method more consistent
  - remove the `asynch/connection.py::connect` function
  - get rid of inheritance from the `asyncio.AbstractServer` for the `Pool` class (mypy is satisfied)
  - check the freshness of a connection before giving it from a pool (inspired by the issue #127 from @nils-borrmann-tacto).
  - remove the`asynch/pool.py::create_pool` function
- Move to poetry>=2.1. By @stankudrow in #133.
- Add `mypy` dependency. By @stankudrow in #128.
- Gracefully handle connections terminated by the server. By @nils-borrmann-tacto in #129.
- Remove the deprecated API from `cursor.py` module. By @stankudrow in #125.
- Remove the deprecated `Pool` API. By @stankudrow in #120.
- Allow requesting more connections from a `Pool` object without raising AsynchPoolError("no free connections"). The issue #121 by @itssimon. By @stankudrow in #124.

## 0.2

### 0.2.5

- Add more validation rules in the `parse_dsn` function. By @stankudrow in #113
- Reconsider the API of the `Connection`, `Cursor` and `Pool` classes and deprecate outdated methods or properties. Define the DB-API v2.0 compliant exception hierarchy. Update project dependencies and metadata. By @stankudrow in #111.
- Fix infinite iteration case when a cursor object is put in the `async for` loop (the discussion #100 by @KuzenkovAG). By @stankudrow in #112.
- Fix pool connection management (the discussion #108 by @DFilyushin) by @stankudrow in #109:

  - add the asynchronous context manager support to the `Pool` class with the pool "startup()" as `__aenter__` and "shutdown()" as `__aexit__` methods;
  - enrich the `Pool` class with the "connection()" method returning an asynchronous context manager responsible for acquiring connections from a pool object and releasing them back into the pool;
  - refactor the `Connection` and `Pool` classes.
- Add the asynchronous context manager support to the `Connection` class. By @stankudrow in #107.
- Make Python3.9 the minimum supported version. Update the project dependencies, metadata, tests. By @stankudrow in #106.

### 0.2.4

- Reset connection state. By @boolka in #101.
- Add lazy date_lut, similar to clickhouse-driver. By @DaniilAnichin in #99.
- Correct check life connection (#71). By @gnomeby in #98.
- Use maxsize for pool connections (#68). By @gnomeby in #97.
- Add Date32 column (#95). By @cortelf in #96.
- Eliminate `IndexError` cases from the `BufferedReader` class methods when reading from an empty buffer. By @stankudrow in #94.
- Fix a bytearray index out of range error while reading a string. By @pufit in #90.
- Make a connection be closed for `ExecuteContext` manager class. By @KPull in #82.
- Add connection validity check in `acquire` method. By @lxneng in #81.

### 0.2.3

- Support json column. (#73)
- Fix connection with `secure=True` and `verify=False`.
- Fix compression.
- Fix exception `Cannot set verify_mode to CERT_NONE when check_hostname is enabled`.

### 0.2.2

- Add `Int128Column`, `Int256Column`, `UInt128Column`, `UInt256Column`, `Decimal256Column`. (#57)
- Add Geo type support. (#56)
- Add decimals in map support. (#55)
- Add `NestedColumn`. (#54)
- Add execution_options support. (#53)
- Fix `IPv6Column`. (#52)
- Fix execution context exception handling. (#51)
- Fix stream_mode. (#44)
- Fix `SimpleAggregateFunction` for nested. (#41)

### 0.2.1

- Fix ping message for unstable network. (#48)

### 0.2.0

- Fix compression not working. (#36)
- Add `BoolColumn`. (#38)

## 0.1

### 0.1.9

- Fix LowCardinalityColumn keys column exception. (#17)

### 0.1.8

- Fix bug in protocol for `FixedString`

### 0.1.7

- Fix bug with `FixedString`

### 0.1.6

- Fix syntax error

### 0.1.5

- Fix syntax error
- Fix `BufferReader.read_bytes`

### 0.1.4

- Fix bugs with `TupleColumn`

### 0.1.3

- Fix bugs with `ArrayColumn` and `LowCardinalityColumn`.

### 0.1.2

- Fix exception and read data bugs.

### 0.1.1

- Add connect pool.

### 0.1.0

- Release first version.
