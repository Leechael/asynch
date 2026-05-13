# ClickHouse Type Support Matrix

Source baseline:

- Local ClickHouse checkout: `repos/ClickHouse/src/DataTypes`
- Runtime check: ClickHouse `26.5.1.628`, revision `54510`
- Runtime inventory query: `SELECT name, alias_to, case_insensitive FROM system.data_type_families`

## Supported Families

`asynch` supports these ClickHouse native type families through binary protocol
roundtrip tests or parser-level coverage:

- Numeric: `Int8`, `Int16`, `Int32`, `Int64`, `Int128`, `Int256`, `UInt8`, `UInt16`, `UInt32`, `UInt64`, `UInt128`, `UInt256`, `BFloat16`, `Float32`, `Float64`
- Decimal: `Decimal`, `Decimal32`, `Decimal64`, `Decimal128`, `Decimal256`
- Date/time: `Date`, `Date32`, `DateTime`, `DateTime32`, `DateTime64`, `Time`, `Time64`
- Strings: `String`, `FixedString`
- Domain/simple: `Bool`, `UUID`, `IPv4`, `IPv6`, `Nothing`, `Null`
- Enum: `Enum`, `Enum8`, `Enum16`
- Containers/wrappers: `Array`, `Tuple`, `Nested`, `Nullable`, `LowCardinality`, `SimpleAggregateFunction`, `Map`
- Intervals: `IntervalNanosecond`, `IntervalMicrosecond`, `IntervalMillisecond`, `IntervalSecond`, `IntervalMinute`, `IntervalHour`, `IntervalDay`, `IntervalWeek`, `IntervalMonth`, `IntervalQuarter`, `IntervalYear`
- Geo aliases: `Point`, `LineString`, `MultiLineString`, `Ring`, `Polygon`, `MultiPolygon`
- Legacy object JSON: `Object('json')`

## Supported Aliases

ClickHouse aliases now normalize before column construction:

- Integer aliases: `TINYINT`, `INT1`, `BYTE`, `SMALLINT`, `INT`, `INTEGER`, `MEDIUMINT`, `BIGINT`, `SIGNED`, plus their `SIGNED`/`UNSIGNED` variants, `YEAR`, `BIT`, `SET`, `UNSIGNED`
- Float aliases: `FLOAT`, `REAL`, `SINGLE`, `DOUBLE`, `DOUBLE PRECISION`
- String aliases: `CHAR`, `NCHAR`, `CHARACTER`, `VARCHAR`, `NVARCHAR`, `VARCHAR2`, `TEXT`, `TINYTEXT`, `MEDIUMTEXT`, `LONGTEXT`, `BLOB`, `CLOB`, `TINYBLOB`, `MEDIUMBLOB`, `LONGBLOB`, `BYTEA`, `CHARACTER LARGE OBJECT`, `CHARACTER VARYING`, `CHAR LARGE OBJECT`, `CHAR VARYING`, `NATIONAL CHAR`, `NATIONAL CHARACTER`, `NATIONAL CHARACTER LARGE OBJECT`, `NATIONAL CHARACTER VARYING`, `NATIONAL CHAR VARYING`, `NCHAR VARYING`, `NCHAR LARGE OBJECT`, `BINARY LARGE OBJECT`, `BINARY VARYING`, `VARBINARY`
- Fixed string alias: `BINARY(N)` -> `FixedString(N)`
- Decimal aliases: `DEC`, `NUMERIC`, `FIXED`
- Other aliases: `TIMESTAMP`, `INET4`, `INET6`, `bool`, `boolean`, `ENUM`

## Not Yet Supported

These families exist in modern ClickHouse, but require dedicated native
substream/custom serialization that is not compatible with the existing
single-column primitives:

- `AggregateFunction`
- `Dynamic`
- `Geometry` / `GEOMETRY` because it is backed by `Variant(...)`
- `JSON` (the modern type, distinct from legacy `Object('json')`)
- `QBit`
- `Variant`

They should remain explicit `UnknownTypeError` cases until their binary layouts
are implemented and covered by real ClickHouse roundtrip tests.
