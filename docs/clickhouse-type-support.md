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
- Containers/wrappers: `Array`, `Tuple`, `Nested`, `Nullable`, `LowCardinality`, `SimpleAggregateFunction`, `AggregateFunction`, `Map`
- Intervals: `IntervalNanosecond`, `IntervalMicrosecond`, `IntervalMillisecond`, `IntervalSecond`, `IntervalMinute`, `IntervalHour`, `IntervalDay`, `IntervalWeek`, `IntervalMonth`, `IntervalQuarter`, `IntervalYear`
- Geo aliases: `Point`, `LineString`, `MultiLineString`, `Ring`, `Polygon`, `MultiPolygon`, `Geometry`
- Semi-structured and dynamic: `JSON`, legacy `Object('json')`, `Dynamic`, `Variant`
- Vector/quantized: `QBit`

## Supported Aliases

ClickHouse aliases now normalize before column construction:

- Integer aliases: `TINYINT`, `INT1`, `BYTE`, `SMALLINT`, `INT`, `INTEGER`, `MEDIUMINT`, `BIGINT`, `SIGNED`, plus their `SIGNED`/`UNSIGNED` variants, `YEAR`, `BIT`, `SET`, `UNSIGNED`
- Float aliases: `FLOAT`, `REAL`, `SINGLE`, `DOUBLE`, `DOUBLE PRECISION`
- String aliases: `CHAR`, `NCHAR`, `CHARACTER`, `VARCHAR`, `NVARCHAR`, `VARCHAR2`, `TEXT`, `TINYTEXT`, `MEDIUMTEXT`, `LONGTEXT`, `BLOB`, `CLOB`, `TINYBLOB`, `MEDIUMBLOB`, `LONGBLOB`, `BYTEA`, `CHARACTER LARGE OBJECT`, `CHARACTER VARYING`, `CHAR LARGE OBJECT`, `CHAR VARYING`, `NATIONAL CHAR`, `NATIONAL CHARACTER`, `NATIONAL CHARACTER LARGE OBJECT`, `NATIONAL CHARACTER VARYING`, `NATIONAL CHAR VARYING`, `NCHAR VARYING`, `NCHAR LARGE OBJECT`, `BINARY LARGE OBJECT`, `BINARY VARYING`, `VARBINARY`
- Fixed string alias: `BINARY(N)` -> `FixedString(N)`
- Decimal aliases: `DEC`, `NUMERIC`, `FIXED`
- Other aliases: `TIMESTAMP`, `INET4`, `INET6`, `bool`, `boolean`, `ENUM`

## Support Notes

The following families have dedicated Native serialization and are covered by
layout-level tests plus real ClickHouse roundtrip tests:

- `Variant` uses the basic discriminator stream and nested variant streams. For
  inserts, ambiguous values can be tagged as `("TypeName", value)` or
  `{"type": "TypeName", "value": value}`.
- `Dynamic` supports V1/V2 structure serialization and writes V1-compatible
  Native data. Values can be inferred or explicitly tagged with the same
  convention as `Variant`.
- `JSON` supports modern `JSON` separately from legacy `Object('json')`. Writes
  use ClickHouse's Native string serialization; reads support the structured
  V1/V2/V3 object layout for ordinary dynamic paths.
- `QBit` supports `BFloat16`, `Float32`, and `Float64` element encodings with
  ClickHouse's bit-plane tuple layout.
- `Geometry` is implemented as ClickHouse's `Variant(LineString,
  MultiLineString, MultiPolygon, Point, Polygon, Ring)`.
- `AggregateFunction` states are function-specific. Built-in Native decoding is
  implemented for `count`, numeric `sum`, and numeric `avg`; other aggregate
  states intentionally remain unsupported until their binary state layouts are
  implemented explicitly.

## Not Yet Supported

No complete generic decoder exists for arbitrary `AggregateFunction(...)`
states, because ClickHouse delegates those bytes to each aggregate function's
own state serializer.
