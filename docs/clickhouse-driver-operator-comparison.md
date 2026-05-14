# ClickHouse Driver Operator Comparison

Baseline checked locally:

- Original driver checkout: `repos/clickhouse-driver`
- Current branch: `clickhouse-driver-compact`
- Runtime verification: ClickHouse `26.5.1.628`, UTC, native TCP port `19000`

## Query Parameter Operators

| Area | Original `clickhouse-driver` coverage | Current coverage |
| --- | --- | --- |
| Scalar substitution | Int, null, date/time, datetime, string, enum, float, decimal, UUID | Same coverage, async port retained |
| Array substitution | Array literal substitution and select result | Same, plus array values used inside query operators |
| Tuple substitution | Tuple literal substitution and one `WHERE a IN (...)` case | Same, plus tuple values used in `WHERE (a, b) IN (...)` |
| Server-side params | Primitive server-side params and escaped strings | Same, plus JSON dict through `{x:String}` |
| JSON dict params | Not covered; dict falls through as Python repr | Dict serializes to compact JSON and is safely string-escaped |
| Operator matrix | No dedicated broad `WHERE` operator matrix | Dedicated `WHERE` tests for scalar, string, UUID, temporal, nullable, tuple, array, map, vector, and JSON predicates |

## Current `WHERE` Predicate Coverage

- Scalar comparisons: `=`, `!=`, `<`, `<=`, `>`, `>=`
- Set/range predicates: `IN`, `NOT IN`, `BETWEEN`
- String predicates: `LIKE`, `match`
- Null predicates: `IS NULL`
- Tuple predicates: `(id, value) IN (...)`
- Date/time predicates: Date and DateTime casts in comparisons
- Array predicates: `has`, `hasAll`, `arrayExists`
- Map predicates: `attrs['key']`
- Vector predicates: `L2Distance`
- JSON predicates: modern `JSON` subcolumns and `JSONExtractBool` with Python dict parameters

## Known Boundary

ClickHouse executes these operators server-side. The driver-side contract is
correctly escaping or binding Python values so those expressions receive the
intended ClickHouse literals or Native values.
