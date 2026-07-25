"""Datetime parameter handling across insert paths and column precisions.

A datetime bound as a query parameter reaches the server through one of two
paths. Data inserts (``args`` is a sequence) go through the native block
protocol, where the server-provided sample block carries the exact column type
and the column writers truncate accordingly. Ordinary queries (``args`` is a
mapping) are substituted textually by ``escape_param``, which never sees a
column type.

These tests pin the contract to the column definition: the stored value must be
decided by the target column, not by the path the caller happened to use. Cases
covering the mapping path are expected to fail until the textual path becomes
column-aware; they are written as plain assertions so CI reports the real
behaviour of each ClickHouse version instead of a guess.
"""

import uuid
from datetime import datetime

import pytest

from asynch.connection import Connection
from asynch.errors import ServerException

VALUE = datetime(2026, 7, 25, 17, 33, 45, 123456)
SECOND = datetime(2026, 7, 25, 17, 33, 45)
MILLISECOND = datetime(2026, 7, 25, 17, 33, 45, 123000)
MICROSECOND = datetime(2026, 7, 25, 17, 33, 45, 123456)

JUST_BEFORE = datetime(2026, 7, 25, 17, 33, 44, 999999)

LITERAL = "2026-07-25 17:33:45.123456"
LITERAL_JUST_BEFORE = "2026-07-25 17:33:44.999999"

TABLE_COLUMNS = """
    seq          UInt32,
    dt           DateTime,
    dt64_3       DateTime64(3),
    dt64_6       DateTime64(6),
    dt_null      Nullable(DateTime),
    dt64_3_null  Nullable(DateTime64(3)),
    s            String
"""

# (column, expected value once stored in that column)
PRECISIONS = [
    ("dt", SECOND),
    ("dt64_3", MILLISECOND),
    ("dt64_6", MICROSECOND),
    ("dt_null", SECOND),
    ("dt64_3_null", MILLISECOND),
]


@pytest.fixture(scope="function")
async def table(conn: Connection) -> str:
    name = f"test.dt_param_{uuid.uuid4().hex[:8]}"
    async with conn.cursor() as cursor:
        await cursor.execute(f"CREATE TABLE {name} ({TABLE_COLUMNS}) ENGINE = Memory")
    yield name
    async with conn.cursor() as cursor:
        await cursor.execute(f"DROP TABLE IF EXISTS {name}")


@pytest.fixture(scope="function")
async def mutable_table(conn: Connection) -> str:
    """Mutations need a MergeTree table; the Memory engine rejects ALTER UPDATE."""

    name = f"test.dt_mut_{uuid.uuid4().hex[:8]}"
    async with conn.cursor() as cursor:
        await cursor.execute(
            f"CREATE TABLE {name} ({TABLE_COLUMNS}) ENGINE = MergeTree ORDER BY seq"
        )
    yield name
    async with conn.cursor() as cursor:
        await cursor.execute(f"DROP TABLE IF EXISTS {name}")


async def _stored(conn: Connection, table: str, column: str):
    async with conn.cursor() as cursor:
        await cursor.execute(f"SELECT {column} FROM {table}")
        row = await cursor.fetchone()
    return row[0]


async def _execute(conn: Connection, query: str, args=None, settings=None):
    return await conn._connection.execute(query, args=args, settings=settings)


async def _seed(conn: Connection, table: str, column: str, value: datetime) -> None:
    """Store a value through the block path, which already honours the column."""

    await _execute(
        conn,
        f"INSERT INTO {table} (seq, {column}) VALUES (%(seq)s, %({column})s)",
        [{"seq": 1, column: value}],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(("column", "expected"), PRECISIONS)
async def test_block_path_stores_value_at_column_precision(
    conn: Connection,
    table: str,
    column: str,
    expected: datetime,
):
    """Sequence args reach the typed block writers, which honour the column.

    Dict rows are matched to the sample block by column name (``block.py:204``),
    so the keys have to be the column names rather than the placeholder names.
    """

    async with conn.cursor() as cursor:
        await cursor.executemany(
            f"INSERT INTO {table} (seq, {column}) VALUES (%(seq)s, %({column})s)",
            [{"seq": 1, column: VALUE}],
        )

    assert await _stored(conn, table, column) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(("column", "expected"), PRECISIONS)
async def test_mapping_path_stores_value_at_column_precision(
    conn: Connection,
    table: str,
    column: str,
    expected: datetime,
):
    """Mapping args are substituted textually and must reach the same result."""

    async with conn.cursor() as cursor:
        await cursor.execute(
            f"INSERT INTO {table} (seq, {column}) VALUES (%(seq)s, %(value)s)",
            {"seq": 1, "value": VALUE},
        )

    assert await _stored(conn, table, column) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(("column", "expected"), PRECISIONS)
async def test_mapping_path_with_server_side_expression_stores_at_column_precision(
    conn: Connection,
    table: str,
    column: str,
    expected: datetime,
):
    """A non-placeholder expression in VALUES keeps the statement on the textual path.

    This is the shape real applications write when a column is filled by the
    server (``generateSnowflakeID()``, ``now()``, ``rand()``), so it can never be
    rewritten into a data insert.
    """

    async with conn.cursor() as cursor:
        await cursor.execute(
            f"INSERT INTO {table} (seq, {column}) VALUES (rand(), %(value)s)",
            {"value": VALUE},
        )

    assert await _stored(conn, table, column) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("column", [name for name, _ in PRECISIONS])
async def test_where_filter_uses_full_precision_of_bound_value(
    conn: Connection,
    table: str,
    column: str,
):
    """The same escaping serves reads: a bound datetime must compare exactly.

    Every column holds the same whole second, so a bound value one fraction
    later must exclude the row and one fraction earlier must keep it. Losing the
    fraction on the way in silently widens the filter instead of failing.
    """

    await _seed(conn, table, column, SECOND)

    async with conn.cursor() as cursor:
        await cursor.execute(
            f"SELECT count() FROM {table} WHERE {column} >= %(value)s",
            {"value": VALUE},
        )
        after = await cursor.fetchone()

        await cursor.execute(
            f"SELECT count() FROM {table} WHERE {column} >= %(value)s",
            {"value": JUST_BEFORE},
        )
        before = await cursor.fetchone()

    assert after[0] == 0
    assert before[0] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("column", [name for name, _ in PRECISIONS])
async def test_where_filter_with_typed_literal_uses_full_precision(
    conn: Connection,
    table: str,
    column: str,
):
    """Server capability probe: the typed literal form in the same comparisons."""

    await _seed(conn, table, column, SECOND)

    async with conn.cursor() as cursor:
        await cursor.execute(
            f"SELECT count() FROM {table} WHERE {column} >= toDateTime64('{LITERAL}', 6)"
        )
        after = await cursor.fetchone()

        await cursor.execute(
            f"SELECT count() FROM {table} "
            f"WHERE {column} >= toDateTime64('{LITERAL_JUST_BEFORE}', 6)"
        )
        before = await cursor.fetchone()

    assert after[0] == 0
    assert before[0] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(("column", "expected"), PRECISIONS)
async def test_typed_literal_conforms_to_column_definition(
    conn: Connection,
    table: str,
    column: str,
    expected: datetime,
):
    """Server capability probe: does a typed literal let the column decide?

    No driver code is involved. If every ClickHouse version in the matrix
    accepts ``toDateTime64(..., 6)`` into these columns, ``escape_param`` can
    emit a typed literal instead of a bare string and the column definition
    decides the result without the driver knowing the schema.
    """

    async with conn.cursor() as cursor:
        await cursor.execute(
            f"INSERT INTO {table} (seq, {column}) VALUES (1, toDateTime64('{LITERAL}', 6))"
        )

    assert await _stored(conn, table, column) == expected


@pytest.mark.asyncio
async def test_typed_literal_into_string_column_keeps_microseconds(
    conn: Connection,
    table: str,
):
    """Server capability probe: a typed literal bound to a String column."""

    async with conn.cursor() as cursor:
        await cursor.execute(
            f"INSERT INTO {table} (seq, s) VALUES (1, toDateTime64('{LITERAL}', 6))"
        )

    assert await _stored(conn, table, "s") == LITERAL


@pytest.mark.asyncio
@pytest.mark.parametrize(("column", "expected"), PRECISIONS)
async def test_mutation_sets_value_at_column_precision(
    conn: Connection,
    mutable_table: str,
    column: str,
    expected: datetime,
):
    """ALTER UPDATE evaluates its SET as an expression, not through VALUES."""

    await _seed(conn, mutable_table, column, SECOND)
    await _execute(
        conn,
        f"ALTER TABLE {mutable_table} UPDATE {column} = %(value)s WHERE seq = 1",
        args={"value": VALUE},
        settings={"mutations_sync": 1},
    )

    assert await _stored(conn, mutable_table, column) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(("column", "expected"), PRECISIONS)
async def test_mutation_with_typed_literal_sets_value_at_column_precision(
    conn: Connection,
    mutable_table: str,
    column: str,
    expected: datetime,
):
    """Server capability probe: the typed literal form in a mutation."""

    await _seed(conn, mutable_table, column, SECOND)
    await _execute(
        conn,
        f"ALTER TABLE {mutable_table} UPDATE {column} = toDateTime64('{LITERAL}', 6) WHERE seq = 1",
        settings={"mutations_sync": 1},
    )

    assert await _stored(conn, mutable_table, column) == expected


@pytest.mark.asyncio
async def test_select_projects_typed_literal_at_full_precision(conn: Connection):
    """The read direction: a sub-second value must decode without losing digits."""

    rows = await _execute(conn, f"SELECT toDateTime64('{LITERAL}', 6)")

    assert rows[0][0] == MICROSECOND


@pytest.mark.asyncio
async def test_bare_fractional_string_is_rejected_by_second_precision_column(
    conn: Connection,
    table: str,
):
    """Why the textual path breaks today: escape_param emits exactly this string."""

    async with conn.cursor() as cursor:
        with pytest.raises(ServerException):
            await cursor.execute(f"INSERT INTO {table} (seq, dt) VALUES (1, '{LITERAL}')")


@pytest.mark.asyncio
async def test_bare_fractional_string_is_accepted_by_subsecond_column(
    conn: Connection,
    table: str,
):
    """The counterpart: the same string is what a DateTime64 column needs."""

    async with conn.cursor() as cursor:
        await cursor.execute(f"INSERT INTO {table} (seq, dt64_6) VALUES (1, '{LITERAL}')")

    assert await _stored(conn, table, "dt64_6") == MICROSECOND


# The VALUES section is the only context that rejects a sub-second value on the
# LTS servers: the same typed literal compares fine in WHERE everywhere. These
# probe whether a query setting is enough to make the VALUES section accept one
# form for every column precision, which would avoid teaching the driver the
# schema. Each strategy is (name, settings, literal expression).
INSERT_STRATEGIES = [
    (
        "best_effort_text",
        {"date_time_input_format": "best_effort"},
        f"'{LITERAL}'",
    ),
    (
        "no_template_deduction_typed",
        {"input_format_values_deduce_templates_of_expressions": 0},
        f"toDateTime64('{LITERAL}', 6)",
    ),
    (
        "inaccurate_literal_types_typed",
        {"input_format_values_accurate_types_of_literals": 0},
        f"toDateTime64('{LITERAL}', 6)",
    ),
]

# A usable strategy has to satisfy every column at once: truncate for a second
# precision column, keep every digit for a microsecond one, and store a value
# rather than NULL where the column is nullable.
STRATEGY_TARGETS = PRECISIONS


@pytest.mark.asyncio
@pytest.mark.parametrize(("name", "settings", "literal"), INSERT_STRATEGIES)
@pytest.mark.parametrize(("column", "expected"), STRATEGY_TARGETS)
async def test_insert_strategy_conforms_to_column_definition(
    conn: Connection,
    table: str,
    name: str,
    settings: dict,
    literal: str,
    column: str,
    expected: datetime,
):
    """Server capability probe: can one VALUES form serve every column precision?"""

    await _execute(
        conn,
        f"INSERT INTO {table} (seq, {column}) VALUES (1, {literal})",
        settings=settings,
    )

    assert await _stored(conn, table, column) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("column", [name for name, _ in PRECISIONS])
async def test_where_filter_with_best_effort_setting_uses_full_precision(
    conn: Connection,
    table: str,
    column: str,
):
    """Server capability probe: does the insert-side setting also serve reads?

    Best-effort input parsing accepts the fractional text, but parsing it into
    the column's own type would compare at the column's resolution rather than
    at the value's. If that holds, the setting alone cannot serve both contexts
    and the comparison needs a form that carries the sub-second part.
    """

    await _seed(conn, table, column, SECOND)
    settings = {"date_time_input_format": "best_effort"}

    after = await _execute(
        conn,
        f"SELECT count() FROM {table} WHERE {column} >= %(value)s",
        args={"value": VALUE},
        settings=settings,
    )
    before = await _execute(
        conn,
        f"SELECT count() FROM {table} WHERE {column} >= %(value)s",
        args={"value": JUST_BEFORE},
        settings=settings,
    )

    assert after[0][0] == 0
    assert before[0][0] == 1
