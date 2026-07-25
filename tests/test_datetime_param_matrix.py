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

LITERAL = "2026-07-25 17:33:45.123456"

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


async def _stored(conn: Connection, table: str, column: str):
    async with conn.cursor() as cursor:
        await cursor.execute(f"SELECT {column} FROM {table}")
        row = await cursor.fetchone()
    return row[0]


@pytest.mark.asyncio
@pytest.mark.parametrize(("column", "expected"), PRECISIONS)
async def test_block_path_stores_value_at_column_precision(
    conn: Connection,
    table: str,
    column: str,
    expected: datetime,
):
    """Sequence args reach the typed block writers, which honour the column."""

    async with conn.cursor() as cursor:
        await cursor.executemany(
            f"INSERT INTO {table} (seq, {column}) VALUES (%(seq)s, %(value)s)",
            [{"seq": 1, "value": VALUE}],
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
async def test_mapping_path_datetime_filter_compares_against_second_precision_column(
    conn: Connection,
    table: str,
):
    """The same escaping serves SELECT, so a bound datetime must compare sanely."""

    async with conn.cursor() as cursor:
        await cursor.executemany(
            f"INSERT INTO {table} (seq, dt) VALUES (%(seq)s, %(value)s)",
            [{"seq": 1, "value": SECOND}],
        )

        await cursor.execute(
            f"SELECT count() FROM {table} WHERE dt >= %(value)s",
            {"value": VALUE},
        )
        after = await cursor.fetchone()

        await cursor.execute(
            f"SELECT count() FROM {table} WHERE dt >= %(value)s",
            {"value": datetime(2026, 7, 25, 17, 33, 44, 999999)},
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
async def test_typed_literal_compares_against_second_precision_column(
    conn: Connection,
    table: str,
):
    """Server capability probe: the typed literal must also work in WHERE."""

    async with conn.cursor() as cursor:
        await cursor.executemany(
            f"INSERT INTO {table} (seq, dt) VALUES (%(seq)s, %(value)s)",
            [{"seq": 1, "value": SECOND}],
        )

        await cursor.execute(
            f"SELECT count() FROM {table} WHERE dt >= toDateTime64('{LITERAL}', 6)"
        )
        after = await cursor.fetchone()

        await cursor.execute(
            f"SELECT count() FROM {table} WHERE dt >= toDateTime64('2026-07-25 17:33:44.999999', 6)"
        )
        before = await cursor.fetchone()

    assert after[0] == 0
    assert before[0] == 1


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
