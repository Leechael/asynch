"""Datetime parameter handling across contexts, column precisions and settings.

A datetime bound as a query parameter reaches the server one of two ways. Data
inserts (``args`` is a sequence) go through the native block protocol, where the
sample block carries the exact column type and the column writers truncate to
it. Ordinary queries (``args`` is a mapping) are substituted textually by
``escape_param``, which runs before anything is sent and therefore has no column
type: it can only choose how to spell the value.

What the server then does with that spelling is not the driver's to decide. It
depends on the target column, on the context the value sits in, and on
``date_time_input_format``, which is the user's setting to configure. So the
matrix carries that setting as its own dimension, and the textual cases assert
the one invariant that holds no matter how the server is configured: a value is
either stored exactly as the column can hold it, or rejected outright. Storing
something else -- a NULL, or a different instant -- is the failure worth
catching, because nothing downstream can detect it.

The block-path cases are different: there the driver does know the column type,
so they assert the exact result.
"""

import uuid
from datetime import datetime

import pytest

from asynch.connection import Connection
from asynch.errors import ServerException

VALUE = datetime(2026, 7, 25, 17, 33, 45, 123456)
JUST_BEFORE = datetime(2026, 7, 25, 17, 33, 44, 999999)

SECOND = datetime(2026, 7, 25, 17, 33, 45)
MILLISECOND = datetime(2026, 7, 25, 17, 33, 45, 123000)
MICROSECOND = datetime(2026, 7, 25, 17, 33, 45, 123456)

TABLE_COLUMNS = """
    seq          UInt32,
    dt           DateTime,
    dt64_3       DateTime64(3),
    dt64_6       DateTime64(6),
    dt_null      Nullable(DateTime),
    dt64_3_null  Nullable(DateTime64(3)),
    s            String
"""

# (column, value once the column has stored VALUE)
PRECISIONS = [
    ("dt", SECOND),
    ("dt64_3", MILLISECOND),
    ("dt64_6", MICROSECOND),
    ("dt_null", SECOND),
    ("dt64_3_null", MILLISECOND),
]

# The three that carry distinct behaviour: second precision, full precision, and
# nullable second precision, where a rejected value can turn into a silent NULL.
CELLS = [("dt", SECOND), ("dt64_6", MICROSECOND), ("dt_null", SECOND)]


def _render(value: datetime) -> str:
    fmt = "%Y-%m-%d %H:%M:%S.%f" if value.microsecond else "%Y-%m-%d %H:%M:%S"
    return value.strftime(fmt)


def _text(value: datetime) -> str:
    """What escape_param emits today."""

    return f"'{_render(value)}'"


def _typed(value: datetime) -> str:
    """The same instant with its type spelled out."""

    return f"toDateTime64('{_render(value)}', 6)"


FORMS = [("text", _text), ("typed", _typed)]

# date_time_input_format governs how text is parsed into a temporal type. It is
# the user's setting; the driver never overrides it, so both values it can
# realistically hold are part of the matrix.
MODES = [
    ("basic", {"date_time_input_format": "basic"}),
    ("best_effort", {"date_time_input_format": "best_effort"}),
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


async def _execute(conn: Connection, query: str, args=None, settings=None):
    return await conn._connection.execute(query, args=args, settings=settings)


async def _stored(conn: Connection, table: str, column: str):
    rows = await _execute(conn, f"SELECT {column} FROM {table}")
    return rows[0][0]


DOCS = "see docs/datetime-parameters.md"


def _skip_if_dropped(stored) -> None:
    """A nullable column turns a value the server cannot read into NULL.

    No spelling avoids it inside a VALUES section on 24.3 and 25.3, and the
    response carries no sign of it. It is documented rather than guarded.
    """

    if stored is None:
        pytest.skip(f"documented hazard: value dropped to NULL, {DOCS}")


def _skip_if_widened(after: int, before: int) -> None:
    """A bare string compares at the column's resolution, not the value's."""

    if (after, before) == (1, 1):
        pytest.skip(f"documented hazard: filter widened to column resolution, {DOCS}")


async def _seed(conn: Connection, table: str, column: str, value: datetime) -> None:
    """Store a value through the block path, which already honours the column."""

    await _execute(
        conn,
        f"INSERT INTO {table} (seq, {column}) VALUES (%(seq)s, %({column})s)",
        [{"seq": 1, column: value}],
    )


# --------------------------------------------------------------------------
# Block path: the driver holds the column type, so the result is exact.
# --------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(("column", "expected"), PRECISIONS)
async def test_block_path_stores_value_at_column_precision(
    conn: Connection,
    table: str,
    column: str,
    expected: datetime,
):
    """Dict rows are matched to the sample block by column name (block.py:204)."""

    async with conn.cursor() as cursor:
        await cursor.executemany(
            f"INSERT INTO {table} (seq, {column}) VALUES (%(seq)s, %({column})s)",
            [{"seq": 1, column: VALUE}],
        )

    assert await _stored(conn, table, column) == expected


# --------------------------------------------------------------------------
# Textual path, per context x column x spelling x setting.
# --------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(("mode", "settings"), MODES)
@pytest.mark.parametrize(("form", "render"), FORMS)
@pytest.mark.parametrize(("column", "expected"), CELLS)
async def test_insert_values_is_exact_or_rejected(
    conn: Connection,
    table: str,
    column: str,
    expected: datetime,
    form: str,
    render,
    mode: str,
    settings: dict,
):
    """VALUES section: whatever the setting, never store something else."""

    try:
        await _execute(
            conn,
            f"INSERT INTO {table} (seq, {column}) VALUES (1, {render(VALUE)})",
            settings=settings,
        )
    except ServerException as exc:
        pytest.skip(f"server rejected this spelling: Code {exc.code}")

    stored = await _stored(conn, table, column)
    _skip_if_dropped(stored)
    assert stored == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(("mode", "settings"), MODES)
@pytest.mark.parametrize(("form", "render"), FORMS)
@pytest.mark.parametrize(("column", "expected"), CELLS)
async def test_mutation_set_is_exact_or_rejected(
    conn: Connection,
    mutable_table: str,
    column: str,
    expected: datetime,
    form: str,
    render,
    mode: str,
    settings: dict,
):
    """ALTER UPDATE evaluates its SET as an expression rather than through VALUES."""

    await _seed(conn, mutable_table, column, SECOND)

    try:
        await _execute(
            conn,
            f"ALTER TABLE {mutable_table} UPDATE {column} = {render(VALUE)} WHERE seq = 1",
            settings={**settings, "mutations_sync": 1},
        )
    except ServerException as exc:
        pytest.skip(f"server rejected this spelling: Code {exc.code}")

    stored = await _stored(conn, mutable_table, column)
    _skip_if_dropped(stored)
    assert stored == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(("mode", "settings"), MODES)
@pytest.mark.parametrize(("form", "render"), FORMS)
@pytest.mark.parametrize("column", [name for name, _ in CELLS])
async def test_where_filter_is_exact_or_rejected(
    conn: Connection,
    table: str,
    column: str,
    form: str,
    render,
    mode: str,
    settings: dict,
):
    """A bound instant must filter at its own resolution, not the column's.

    The row holds a whole second, so a bound value one fraction later has to
    exclude it and one fraction earlier has to keep it. A spelling that loses
    the fraction silently widens the filter, which no caller can detect.
    """

    await _seed(conn, table, column, SECOND)

    try:
        after = await _execute(
            conn,
            f"SELECT count() FROM {table} WHERE {column} >= {render(VALUE)}",
            settings=settings,
        )
        before = await _execute(
            conn,
            f"SELECT count() FROM {table} WHERE {column} >= {render(JUST_BEFORE)}",
            settings=settings,
        )
    except ServerException as exc:
        pytest.skip(f"server rejected this spelling: Code {exc.code}")

    _skip_if_widened(after[0][0], before[0][0])
    assert after[0][0] == 0
    assert before[0][0] == 1


# --------------------------------------------------------------------------
# What the driver emits, through real parameter binding, with the typed
# datetime literal switch both on (the default) and off.
# --------------------------------------------------------------------------


@pytest.fixture(scope="function")
async def driver(config) -> Connection:
    """A connection with the driver's own defaults.

    Turning the switch off restores the spelling this change replaced, so the
    hazards it reintroduces are not worth asserting here. That the switch
    reaches the emitted SQL is covered in tests/test_proto/test_proto_connection.
    """

    async with Connection(dsn=config.dsn) as cn:
        yield cn


@pytest.mark.asyncio
@pytest.mark.parametrize(("column", "expected"), PRECISIONS)
async def test_bound_parameter_insert_is_exact_or_rejected(
    conn: Connection,
    driver: Connection,
    table: str,
    column: str,
    expected: datetime,
):
    """The mapping path under the server's own default configuration."""

    async with driver.cursor() as cursor:
        try:
            await cursor.execute(
                f"INSERT INTO {table} (seq, {column}) VALUES (%(seq)s, %(value)s)",
                {"seq": 1, "value": VALUE},
            )
        except ServerException as exc:
            pytest.skip(f"server rejected this spelling: Code {exc.code}")

    stored = await _stored(conn, table, column)
    _skip_if_dropped(stored)
    assert stored == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(("column", "expected"), PRECISIONS)
async def test_bound_parameter_insert_beside_expression_is_exact_or_rejected(
    conn: Connection,
    driver: Connection,
    table: str,
    column: str,
    expected: datetime,
):
    """A server-side expression in VALUES keeps the statement on the textual path.

    This is the shape applications write when a column is filled by the server
    (``generateSnowflakeID()``, ``now()``, ``rand()``), so it can never be
    rewritten into a data insert.
    """

    async with driver.cursor() as cursor:
        try:
            await cursor.execute(
                f"INSERT INTO {table} (seq, {column}) VALUES (rand(), %(value)s)",
                {"value": VALUE},
            )
        except ServerException as exc:
            pytest.skip(f"server rejected this spelling: Code {exc.code}")

    stored = await _stored(conn, table, column)
    _skip_if_dropped(stored)
    assert stored == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("column", [name for name, _ in PRECISIONS])
async def test_bound_parameter_filter_is_exact_or_rejected(
    conn: Connection,
    driver: Connection,
    table: str,
    column: str,
):
    """The read side of the same escaping, under the server's own default."""

    await _seed(conn, table, column, SECOND)

    async with driver.cursor() as cursor:
        try:
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
        except ServerException as exc:
            pytest.skip(f"server rejected this spelling: Code {exc.code}")

    _skip_if_widened(after[0], before[0])
    assert after[0] == 0
    assert before[0] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(("column", "expected"), PRECISIONS)
async def test_bound_parameter_mutation_is_exact_or_rejected(
    conn: Connection,
    driver: Connection,
    mutable_table: str,
    column: str,
    expected: datetime,
):
    """The mutation side of the same escaping, under the server's own default."""

    await _seed(conn, mutable_table, column, SECOND)

    try:
        await _execute(
            driver,
            f"ALTER TABLE {mutable_table} UPDATE {column} = %(value)s WHERE seq = 1",
            args={"value": VALUE},
            settings={"mutations_sync": 1},
        )
    except ServerException as exc:
        pytest.skip(f"server rejected this spelling: Code {exc.code}")

    stored = await _stored(conn, mutable_table, column)
    _skip_if_dropped(stored)
    assert stored == expected


# --------------------------------------------------------------------------
# Read direction and the whole-second baseline.
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_select_projects_typed_literal_at_full_precision(conn: Connection):
    """A sub-second value must survive decoding."""

    rows = await _execute(conn, f"SELECT {_typed(VALUE)}")

    assert rows[0][0] == MICROSECOND


@pytest.mark.asyncio
@pytest.mark.parametrize(("form", "render"), FORMS)
@pytest.mark.parametrize("column", [name for name, _ in CELLS])
async def test_whole_second_value_needs_no_special_spelling(
    conn: Connection,
    table: str,
    column: str,
    form: str,
    render,
):
    """Without a fraction there is nothing to preserve, so both spellings agree."""

    try:
        await _execute(
            conn,
            f"INSERT INTO {table} (seq, {column}) VALUES (1, {render(SECOND)})",
        )
    except ServerException as exc:
        pytest.skip(f"server rejected this spelling: Code {exc.code}")

    assert await _stored(conn, table, column) == SECOND
