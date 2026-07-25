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
from datetime import timezone as dt_timezone

import pytest

from asynch.connection import Connection
from asynch.errors import ServerException

VALUE = datetime(2026, 7, 25, 17, 33, 45, 123456)
JUST_BEFORE = datetime(2026, 7, 25, 17, 33, 44, 999999)

# Only an aware value is spelled as a typed literal: it denotes an instant, so
# rebasing it onto the server's zone loses nothing. A naive value denotes a
# wall time in the target column's zone, which only the server can resolve.
AWARE_VALUE = VALUE.replace(tzinfo=dt_timezone.utc)
AWARE_JUST_BEFORE = JUST_BEFORE.replace(tzinfo=dt_timezone.utc)

# 2025-11-02 in New York: 05:30 and 06:30 UTC both read 01:30 on the clock.
DST_FIRST = datetime(2025, 11, 2, 5, 30, 0, 123456, tzinfo=dt_timezone.utc)
DST_SECOND = datetime(2025, 11, 2, 6, 30, 0, 123456, tzinfo=dt_timezone.utc)

SECOND = datetime(2026, 7, 25, 17, 33, 45)
MILLISECOND = datetime(2026, 7, 25, 17, 33, 45, 123000)
MICROSECOND = datetime(2026, 7, 25, 17, 33, 45, 123456)

AWARE_SECOND = SECOND.replace(tzinfo=dt_timezone.utc)

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

# A column may carry its own timezone. A bare string is parsed as a wall time
# in that timezone, and the block writer localises a naive value the same way
# (columns/datetimecolumn.py:74). A typed literal without a timezone argument
# is parsed in the server's timezone instead, so the three could disagree on
# which instant a naive value means. Two zones far from any plausible server
# default, so a disagreement cannot hide behind a coincidence.
ZONED_TABLE_COLUMNS = """
    seq       UInt32,
    dt        DateTime('Asia/Tokyo'),
    dt64_6    DateTime64(6, 'America/Los_Angeles')
"""

ZONED_CELLS = [("dt", SECOND), ("dt64_6", MICROSECOND)]


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
async def zoned_table(conn: Connection) -> str:
    name = f"test.dt_zoned_{uuid.uuid4().hex[:8]}"
    async with conn.cursor() as cursor:
        await cursor.execute(f"CREATE TABLE {name} ({ZONED_TABLE_COLUMNS}) ENGINE = Memory")
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


# --------------------------------------------------------------------------
# Timezone-qualified columns: does the spelling change which instant is meant?
#
# Reading such a column back yields an aware datetime in the column's own zone,
# so comparing the wall time answers "which instant did the server understand".
# --------------------------------------------------------------------------


def _wall_time(stored: datetime) -> datetime:
    return stored.replace(tzinfo=None)


@pytest.mark.asyncio
@pytest.mark.parametrize(("column", "expected"), ZONED_CELLS)
async def test_block_path_keeps_the_wall_time_of_a_naive_value(
    conn: Connection,
    zoned_table: str,
    column: str,
    expected: datetime,
):
    """The reference: the block writer localises a naive value to the column."""

    async with conn.cursor() as cursor:
        await cursor.executemany(
            f"INSERT INTO {zoned_table} (seq, {column}) VALUES (%(seq)s, %({column})s)",
            [{"seq": 1, column: VALUE}],
        )

    assert _wall_time(await _stored(conn, zoned_table, column)) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(("column", "expected"), ZONED_CELLS)
async def test_bound_parameter_insert_agrees_with_the_block_path(
    conn: Connection,
    driver: Connection,
    zoned_table: str,
    column: str,
    expected: datetime,
):
    """A naive value must mean the same instant whichever path carries it."""

    async with driver.cursor() as cursor:
        try:
            await cursor.execute(
                f"INSERT INTO {zoned_table} (seq, {column}) VALUES (%(seq)s, %(value)s)",
                {"seq": 1, "value": VALUE},
            )
        except ServerException as exc:
            pytest.skip(f"server rejected this spelling: Code {exc.code}")

    stored = await _stored(conn, zoned_table, column)
    _skip_if_dropped(stored)
    assert _wall_time(stored) == expected


@pytest.mark.asyncio
async def test_typed_spelling_means_the_same_instant_as_the_column_timezone(
    conn: Connection,
    driver: Connection,
    zoned_table: str,
):
    """The question the typed spelling actually raises.

    A bare string is parsed as a wall time in the column's zone, and the block
    writer localises a naive value the same way. A typed literal carries no
    zone, so if the server reads it in its own zone instead, the row seeded here
    stops matching an equality filter for the very value it was seeded with.

    The sub-second part matters: without one the driver emits a bare string and
    the question never arises. The column sits far from any plausible server
    default, so a match cannot be a coincidence.
    """

    async with conn.cursor() as cursor:
        await cursor.executemany(
            f"INSERT INTO {zoned_table} (seq, dt64_6) VALUES (%(seq)s, %(dt64_6)s)",
            [{"seq": 1, "dt64_6": VALUE}],
        )

    async with driver.cursor() as cursor:
        try:
            await cursor.execute(
                f"SELECT count() FROM {zoned_table} WHERE dt64_6 = %(value)s",
                {"value": VALUE},
            )
            matched = await cursor.fetchone()
        except ServerException as exc:
            pytest.skip(f"server rejected this spelling: Code {exc.code}")

    assert matched[0] == 1


# --------------------------------------------------------------------------
# Aware values, where the typed spelling applies.
# --------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("column", [name for name, _ in PRECISIONS])
async def test_aware_parameter_filter_keeps_full_precision(
    conn: Connection,
    driver: Connection,
    table: str,
    column: str,
):
    """What the typed spelling buys: a filter at the value's own resolution."""

    await _seed(conn, table, column, SECOND)

    # No skip here. This spelling has to work on every server in the matrix, so
    # a rejection is a regression rather than a documented limitation.
    async with driver.cursor() as cursor:
        await cursor.execute(
            f"SELECT count() FROM {table} WHERE {column} >= %(value)s",
            {"value": AWARE_VALUE},
        )
        after = await cursor.fetchone()

        await cursor.execute(
            f"SELECT count() FROM {table} WHERE {column} >= %(value)s",
            {"value": AWARE_JUST_BEFORE},
        )
        before = await cursor.fetchone()

    assert after[0] == 0
    assert before[0] == 1


@pytest.mark.asyncio
async def test_aware_parameter_means_the_same_instant_on_a_zoned_column(
    conn: Connection,
    driver: Connection,
    zoned_table: str,
):
    """The typed spelling must not move an instant onto the server's zone.

    The row is seeded through the block path, which resolves the value against
    the column, and then asked for back by equality. A literal read in the
    server's zone instead would stop matching the value it was seeded with, and
    the column sits far enough away that a match cannot be a coincidence.
    """

    async with conn.cursor() as cursor:
        await cursor.executemany(
            f"INSERT INTO {zoned_table} (seq, dt64_6) VALUES (%(seq)s, %(dt64_6)s)",
            [{"seq": 1, "dt64_6": AWARE_VALUE}],
        )

    async with driver.cursor() as cursor:
        await cursor.execute(
            f"SELECT count() FROM {zoned_table} WHERE dt64_6 = %(value)s",
            {"value": AWARE_VALUE},
        )
        matched = await cursor.fetchone()

    assert matched[0] == 1


@pytest.mark.asyncio
async def test_repeated_wall_clock_hour_keeps_two_instants_apart(
    conn: Connection,
    driver: Connection,
    table: str,
):
    """A fall-back hour reads the same on the clock twice.

    New York turns 01:59:59 EDT into 01:00:00 EST on 2025-11-02, so 05:30 and
    06:30 UTC both read 01:30 locally. A spelling that goes through a wall time
    cannot tell them apart, and a filter would then match the wrong row.
    """

    async with conn.cursor() as cursor:
        await cursor.executemany(
            f"INSERT INTO {table} (seq, dt64_6) VALUES (%(seq)s, %(dt64_6)s)",
            [{"seq": 1, "dt64_6": DST_FIRST}, {"seq": 2, "dt64_6": DST_SECOND}],
        )

    async with driver.cursor() as cursor:
        await cursor.execute(
            f"SELECT seq FROM {table} WHERE dt64_6 = %(value)s",
            {"value": DST_FIRST},
        )
        matched = await cursor.fetchall()

    assert [row[0] for row in matched] == [1]


@pytest.mark.asyncio
async def test_aware_whole_second_means_the_same_instant_on_a_zoned_column(
    conn: Connection,
    driver: Connection,
    zoned_table: str,
):
    """A whole second has no fraction to preserve but still has an instant to.

    Sent as a wall time it would carry no zone, and the column would read it as
    a local one, so the row seeded here would stop matching the value it was
    seeded with even though nothing about precision was at stake.
    """

    async with conn.cursor() as cursor:
        await cursor.executemany(
            f"INSERT INTO {zoned_table} (seq, dt) VALUES (%(seq)s, %(dt)s)",
            [{"seq": 1, "dt": AWARE_SECOND}],
        )

    async with driver.cursor() as cursor:
        await cursor.execute(
            f"SELECT count() FROM {zoned_table} WHERE dt = %(value)s",
            {"value": AWARE_SECOND},
        )
        matched = await cursor.fetchone()

    assert matched[0] == 1


@pytest.mark.asyncio
async def test_a_column_named_after_a_source_keyword_still_reads_as_a_data_section(
    conn: Connection,
    driver: Connection,
):
    """ClickHouse takes `format` as an identifier; the driver must not read it as one.

    Were the column list to decide where the rows come from, this statement
    would look like a query and the driver would put a typed literal into a real
    VALUES section, which older servers refuse.
    """

    table = f"test.dt_kw_{uuid.uuid4().hex[:8]}"
    async with conn.cursor() as cursor:
        await cursor.execute(
            f"CREATE TABLE {table} (format String, dt64_6 DateTime64(6)) ENGINE = Memory"
        )

    try:
        async with driver.cursor() as cursor:
            await cursor.execute(
                f"INSERT INTO {table} (format, dt64_6) VALUES (%(format)s, %(dt64_6)s)",
                {"format": "json", "dt64_6": AWARE_VALUE},
            )

        assert await _stored(conn, table, "dt64_6") == MICROSECOND
    finally:
        async with conn.cursor() as cursor:
            await cursor.execute(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.asyncio
async def test_format_values_payload_takes_the_bare_spelling(
    conn: Connection,
    driver: Connection,
    table: str,
):
    """FORMAT Values reaches the same parser as a VALUES section.

    A whole second so the bare spelling is accepted everywhere, and a second
    precision column so a typed literal would not be: servers before 25.4
    refuse one there, which is what this asserts has not been emitted. No skip,
    because that rejection is the failure being guarded against.
    """

    async with driver.cursor() as cursor:
        await cursor.execute(
            f"INSERT INTO {table} (seq, dt) FORMAT Values (1, %(value)s)",
            {"value": AWARE_SECOND},
        )

    assert await _stored(conn, table, "dt") == SECOND
