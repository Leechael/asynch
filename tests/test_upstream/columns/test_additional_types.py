from datetime import date, datetime, time, timedelta
from decimal import Decimal

import pytest
from pytz import timezone

from tests.test_upstream.columns._helpers import create_table, execute

pytestmark = pytest.mark.asyncio


async def _has_type_family(conn, family):
    rows = await execute(
        conn,
        "SELECT count() FROM system.data_type_families WHERE name = %(family)s",
        {"family": family},
    )
    return bool(rows[0][0])


async def test_bfloat16_roundtrip(conn):
    if not await _has_type_family(conn, "BFloat16"):
        pytest.skip("ClickHouse server does not expose BFloat16")

    async with create_table(conn, "a BFloat16") as table:
        await execute(conn, f"INSERT INTO {table} VALUES", [(42.7,), (-1.25,)], types_check=True)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == [(42.5,), (-1.25,)]


async def test_time_and_time64_roundtrip(conn):
    if not await _has_type_family(conn, "Time"):
        pytest.skip("ClickHouse server does not expose Time")

    settings = {"enable_time_time64_type": 1}
    async with create_table(conn, "t Time, t64 Time64(3)", settings=settings) as table:
        await execute(
            conn,
            f"INSERT INTO {table} VALUES",
            [(time(1, 2, 3), "01:02:03.456")],
            types_check=True,
            settings=settings,
        )

        inserted = await execute(conn, f"SELECT * FROM {table}", settings=settings)

    assert inserted == [(timedelta(hours=1, minutes=2, seconds=3), timedelta(hours=1, minutes=2, seconds=3, milliseconds=456))]


async def test_datetime32_roundtrip(conn):
    if not await _has_type_family(conn, "DateTime32"):
        pytest.skip("ClickHouse server does not expose DateTime32")

    utc = timezone("UTC")
    async with create_table(conn, "a DateTime32('UTC')") as table:
        await execute(conn, f"INSERT INTO {table} VALUES", [("2020-01-02 03:04:05",)], types_check=True)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == [(utc.localize(datetime(2020, 1, 2, 3, 4, 5)),)]


@pytest.mark.parametrize("column_type", ["Decimal32(2)", "Decimal64(4)", "Decimal128(6)", "Decimal256(8)"])
async def test_exact_decimal_families_roundtrip(conn, column_type):
    async with create_table(conn, f"a {column_type}") as table:
        await execute(conn, f"INSERT INTO {table} VALUES", [(1.25,)], types_check=True)

        inserted = await execute(conn, f"SELECT toString(a) FROM {table}")

    assert inserted == [("1.25",)]


@pytest.mark.parametrize("column_type", ["IntervalNanosecond", "IntervalMicrosecond", "IntervalMillisecond", "IntervalQuarter"])
async def test_remaining_interval_families_roundtrip(conn, column_type):
    if not await _has_type_family(conn, column_type):
        pytest.skip(f"ClickHouse server does not expose {column_type}")

    async with create_table(conn, f"a {column_type}") as table:
        await execute(conn, f"INSERT INTO {table} VALUES", [(3,)], types_check=True)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == [(3,)]


@pytest.mark.parametrize(
    "column_type, value",
    [
        ("LineString", [(1.5, 2), (3, 4)]),
        ("MultiLineString", [[(1.5, 2), (3, 4)], [(5.5, 6), (7, 8)]]),
    ],
)
async def test_remaining_geo_families_roundtrip(conn, column_type, value):
    if not await _has_type_family(conn, column_type):
        pytest.skip(f"ClickHouse server does not expose {column_type}")

    settings = {"allow_experimental_geo_types": 1}
    async with create_table(conn, f"a {column_type}", settings=settings) as table:
        await execute(conn, f"INSERT INTO {table} VALUES", [(value,)], settings=settings)

        inserted = await execute(conn, f"SELECT * FROM {table}", settings=settings)

    assert inserted == [(value,)]


async def test_alias_families_roundtrip(conn):
    columns = """
        i TINYINT SIGNED,
        u YEAR,
        f DOUBLE PRECISION,
        s VARCHAR(255),
        d DEC(9, 2),
        ts TIMESTAMP,
        ip INET4,
        b BOOLEAN
    """
    async with create_table(conn, columns) as table:
        await execute(
            conn,
            f"INSERT INTO {table} VALUES",
            [(-1, 2026, 1.5, "hello", 1.25, "2020-01-02 03:04:05", "127.0.0.1", True)],
            types_check=True,
        )

        inserted = await execute(conn, f"SELECT i, u, f, s, d, toDate(ts), toString(ip), b FROM {table}")

    assert inserted == [
        (-1, 2026, 1.5, "hello", Decimal("1.25"), date(2020, 1, 2), "127.0.0.1", True)
    ]
