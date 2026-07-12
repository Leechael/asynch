import pytest

from asynch.errors import ServerException
from tests.test_upstream.columns._helpers import create_table, execute

pytestmark = pytest.mark.asyncio


async def test_all(conn):
    interval = [
        ("YEAR", 1),
        ("MONTH", 2),
        ("WEEK", 3),
        ("DAY", 4),
        ("HOUR", 5),
        ("MINUTE", 6),
        ("SECOND", 7),
    ]
    columns = ", ".join([f"INTERVAL {value} {kind}" for kind, value in interval])

    rv = await execute(conn, f"SELECT {columns}")

    assert rv == [(1, 2, 3, 4, 5, 6, 7)]


@pytest.mark.parametrize(
    "column_type",
    [
        "IntervalYear",
        "IntervalMonth",
        "IntervalWeek",
        "IntervalDay",
        "IntervalHour",
        "IntervalMinute",
        "IntervalSecond",
    ],
)
async def test_interval_insert_roundtrip(conn, column_type):
    try:
        async with create_table(conn, f"a {column_type}") as table:
            await execute(conn, f"INSERT INTO {table} VALUES", [(3,)], types_check=True)
            inserted = await execute(conn, f"SELECT * FROM {table}")
    except ServerException as exc:
        if "cannot be used in tables" in str(exc):
            pytest.skip(f"ClickHouse server cannot use {column_type} in tables")
        raise

    assert inserted == [(3,)]
