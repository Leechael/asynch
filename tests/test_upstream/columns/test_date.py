import os
from datetime import date, datetime
from unittest.mock import patch

import pytest

from tests.test_upstream.columns._helpers import create_table, execute

pytestmark = pytest.mark.asyncio


async def test_do_not_use_timezone(conn):
    data = [(date(1970, 1, 2),)]

    async with create_table(conn, "a Date") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        with patch.dict(os.environ, {"TZ": "US/Hawaii"}):
            inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_insert_datetime_to_date(conn):
    async with create_table(conn, "a Date") as table:
        test_time = datetime(2015, 6, 6, 12, 30, 54)
        await execute(conn, f"INSERT INTO {table} (a) VALUES", [(test_time,)])

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == [(date(2015, 6, 6),)]


async def test_wrong_date_insert(conn):
    data = [
        (date(5555, 1, 1),),
        (date(1, 1, 1),),
        (date(2149, 6, 7),),
    ]

    async with create_table(conn, "a Date") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == [(date(1970, 1, 1),)] * 3


async def test_boundaries(conn):
    data = [(date(1970, 1, 1),), (date(2149, 6, 6),)]

    async with create_table(conn, "a Date") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_date32_wrong_date_insert(conn):
    data = [
        (date(5555, 1, 1),),
        (date(1, 1, 1),),
        (date(2300, 1, 1),),
        (date(1899, 12, 31),),
    ]

    async with create_table(conn, "a Date32") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == [(date(1970, 1, 1),)] * 4


async def test_date32_boundaries_1900(conn):
    data = [(date(1900, 1, 1),), (date(2299, 12, 31),)]

    async with create_table(conn, "a Date32") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_date32_boundaries(conn):
    data = [(date(1925, 1, 1),), (date(2283, 11, 11),)]

    async with create_table(conn, "a Date32") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data
