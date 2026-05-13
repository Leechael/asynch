from decimal import Decimal

import pytest

from tests.test_upstream.columns._helpers import create_table, execute

pytestmark = pytest.mark.asyncio


async def test_simple(conn):
    data = [
        ({},),
        ({"key1": 1},),
        ({"key1": 2, "key2": 20},),
        ({"key1": 3, "key2": 30, "key3": 50},),
    ]

    async with create_table(conn, "a Map(String, UInt64)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_array(conn):
    data = [
        ({"key1": []},),
        ({"key2": [1, 2, 3]},),
        ({"key3": [1, 1, 1, 1]},),
    ]

    async with create_table(conn, "a Map(String, Array(UInt64))") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_decimal(conn):
    data = [
        ({"key1": Decimal("123.45")},),
        ({"key2": Decimal("234.56")},),
        ({"key3": Decimal("345.67")},),
    ]

    async with create_table(conn, "a Map(String, Decimal(9, 2))") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_only_empty_map(conn):
    data = [({},)]

    async with create_table(conn, "a Map(String, Map(String, Int32))") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data
