from enum import IntEnum

import pytest

from tests.test_upstream.columns._helpers import create_table, execute

pytestmark = pytest.mark.asyncio


class A(IntEnum):
    hello = -1
    world = 2


async def test_simple(conn):
    data = [(3,), (2,)]

    async with create_table(conn, "a SimpleAggregateFunction(any, Int32)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_nullable(conn):
    data = [(3,), (None,), (2,)]

    async with create_table(
        conn, "a SimpleAggregateFunction(any, Nullable(Int32))"
    ) as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_simple_agg_function(conn):
    columns = "a SimpleAggregateFunction(anyLast, Enum8('hello' = -1, 'world' = 2))"
    data = [(A.hello,), (A.world,), (-1,), (2,)]

    async with create_table(conn, columns) as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == [("hello",), ("world",), ("hello",), ("world",)]


async def test_simple_agg_function_nullable(conn):
    columns = (
        "a SimpleAggregateFunction(anyLast, "
        "Nullable(Enum8('hello' = -1, 'world' = 2)))"
    )
    data = [(A.hello,), (A.world,), (None,), (-1,), (2,)]

    async with create_table(conn, columns) as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == [("hello",), ("world",), (None,), ("hello",), ("world",)]
