from datetime import date

import pytest

from asynch.errors import TypeMismatchError
from tests.test_upstream.columns._helpers import create_table, execute

pytestmark = pytest.mark.asyncio


async def test_simple(conn):
    data = [((1, "a"),), ((2, "b"),)]

    async with create_table(conn, "a Tuple(Int32, String)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_tuple_single_element(conn):
    data = [((1,),), ((2,),)]

    async with create_table(conn, "a Tuple(Int32)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_nullable(conn):
    data = [
        ((1, "a"),),
        ((2, None),),
        ((None, None),),
        ((None, "d"),),
        ((5, "e"),),
    ]

    async with create_table(conn, "a Tuple(Nullable(Int32), Nullable(String))") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_nested_tuple_with_common_types(conn):
    data = [(("one", (1, "a"), "two"),), (("three", (2, "b"), "four"),)]

    async with create_table(conn, "a Tuple(String, Tuple(Int32, String), String)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_tuple_of_tuples(conn):
    columns = "a Tuple(Tuple(Int32, String),Tuple(Enum8('hello' = 1, 'world' = 2), Date))"
    data = [
        (((1, "a"), (1, date(2020, 3, 11))),),
        (((2, "b"), (2, date(2020, 3, 12))),),
    ]

    async with create_table(conn, columns) as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == [
        (((1, "a"), ("hello", date(2020, 3, 11))),),
        (((2, "b"), ("world", date(2020, 3, 12))),),
    ]


async def test_tuple_of_arrays(conn):
    data = [(([1, 2, 3],),), (([4, 5, 6],),)]

    async with create_table(conn, "a Tuple(Array(Int32))") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_array_of_tuples(conn):
    data = [([(1, 2, 3), (4, 5, 6)],), ([(7, 8, 9)],)]

    async with create_table(conn, "a Array(Tuple(UInt8, UInt8, UInt8))") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_type_mismatch_error(conn):
    async with create_table(conn, "a Tuple(Int32)") as table:
        with pytest.raises(TypeMismatchError):
            await execute(conn, f"INSERT INTO {table} (a) VALUES", [("test",)])


async def test_tuple_of_low_cardinality(conn):
    data = [(("1", "2"),)]
    columns = "a Tuple(LowCardinality(String), LowCardinality(String))"

    async with create_table(conn, columns) as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data
