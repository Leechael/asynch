from enum import IntEnum

import pytest

from asynch.errors import LogicalError
from tests.test_upstream.columns._helpers import create_table, execute

pytestmark = pytest.mark.asyncio


class A(IntEnum):
    hello = -1
    world = 2


class B(IntEnum):
    foo = -300
    bar = 300


async def test_simple(conn):
    data = [(A.hello, B.bar), (A.world, B.foo), (-1, 300), (2, -300)]
    columns = (
        "a Enum8('hello' = -1, 'world' = 2), "
        "b Enum16('foo' = -300, 'bar' = 300)"
    )

    async with create_table(conn, columns) as table:
        await execute(conn, f"INSERT INTO {table} (a, b) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == [
        ("hello", "bar"),
        ("world", "foo"),
        ("hello", "bar"),
        ("world", "foo"),
    ]


async def test_enum_by_string(conn):
    data = [("hello",), ("world",)]

    async with create_table(conn, "a Enum8('hello' = 1, 'world' = 2)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_errors(conn):
    async with create_table(conn, "a Enum8('test' = 1, 'me' = 2)") as table:
        with pytest.raises(LogicalError):
            await execute(conn, f"INSERT INTO {table} (a) VALUES", [(A.world,)])

    async with create_table(conn, "a Enum8('test' = 1, 'me' = 2)") as table:
        with pytest.raises(LogicalError):
            await execute(conn, f"INSERT INTO {table} (a) VALUES", [(3,)])


async def test_quote_in_name(conn):
    data = [(-1,), (" ' t = ",)]

    async with create_table(conn, "a Enum8(' \\' t = ' = -1, 'test' = 2)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == [(" ' t = ",), (" ' t = ",)]


async def test_comma_and_space_in_name(conn):
    data = [(2,), ("two_with_comma, ",)]
    columns = "a Enum8('one' = 1, 'two_with_comma, ' = 2, 'three' = 3)"

    async with create_table(conn, columns) as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == [("two_with_comma, ",), ("two_with_comma, ",)]


async def test_nullable(conn):
    data = [(None,), (A.hello,), (None,), (A.world,)]

    async with create_table(
        conn, "a Nullable(Enum8('hello' = -1, 'world' = 2))"
    ) as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == [(None,), ("hello",), (None,), ("world",)]


async def test_invalid_python_names(conn):
    data = [(1,), (2,), (3,), ("",), ("mro",)]

    async with create_table(
        conn, "a Enum8('mro' = 1, '' = 2, 'test' = 3)"
    ) as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == [("mro",), ("",), ("test",), ("",), ("mro",)]
