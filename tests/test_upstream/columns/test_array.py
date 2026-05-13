from uuid import UUID

import pytest

from asynch.errors import TypeMismatchError
from tests.test_upstream.columns._helpers import create_table, execute

pytestmark = pytest.mark.asyncio


async def test_empty(conn):
    data = [([],)]

    async with create_table(conn, "a Array(Int32)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_simple(conn):
    data = [([100, 500],)]

    async with create_table(conn, "a Array(Int32)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_write_column_as_nested_array(conn):
    data = [([100, 500],), ([100, 500],)]

    async with create_table(conn, "a Array(Int32)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_nested_with_enum(conn):
    data = [([["hello", "world"], ["hello"]],)]

    async with create_table(
        conn, "a Array(Array(Enum8('hello' = -1, 'world' = 2)))"
    ) as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_nested_of_nested(conn):
    value = [
        [[255, 170], [127, 127, 127, 127, 127], [170, 170, 170], [170]],
        [[255, 255, 255], [255]],
        [[255], [255], [255]],
    ]
    data = [(value, value)]

    async with create_table(
        conn, "a Array(Array(Array(Int32))), b Array(Array(Array(Int32)))"
    ) as table:
        await execute(conn, f"INSERT INTO {table} (a, b) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_multidimensional(conn):
    data = [
        (
            [
                [["str1_1", "str1_2", None], [None]],
                [["str1_3", "str1_4", None], [None]],
            ],
        ),
        ([[["str2_1", "str2_2", None], [None]]],),
        ([[["str3_1", "str3_2", None], [None]]],),
    ]

    async with create_table(conn, "a Array(Array(Array(Nullable(String))))") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_empty_nested(conn):
    data = [([], [[]])]

    async with create_table(
        conn, "a Array(Array(Array(Int32))), b Array(Array(Array(Int32)))"
    ) as table:
        await execute(conn, f"INSERT INTO {table} (a, b) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_type_mismatch_error(conn):
    async with create_table(conn, "a Array(Int32)") as table:
        with pytest.raises(TypeMismatchError):
            await execute(conn, f"INSERT INTO {table} (a) VALUES", [("test",)])

    async with create_table(conn, "a Array(Int32)") as table:
        with pytest.raises(TypeMismatchError):
            await execute(conn, f"INSERT INTO {table} (a) VALUES", [(["test"],)])


async def test_string_array(conn):
    data = [(["aaa", "bbb"],)]

    async with create_table(conn, "a Array(String)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_string_nullable_array(conn):
    data = [(["aaa", None, "bbb"],)]

    async with create_table(conn, "a Array(Nullable(String))") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_uuid_array(conn):
    data = [
        (
            [
                UUID("c0fcbba9-0752-44ed-a5d6-4dfb4342b89d"),
                UUID("2efcead4-ff55-4db5-bdb4-6b36a308d8e0"),
            ],
        )
    ]

    async with create_table(conn, "a Array(UUID)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_uuid_nullable_array(conn):
    data = [
        (
            [
                UUID("c0fcbba9-0752-44ed-a5d6-4dfb4342b89d"),
                None,
                UUID("2efcead4-ff55-4db5-bdb4-6b36a308d8e0"),
            ],
        )
    ]

    async with create_table(conn, "a Array(Nullable(UUID))") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_tuple_array(conn):
    data = [([],)]

    async with create_table(conn, "a Array(Tuple(Int32))") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data
