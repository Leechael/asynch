import pytest

from asynch.errors import TooLargeStringSize, TypeMismatchError
from tests.test_upstream.columns._helpers import create_table, execute

pytestmark = pytest.mark.asyncio


async def test_simple(conn):
    data = [("a",), ("bb",), ("ccc",), ("dddd",), ("я",)]

    async with create_table(conn, "a FixedString(4)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_non_utf(conn):
    data = [("яндекс".encode("koi8-r"),)]

    async with create_table(conn, "a FixedString(6)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_oversized(conn):
    for data in [[("aaaaa",)], [("тест",)]]:
        async with create_table(conn, "a FixedString(4)") as table:
            with pytest.raises(TooLargeStringSize):
                await execute(conn, f"INSERT INTO {table} (a) VALUES", data)


async def test_nullable(conn):
    data = [(None,), ("test",), (None,), ("nullable",)]

    async with create_table(conn, "a Nullable(FixedString(10))") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_null_byte_in_the_middle(conn):
    data = [("test\0test",)]

    async with create_table(conn, "a FixedString(9)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_empty(conn):
    data = [("",)]

    async with create_table(conn, "a FixedString(5)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_custom_encoding(conn):
    settings = {"strings_encoding": "cp1251"}
    data = [("яндекс",), ("test",)]

    async with create_table(conn, "a FixedString(10)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data, settings=settings)

        inserted = await execute(conn, f"SELECT * FROM {table}", settings=settings)

    assert inserted == data
    assert isinstance(inserted[0][0], str)
    assert isinstance(inserted[1][0], str)


async def test_not_supported_types(conn):
    datas = [[(bytearray(b"asd"),)], [(123,)]]

    async with create_table(conn, "a String") as table:
        for data in datas:
            with pytest.raises(TypeMismatchError) as exc:
                await execute(
                    conn,
                    f"INSERT INTO {table} (a) VALUES",
                    data,
                    types_check=True,
                )
            assert 'for column "a"' in str(exc.value)

            with pytest.raises(AttributeError):
                await execute(conn, f"INSERT INTO {table} (a) VALUES", data)


async def test_bytes_oversized(conn):
    for data in [
        [(bytes("aaaaa".encode("utf-8")),)],
        [(bytes("тест".encode("utf-8")),)],
    ]:
        async with create_table(conn, "a FixedString(4)") as table:
            with pytest.raises(TooLargeStringSize):
                await execute(
                    conn,
                    f"INSERT INTO {table} (a) VALUES",
                    data,
                    settings={"strings_as_bytes": True},
                )


async def test_bytes_not_decoded(conn):
    data = [
        (bytes("яндекс".encode("cp1251")),),
        (bytes("test".encode("cp1251")),),
    ]

    async with create_table(conn, "a FixedString(8)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(
            conn,
            f"SELECT * FROM {table}",
            settings={"strings_as_bytes": True},
        )

    assert inserted == [
        ("яндекс".encode("cp1251") + b"\x00" * 2,),
        ("test".encode("cp1251") + b"\x00" * 4,),
    ]
    assert isinstance(inserted[0][0], bytes)
    assert isinstance(inserted[1][0], bytes)


async def test_bytes_nullable(conn):
    data = [
        (None,),
        (b"test\x00\x00\x00\x00\x00\x00",),
        (None,),
        (b"nullable\x00\x00",),
    ]

    async with create_table(conn, "a Nullable(FixedString(10))") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(
            conn,
            f"SELECT * FROM {table}",
            settings={"strings_as_bytes": True},
        )

    assert inserted == data


async def test_bytes_not_supported_types(conn):
    datas = [[("asd",)], [(bytearray(b"asd"),)], [(123,)]]

    async with create_table(conn, "a String") as table:
        for data in datas:
            with pytest.raises(TypeMismatchError) as exc:
                await execute(
                    conn,
                    f"INSERT INTO {table} (a) VALUES",
                    data,
                    types_check=True,
                    settings={"strings_as_bytes": True},
                )
            assert 'for column "a"' in str(exc.value)

            with pytest.raises(ValueError) as exc:
                await execute(
                    conn,
                    f"INSERT INTO {table} (a) VALUES",
                    data,
                    settings={"strings_as_bytes": True},
                )
            assert "bytes object expected" in str(exc.value)
