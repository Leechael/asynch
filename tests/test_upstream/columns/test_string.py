import pytest

from asynch.connection import Connection
from asynch.errors import TypeMismatchError
from tests.test_upstream.columns._helpers import create_table, execute

pytestmark = pytest.mark.asyncio


async def test_unicode(conn):
    data = [("яндекс",)]

    async with create_table(conn, "a String") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_non_utf(conn):
    data = [("яндекс".encode("koi8-r"),)]

    async with create_table(conn, "a String") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_null_byte_in_the_middle(conn):
    data = [("a\x00b",)]

    async with create_table(conn, "a String") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_nullable(conn):
    data = [(None,), ("test",), (None,), ("nullable",)]

    async with create_table(conn, "a Nullable(String)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_buffer_reader(conn):
    data = [("a" * 300,)] * 300

    async with create_table(conn, "a String") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_compressed_client(config):
    async with Connection(dsn=config.dsn, compression=True) as conn:
        data = [("a" * 300,)]

        async with create_table(conn, "a String") as table:
            await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

            inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_custom_encoding(conn):
    settings = {"strings_encoding": "cp1251"}
    data = [("яндекс",), ("test",)]

    async with create_table(conn, "a String") as table:
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


async def test_bytes_not_decoded(conn):
    data = [
        (bytes("яндекс".encode("cp1251")),),
        (bytes("test".encode("cp1251")),),
    ]

    async with create_table(conn, "a String") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(
            conn,
            f"SELECT * FROM {table}",
            settings={"strings_as_bytes": True},
        )

    assert inserted == data
    assert isinstance(inserted[0][0], bytes)
    assert isinstance(inserted[1][0], bytes)


async def test_bytes_nullable(conn):
    data = [(None,), (b"test",), (None,), (b"nullable",)]

    async with create_table(conn, "a Nullable(String)") as table:
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
