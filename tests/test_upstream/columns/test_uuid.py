from uuid import UUID

import pytest

from asynch.errors import CannotParseUuidError, TypeMismatchError
from tests.test_upstream.columns._helpers import create_table, execute

pytestmark = pytest.mark.asyncio


async def test_simple(conn):
    data = [
        (UUID("c0fcbba9-0752-44ed-a5d6-4dfb4342b89d"),),
        ("2efcead4-ff55-4db5-bdb4-6b36a308d8e0",),
    ]

    async with create_table(conn, "a UUID") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == [
        (UUID("c0fcbba9-0752-44ed-a5d6-4dfb4342b89d"),),
        (UUID("2efcead4-ff55-4db5-bdb4-6b36a308d8e0"),),
    ]


async def test_type_mismatch(conn):
    data = [(62457709573696417404743346296141175008,)]

    async with create_table(conn, "a UUID") as table:
        with pytest.raises(TypeMismatchError):
            await execute(
                conn,
                f"INSERT INTO {table} (a) VALUES",
                data,
                types_check=True,
            )
        with pytest.raises(AttributeError):
            await execute(conn, f"INSERT INTO {table} (a) VALUES", data)


async def test_bad_uuid(conn):
    async with create_table(conn, "a UUID") as table:
        with pytest.raises(CannotParseUuidError):
            await execute(conn, f"INSERT INTO {table} (a) VALUES", [("a",)])


async def test_nullable(conn):
    data = [(UUID("2efcead4-ff55-4db5-bdb4-6b36a308d8e0"),), (None,)]

    async with create_table(conn, "a Nullable(UUID)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_input_format_null_as_default(conn):
    async with create_table(conn, "a UUID") as table:
        await execute(
            conn,
            f"INSERT INTO {table} (a) VALUES",
            [(None,)],
            settings={"input_format_null_as_default": True},
        )

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == [(UUID(int=0),)]
