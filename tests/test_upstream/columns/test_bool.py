import pytest

from asynch.errors import TypeMismatchError
from tests.test_upstream.columns._helpers import create_table, execute

pytestmark = pytest.mark.asyncio


async def test_simple(conn):
    data = [(1,), (0,), (True,), (False,), (None,), ("False",), ("",)]

    async with create_table(conn, "a Bool") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == [
        (True,),
        (False,),
        (True,),
        (False,),
        (False,),
        (True,),
        (False,),
    ]


async def test_errors(conn):
    async with create_table(conn, "a Bool") as table:
        with pytest.raises(TypeMismatchError):
            await execute(
                conn,
                f"INSERT INTO {table} (a) VALUES",
                [(1,)],
                types_check=True,
            )


async def test_nullable(conn):
    data = [(None,), (True,), (False,)]

    async with create_table(conn, "a Nullable(Bool)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == [(None,), (True,), (False,)]
