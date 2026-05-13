import math

import pytest

from asynch.errors import TypeMismatchError
from tests.test_upstream.columns._helpers import create_table, execute

pytestmark = pytest.mark.asyncio


async def test_chop_to_type(conn):
    data = [
        (3.4028235e38, 3.4028235e38),
        (3.4028235e39, 3.4028235e39),
        (-3.4028235e39, 3.4028235e39),
        (1, 2),
    ]

    async with create_table(conn, "a Float32, b Float64") as table:
        with pytest.raises(TypeMismatchError) as exc:
            await execute(conn, f"INSERT INTO {table} (a, b) VALUES", data)

    assert "Column a" in str(exc.value)


async def test_simple(conn):
    data = [
        (3.4028235e38, 3.4028235e38),
        (3.4028235e39, 3.4028235e39),
        (-3.4028235e39, 3.4028235e39),
        (1, 2),
    ]

    async with create_table(conn, "a Float32, b Float64") as table:
        await execute(conn, f"INSERT INTO {table} (a, b) VALUES", data, types_check=True)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == [
        (3.4028234663852886e38, 3.4028235e38),
        (float("inf"), 3.4028235e39),
        (-float("inf"), 3.4028235e39),
        (1, 2),
    ]


async def test_nullable(conn):
    data = [(None,), (0.5,), (None,), (1.5,)]

    async with create_table(conn, "a Nullable(Float32)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_nan(conn):
    data = [(float("nan"),), (0.5,)]

    async with create_table(conn, "a Float32") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert len(inserted) == 2
    assert math.isnan(inserted[0][0])
    assert inserted[1][0] == 0.5
