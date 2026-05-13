from contextlib import asynccontextmanager
from datetime import date

import pytest

from tests.test_upstream.columns._helpers import TABLE_NAME, execute

pytestmark = pytest.mark.asyncio


@asynccontextmanager
async def create_sparse_table(conn, columns):
    await execute(conn, f"DROP TABLE IF EXISTS {TABLE_NAME}")
    await execute(
        conn,
        f"""
        CREATE TABLE {TABLE_NAME} ({columns})
        ENGINE = MergeTree
        ORDER BY tuple()
        SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5
        """,
    )
    try:
        yield TABLE_NAME
    finally:
        await execute(conn, f"DROP TABLE IF EXISTS {TABLE_NAME}")


async def test_int_all_defaults(conn):
    for data in [[(0,), (0,), (0,)], [(0,)]]:
        async with create_sparse_table(conn, "a Int32") as table:
            await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

            inserted = await execute(conn, f"SELECT * FROM {table}")

        assert inserted == data


async def test_int_borders_cases(conn):
    data = [(1,), (0,), (0,), (1,), (0,), (0,), (1,)]

    async with create_sparse_table(conn, "a Int32") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_int_default_last(conn):
    data = [(1,), (0,), (0,)]

    async with create_sparse_table(conn, "a Int32") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_sparse_tuples(conn):
    data = [
        (1, (1, (1, 0))),
        (0, (0, (0, 0))),
        (0, (0, (0, 0))),
    ]

    async with create_sparse_table(conn, "a Int32, b Tuple(Int32, Tuple(Int32, Int32))") as table:
        await execute(conn, f"INSERT INTO {table} VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_sparse_dates(conn):
    data = [(date(1970, 1, 1),), (date(1970, 1, 1),)]

    async with create_sparse_table(conn, "a Date32") as table:
        await execute(conn, f"INSERT INTO {table} VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data
