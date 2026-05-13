import pytest

from tests.test_upstream.columns._helpers import create_table, execute

pytestmark = pytest.mark.asyncio

GEO_SETTINGS = {"allow_experimental_geo_types": True}


def geo_table(conn, columns):
    return create_table(conn, columns, settings=GEO_SETTINGS)


async def test_point(conn):
    data = [((1.5, 2),), ((3, 4),)]

    async with geo_table(conn, "a Point") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_ring(conn):
    data = [([(1.5, 2), (3, 4)],)]

    async with geo_table(conn, "a Ring") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_polygon(conn):
    data = [([[(1.5, 2), (3, 4)], [(5.5, 6), (7, 8)]],)]

    async with geo_table(conn, "a Polygon") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_multipolygon(conn):
    data = [
        (
            [
                [[(1.5, 2), (3, 4)], [(5.5, 6), (7, 8)]],
                [[(2.5, 3), (4, 5)], [(6.5, 7), (8, 9)]],
            ],
        )
    ]

    async with geo_table(conn, "a MultiPolygon") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data
