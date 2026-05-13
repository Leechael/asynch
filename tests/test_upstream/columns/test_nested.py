import pytest

from tests.test_upstream.columns._helpers import TABLE_NAME, create_table, execute

pytestmark = pytest.mark.asyncio


async def set_flatten_nested(conn, value):
    await execute(conn, f"SET flatten_nested = {value}")


async def test_simple(conn):
    data = [([(0, "a"), (1, "b")],)]

    await set_flatten_nested(conn, 0)
    try:
        async with create_table(conn, "n Nested(i Int32, s String)") as table:
            await execute(conn, f"INSERT INTO {table} (n) VALUES", data)

            inserted = await execute(conn, f"SELECT * FROM {table}")
            projected_i = await execute(conn, f"SELECT n.i FROM {table}")
            projected_s = await execute(conn, f"SELECT n.s FROM {table}")
    finally:
        await set_flatten_nested(conn, 1)

    assert inserted == data
    assert projected_i == [([0, 1],)]
    assert projected_s == [(["a", "b"],)]


async def test_multiple_rows(conn):
    data = [([(0, "a"), (1, "b")],), ([(3, "d"), (4, "e")],)]

    await set_flatten_nested(conn, 0)
    try:
        async with create_table(conn, "n Nested(i Int32, s String)") as table:
            await execute(conn, f"INSERT INTO {table} (n) VALUES", data)

            inserted = await execute(conn, f"SELECT * FROM {table}")
    finally:
        await set_flatten_nested(conn, 1)

    assert inserted == data


async def test_dict(conn):
    data = [
        {"n": [{"i": 0, "s": "a"}, {"i": 1, "s": "b"}]},
        {"n": [{"i": 3, "s": "d"}, {"i": 4, "s": "e"}]},
    ]

    await set_flatten_nested(conn, 0)
    try:
        async with create_table(conn, "n Nested(i Int32, s String)") as table:
            await execute(conn, f"INSERT INTO {table} (n) VALUES", data)

            inserted = await execute(conn, f"SELECT * FROM {table}")
    finally:
        await set_flatten_nested(conn, 1)

    assert inserted == [([(0, "a"), (1, "b")],), ([(3, "d"), (4, "e")],)]


async def test_nested_side_effect_as_json(conn):
    data = [([(0, "a"), (1, "b")],)]

    await set_flatten_nested(conn, 0)
    try:
        async with create_table(conn, "n Nested(i Int32, s String)") as table:
            await execute(conn, f"INSERT INTO {TABLE_NAME} (n) VALUES", data)

            inserted = await execute(
                conn,
                f"SELECT * FROM {table}",
                settings={"allow_experimental_object_type": True},
            )
    finally:
        await set_flatten_nested(conn, 1)

    assert inserted == [([{"i": 0, "s": "a"}, {"i": 1, "s": "b"}],)]
