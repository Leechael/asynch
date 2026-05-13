import json

import pytest

from tests.test_upstream.columns._helpers import create_table, execute

pytestmark = pytest.mark.asyncio

JSON_SETTINGS = {"allow_experimental_object_type": True}


def json_table(conn, columns):
    return create_table(conn, columns, settings=JSON_SETTINGS)


async def test_simple(conn):
    inserted = await execute(
        conn,
        """
        SELECT '{"bb": {"cc": [255, 1]}}'::Object('json')
        """,
        settings=JSON_SETTINGS,
    )

    assert inserted == [({"bb": {"cc": [255, 1]}},)]


async def test_from_table(conn):
    data = [
        ({},),
        ({"key1": 1},),
        ({"key1": 2.1, "key2": {"nested": "key"}},),
        ({"key1": 3, "key3": ["test"], "key4": [10, 20]},),
    ]

    async with json_table(conn, "a Object('json')") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}", settings=JSON_SETTINGS)

    assert inserted == [
        ({"key1": 0, "key2": {"nested": ""}, "key3": [], "key4": []},),
        ({"key1": 1, "key2": {"nested": ""}, "key3": [], "key4": []},),
        ({"key1": 2.1, "key2": {"nested": "key"}, "key3": [], "key4": []},),
        ({"key1": 3, "key2": {"nested": ""}, "key3": ["test"], "key4": [10, 20]},),
    ]


async def test_insert_json_strings(conn):
    data = [(json.dumps({"i-am": "dumped json"}),)]

    async with json_table(conn, "a Object('json')") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}", settings=JSON_SETTINGS)

    assert inserted == [({"`i-am`": "dumped json"},)]


async def test_json_as_named_tuple(conn):
    data = [({"key": "value"},)]

    async with json_table(conn, "a Object('json')") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted_as_json = await execute(conn, f"SELECT * FROM {table}", settings=JSON_SETTINGS)
        inserted_as_tuple = await execute(
            conn,
            f"SELECT * FROM {table}",
            settings={**JSON_SETTINGS, "namedtuple_as_json": False},
        )

    assert inserted_as_json == data
    assert inserted_as_tuple == [(("value",),)]
