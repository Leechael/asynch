import pytest

pytestmark = pytest.mark.asyncio


async def _execute(conn, query, external_tables=None):
    return await conn._connection.execute(query, external_tables=external_tables)


async def test_select(conn):
    tables = [
        {
            "name": "test",
            "structure": [("x", "Int32"), ("y", "Array(Int32)")],
            "data": [
                {"x": 100, "y": [2, 4, 6, 8]},
                {"x": 500, "y": [1, 3, 5, 7]},
            ],
        }
    ]

    rv = await _execute(conn, "SELECT * FROM test", external_tables=tables)

    assert rv == [(100, [2, 4, 6, 8]), (500, [1, 3, 5, 7])]


async def test_send_empty_table(conn):
    tables = [
        {
            "name": "test",
            "structure": [("x", "Int32")],
            "data": [],
        }
    ]

    rv = await _execute(conn, "SELECT * FROM test", external_tables=tables)

    assert rv == []


async def test_send_empty_table_structure(conn):
    tables = [
        {
            "name": "test",
            "structure": [],
            "data": [],
        }
    ]

    with pytest.raises(ValueError) as exc:
        await _execute(conn, "SELECT * FROM test", external_tables=tables)

    assert 'Empty table "test" structure' in str(exc.value)
