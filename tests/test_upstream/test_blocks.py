import logging
from contextlib import contextmanager
from io import StringIO

import pytest

from asynch.connection import Connection
from asynch.errors import ServerException

pytestmark = pytest.mark.asyncio


async def _execute(conn, query, args=None, settings=None, **kwargs):
    return await conn._connection.execute(query, args=args, settings=settings, **kwargs)


async def _drop_table(conn, table_name):
    await _execute(conn, f"DROP TABLE IF EXISTS {table_name}")


async def _create_table(conn, table_name, columns):
    await _execute(conn, f"CREATE TABLE {table_name} ({columns}) ENGINE=Memory")


@contextmanager
def captured_connection_logs():
    buffer = StringIO()
    handler = logging.StreamHandler(buffer)
    logger = logging.getLogger("asynch.proto.connection")
    original_level = logger.level
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    try:
        yield buffer
    finally:
        logger.removeHandler(handler)
        logger.setLevel(original_level)


async def test_return_totals_extremes(conn):
    rv = await _execute(
        conn,
        "SELECT a, sum(b + a) FROM ("
        "SELECT arrayJoin(range(3)) - 1 AS a,"
        "arrayJoin(range(4)) AS b"
        ") AS t "
        "GROUP BY a WITH TOTALS "
        "ORDER BY a",
        settings={"extremes": 1},
    )

    assert rv == [
        (-1, 2),
        (0, 6),
        (1, 10),
        (0, 18),
        (-1, 2),
        (1, 10),
    ]


async def test_columnar_result(conn):
    rv = await _execute(
        conn,
        "SELECT a, sum(b + a) FROM ("
        "SELECT arrayJoin(range(3)) - 1 AS a,"
        "arrayJoin(range(4)) AS b"
        ") AS t "
        "GROUP BY a "
        "ORDER BY a",
        columnar=True,
    )

    assert rv == [(-1, 0, 1), (2, 6, 10)]


async def test_columnar_block_extend(conn):
    table_name = "test.upstream_blocks_columnar"
    await _drop_table(conn, table_name)
    await _create_table(conn, table_name, "a Int32")
    try:
        await _execute(conn, f"INSERT INTO {table_name} (a) VALUES", [(1,)])
        await _execute(conn, f"INSERT INTO {table_name} (a) VALUES", [(2,)])

        inserted = await _execute(
            conn,
            f"SELECT * FROM {table_name} ORDER BY a",
            columnar=True,
        )
        assert inserted == [(1, 2)]
    finally:
        await _drop_table(conn, table_name)


async def test_select_with_column_types(conn):
    rv = await _execute(
        conn,
        "SELECT CAST(1 AS Int32) AS x",
        with_column_types=True,
    )

    assert rv == ([(1,)], [("x", "Int32")])


async def test_select_with_columnar_with_column_types(conn):
    progress = await conn._connection.execute_with_progress(
        "SELECT arrayJoin(A) -1 as j,"
        "arrayJoin(A)+1 as k FROM("
        "SELECT range(3) as A)",
        columnar=True,
        with_column_types=True,
    )

    rv = await progress.get_result()

    assert rv == ([(-1, 0, 1), (1, 2, 3)], [("j", "Int16"), ("k", "UInt16")])


async def test_select_with_progress(conn):
    progress = await conn._connection.execute_with_progress("SELECT 2")

    values = [item async for item in progress]

    assert values in [[(1, 0)], [(1, 0), (1, 0)]]
    assert await progress.get_result() == [(2,)]
    assert conn._connection.connected


async def test_progress_totals(conn):
    progress = await conn._connection.execute_with_progress("SELECT 2")
    assert progress.progress_totals.rows == 0
    assert progress.progress_totals.bytes == 0
    assert progress.progress_totals.total_rows == 0

    assert await progress.get_result() == [(2,)]

    assert progress.progress_totals.rows == 1
    assert progress.progress_totals.bytes == 1
    assert progress.progress_totals.total_rows == 0


async def test_select_with_progress_error(conn):
    progress = await conn._connection.execute_with_progress("SELECT error")

    with pytest.raises(ServerException):
        async for _ in progress:
            pass

    assert not conn._connection.connected


async def test_select_with_progress_no_progress_unwind(conn):
    progress = await conn._connection.execute_with_progress("SELECT 2")

    assert await progress.get_result() == [(2,)]
    assert conn._connection.connected


async def test_select_with_progress_with_params(conn):
    progress = await conn._connection.execute_with_progress("SELECT %(x)s", args={"x": 2})

    assert await progress.get_result() == [(2,)]
    assert conn._connection.connected


async def test_select_with_iter(conn):
    result = await conn._connection.execute_iter("SELECT number FROM system.numbers LIMIT 10")

    assert [row async for row in result] == list(zip(range(10)))
    assert [row async for row in result] == []


async def test_select_with_iter_with_column_types(conn):
    result = await conn._connection.execute_iter(
        "SELECT CAST(number AS UInt32) as number "
        "FROM system.numbers LIMIT 10",
        with_column_types=True,
    )

    assert [row async for row in result] == [[("number", "UInt32")]] + list(zip(range(10)))
    assert [row async for row in result] == []


async def test_select_with_iter_error(conn):
    result = await conn._connection.execute_iter("SELECT error")

    with pytest.raises(ServerException):
        async for _ in result:
            pass

    assert not conn._connection.connected


async def test_logs(conn):
    query = "SELECT 1"
    with captured_connection_logs() as buffer:
        await _execute(conn, query, settings={"send_logs_level": "debug"})

    assert query in buffer.getvalue()


async def test_logs_insert(conn):
    table_name = "test.upstream_blocks_logs"
    await _drop_table(conn, table_name)
    await _create_table(conn, table_name, "a Int32")
    try:
        query = f"INSERT INTO {table_name} (a) VALUES"
        with captured_connection_logs() as buffer:
            await _execute(conn, query, [(1,)], settings={"send_logs_level": "debug"})

        assert query in buffer.getvalue()
        assert await _execute(conn, "SELECT 1", settings={"send_logs_level": "debug"}) == [(1,)]
    finally:
        await _drop_table(conn, table_name)


async def test_logs_with_compression(config):
    async with Connection(dsn=config.dsn, compression="lz4") as conn:
        query = "SELECT 1"
        with captured_connection_logs() as buffer:
            await _execute(conn, query, settings={"send_logs_level": "debug"})

    assert query in buffer.getvalue()
