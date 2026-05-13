from contextlib import asynccontextmanager

import pytest

from asynch.errors import ServerException

pytestmark = pytest.mark.asyncio

TABLE_NAME = "test.upstream_query_info"
SAMPLE_QUERY = f"SELECT * FROM {TABLE_NAME} GROUP BY foo ORDER BY foo DESC LIMIT 5"


async def _execute(conn, query, params=None, settings=None):
    return await conn._connection.execute(query, args=params, settings=settings)


@asynccontextmanager
async def sample_table(conn):
    await _execute(conn, f"DROP TABLE IF EXISTS {TABLE_NAME}")
    await _execute(conn, f"CREATE TABLE {TABLE_NAME} (foo UInt8) ENGINE=Memory")
    await _execute(conn, f"INSERT INTO {TABLE_NAME} (foo) VALUES", [(i,) for i in range(42)])
    conn._connection.reset_last_query()
    try:
        yield
    finally:
        last_query = conn._connection.last_query
        await _execute(conn, f"DROP TABLE IF EXISTS {TABLE_NAME}")
        conn._connection.last_query = last_query


def assert_sample_last_query(last_query):
    assert last_query is not None
    assert last_query.profile_info is not None
    assert last_query.profile_info.rows_before_limit == 42

    assert last_query.progress is not None
    assert last_query.progress.rows == 42
    assert last_query.progress.bytes == 42
    assert last_query.progress.total_rows == 0
    assert last_query.progress.elapsed_ns > 0


async def test_default_value(conn):
    assert conn._connection.last_query is None


async def test_store_last_query_after_execute(conn):
    async with sample_table(conn):
        await _execute(conn, SAMPLE_QUERY)

    assert_sample_last_query(conn._connection.last_query)
    assert conn._connection.last_query.elapsed > 0


async def test_last_query_after_execute_iter(conn):
    async with sample_table(conn):
        result = await conn._connection.execute_iter(SAMPLE_QUERY)
        assert [row async for row in result] == [(41,), (40,), (39,), (38,), (37,)]

    assert_sample_last_query(conn._connection.last_query)
    assert conn._connection.last_query.elapsed == 0


async def test_last_query_after_execute_with_progress(conn):
    async with sample_table(conn):
        progress = await conn._connection.execute_with_progress(SAMPLE_QUERY)
        assert await progress.get_result() == [(41,), (40,), (39,), (38,), (37,)]

    assert_sample_last_query(conn._connection.last_query)
    assert conn._connection.last_query.elapsed == 0


async def test_last_query_progress_total_rows(conn):
    await _execute(conn, "SELECT number FROM numbers(10) LIMIT 10")

    last_query = conn._connection.last_query
    assert last_query is not None
    assert last_query.profile_info is not None
    assert last_query.profile_info.rows_before_limit == 10

    assert last_query.progress is not None
    assert last_query.progress.rows == 10
    assert last_query.progress.bytes == 80
    assert last_query.progress.total_rows == 10
    assert last_query.progress.elapsed_ns > 0
    assert last_query.elapsed > 0


async def test_last_query_after_execute_insert(conn):
    async with sample_table(conn):
        await _execute(conn, f"INSERT INTO {TABLE_NAME} (foo) VALUES", [(i,) for i in range(42)])

    last_query = conn._connection.last_query
    assert last_query is not None
    assert last_query.progress is not None
    assert last_query.progress.rows == 0
    assert last_query.progress.bytes == 0
    assert last_query.progress.elapsed_ns == 0
    assert last_query.elapsed > 0


async def test_override_after_subsequent_queries(conn):
    query = f"SELECT * FROM {TABLE_NAME} WHERE foo < %(i)s ORDER BY foo LIMIT 5"

    async with sample_table(conn):
        for i in range(1, 10):
            await _execute(conn, query, {"i": i})

            profile_info = conn._connection.last_query.profile_info
            assert profile_info.rows_before_limit == i


async def test_reset_last_query(conn):
    async with sample_table(conn):
        await _execute(conn, SAMPLE_QUERY)

    assert conn._connection.last_query is not None
    conn._connection.reset_last_query()
    assert conn._connection.last_query is None


async def test_reset_on_query_error(conn):
    with pytest.raises(ServerException):
        await _execute(conn, "SELECT answer FROM universe")

    assert conn._connection.last_query is None


async def test_progress_info_increment(conn):
    await _execute(
        conn,
        "SELECT x FROM (SELECT number AS x FROM numbers(100000000)) ORDER BY x ASC LIMIT 10",
    )

    last_query = conn._connection.last_query
    assert last_query is not None
    assert last_query.progress is not None
    assert last_query.progress.rows >= 100000000
    assert last_query.progress.bytes >= 800000000
    assert last_query.progress.total_rows == 100000000


async def test_progress_info_ddl(conn):
    await _execute(conn, "DROP TABLE IF EXISTS foo")

    last_query = conn._connection.last_query
    assert last_query is not None
    assert last_query.progress is not None
    assert last_query.progress.rows == 0
    assert last_query.progress.bytes == 0
    assert last_query.progress.elapsed_ns == 0
    assert last_query.elapsed > 0
