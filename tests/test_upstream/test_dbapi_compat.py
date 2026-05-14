from contextlib import asynccontextmanager

import pytest

from asynch.connection import Connection
from asynch.cursors import DictCursor
from asynch.errors import InterfaceError, ProgrammingError, ServerException

pytestmark = pytest.mark.asyncio

TABLE_NAME = "test.upstream_dbapi"


@asynccontextmanager
async def create_table(conn, columns):
    async with conn.cursor() as cursor:
        await cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
        await cursor.execute(f"CREATE TABLE {TABLE_NAME} ({columns}) ENGINE=Memory")
    try:
        yield TABLE_NAME
    finally:
        async with conn.cursor() as cursor:
            await cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")


async def test_simple(conn):
    async with conn.cursor() as cursor:
        rv = await cursor.execute("SELECT 1")
        assert rv == 1
        assert await cursor.fetchall() == [(1,)]


async def test_from_dsn(config):
    async with Connection(dsn=config.dsn) as connection:
        async with connection.cursor() as cursor:
            rv = await cursor.execute("SELECT 1")
            assert rv == 1
            assert await cursor.fetchall() == [(1,)]


async def test_connect_default_params(config):
    async with Connection(host=config.host, port=config.port) as connection:
        async with connection.cursor() as cursor:
            rv = await cursor.execute("SELECT 1")
            assert rv == 1
            assert await cursor.fetchall() == [(1,)]


async def test_execute_fetchone(conn):
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT number FROM system.numbers LIMIT 4")

        assert isinstance(cursor._rows, list)
        assert await cursor.fetchone() == (0,)
        assert await cursor.fetchone() == (1,)
        assert await cursor.fetchone() == (2,)
        assert await cursor.fetchone() == (3,)
        assert await cursor.fetchone() is None


async def test_execute_fetchmany(conn):
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT number FROM system.numbers LIMIT 4")

        assert isinstance(cursor._rows, list)
        assert await cursor.fetchmany() == [(0,)]
        assert await cursor.fetchmany(None) == [(1,)]
        assert await cursor.fetchmany(0) == []
        assert await cursor.fetchmany(-1) == [(2,), (3,)]

        await cursor.execute("SELECT number FROM system.numbers LIMIT 4")
        assert await cursor.fetchmany(1) == [(0,)]
        assert await cursor.fetchmany(2) == [(1,), (2,)]
        assert await cursor.fetchmany(3) == [(3,)]
        assert await cursor.fetchmany(3) == []

        cursor.arraysize = 2
        await cursor.execute("SELECT number FROM system.numbers LIMIT 4")
        assert await cursor.fetchmany() == [(0,), (1,)]
        assert await cursor.fetchmany() == [(2,), (3,)]


async def test_execute_fetchall(conn):
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT number FROM system.numbers LIMIT 4")
        assert cursor.rowcount == 4
        assert await cursor.fetchall() == [(0,), (1,), (2,), (3,)]


async def test_streaming_fetchone(conn):
    async with conn.cursor() as cursor:
        cursor.set_stream_results(True, 2)
        await cursor.execute("SELECT number FROM system.numbers LIMIT 4")

        assert await cursor.fetchone() == (0,)
        assert await cursor.fetchone() == (1,)
        assert await cursor.fetchone() == (2,)
        assert await cursor.fetchone() == (3,)
        assert await cursor.fetchone() is None


async def test_streaming_fetchmany(conn):
    async with conn.cursor() as cursor:
        cursor.set_stream_results(True, 2)
        await cursor.execute("SELECT number FROM system.numbers LIMIT 4")

        assert await cursor.fetchmany() == [(0,)]
        assert await cursor.fetchmany(None) == [(1,)]
        assert await cursor.fetchmany(0) == []
        assert await cursor.fetchmany(-1) == [(2,), (3,)]

        await cursor.execute("SELECT number FROM system.numbers LIMIT 4")
        assert await cursor.fetchmany(1) == [(0,)]
        assert await cursor.fetchmany(2) == [(1,), (2,)]
        assert await cursor.fetchmany(3) == [(3,)]
        assert await cursor.fetchmany(3) == []


async def test_streaming_fetchall(conn):
    async with conn.cursor() as cursor:
        cursor.set_stream_results(True, 2)
        await cursor.execute("SELECT number FROM system.numbers LIMIT 4")

        assert cursor.rowcount == -1
        assert await cursor.fetchall() == [(0,), (1,), (2,), (3,)]
        assert cursor.rowcount == -1


async def test_executemany(conn):
    async with create_table(conn, "a UInt32") as table:
        async with conn.cursor() as cursor:
            data = [(0,), (1,), (2,)]
            rv = await cursor.executemany(f"INSERT INTO {table} VALUES", data)
            assert rv == 3
            assert cursor.rowcount == 3

            await cursor.execute(f"SELECT * FROM {table}")
            assert await cursor.fetchall() == data


async def test_exception_execute(config):
    connection = Connection(
        host=config.host,
        port=config.port,
        database=config.database,
        user="wrong_user",
        password=config.password,
    )
    cursor = connection.cursor()
    try:
        with pytest.raises(ServerException) as exc:
            await cursor.execute("SELECT 1")
        assert "Code: 516" in str(exc.value) or "Code: 192" in str(exc.value)
    finally:
        await connection.close()


async def test_exception_executemany(config):
    connection = Connection(
        host=config.host,
        port=config.port,
        database=config.database,
        user="wrong_user",
        password=config.password,
    )
    cursor = connection.cursor()
    try:
        with pytest.raises(ServerException) as exc:
            await cursor.executemany("INSERT INTO test VALUES", [(0,)])
        assert "Code: 516" in str(exc.value) or "Code: 192" in str(exc.value)
        assert cursor.rowcount == -1
    finally:
        await connection.close()


async def test_rowcount_insert_from_select(conn):
    async with create_table(conn, "a UInt8") as table:
        async with conn.cursor() as cursor:
            await cursor.execute(f"INSERT INTO {table} SELECT number FROM system.numbers LIMIT 4")
            assert cursor.rowcount == -1


async def test_execute_insert(conn):
    async with create_table(conn, "a UInt8") as table:
        async with conn.cursor() as cursor:
            rowcount = await cursor.execute(f"INSERT INTO {table} VALUES", [[4]])
            assert rowcount == 1
            assert cursor.rowcount == 1


async def test_description(conn):
    async with conn.cursor() as cursor:
        assert cursor.description is None
        await cursor.execute("SELECT CAST(1 AS UInt32) AS test")
        desc = cursor.description
        assert len(desc) == 1
        assert desc[0].name == "test"
        assert desc[0].type_code == "UInt32"


async def test_pep249_sizes(conn):
    async with conn.cursor() as cursor:
        cursor.setinputsizes(0)
        cursor.setoutputsize(0)


async def test_ddl(conn):
    async with conn.cursor() as cursor:
        await cursor.execute("DROP TABLE IF EXISTS test.upstream_dbapi_ddl")
        assert cursor.description is None
        assert cursor.rowcount == -1
        with pytest.raises(ProgrammingError):
            await cursor.fetchall()


async def test_cursor_repr(conn):
    async with conn.cursor() as cursor:
        assert "status: ready" in repr(cursor)


async def test_connection_repr(conn):
    assert "status: opened" in repr(conn)


async def test_columns_with_types_select(conn):
    async with conn.cursor() as cursor:
        assert cursor.columns_with_types is None
        await cursor.execute("SELECT CAST(number AS UInt64) AS x FROM system.numbers LIMIT 4")
        await cursor.fetchall()
        assert cursor.columns_with_types == [("x", "UInt64")]


async def test_columns_with_types_insert(conn):
    async with create_table(conn, "a UInt8") as table:
        async with conn.cursor() as cursor:
            await cursor.executemany(f"INSERT INTO {table} (a) VALUES", [(123,)])
            assert cursor.columns_with_types is None


async def test_columns_with_types_streaming(conn):
    async with conn.cursor() as cursor:
        cursor.set_stream_results(True, 2)
        await cursor.execute("SELECT CAST(number AS UInt64) AS x FROM system.numbers LIMIT 4")
        assert cursor.columns_with_types == [("x", "UInt64")]
        assert [row async for row in cursor] == [(0,), (1,), (2,), (3,)]
        assert cursor.columns_with_types == [("x", "UInt64")]


async def test_set_external_tables(conn):
    async with conn.cursor() as cursor:
        data = [(0,), (1,), (2,)]
        cursor.set_external_table("table1", [("x", "UInt32")], data)
        await cursor.execute("SELECT * FROM table1")
        assert await cursor.fetchall() == data


async def test_settings(conn):
    async with conn.cursor() as cursor:
        cursor.set_settings({"max_threads": 42})
        await cursor.execute(
            "SELECT name, value, changed FROM system.settings WHERE name = 'max_threads'"
        )
        assert await cursor.fetchall() == [("max_threads", "42", 1)]


async def test_set_query_id(conn):
    async with conn.cursor() as cursor:
        query_id = "my_query_id"
        cursor.set_query_id(query_id)
        await cursor.execute(
            "SELECT query_id FROM system.processes WHERE query_id = %(query_id)s",
            {"query_id": query_id},
        )
        assert await cursor.fetchall() == [(query_id,)]


async def test_cursor_iteration(conn):
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT number FROM system.numbers LIMIT 4")
        assert [row async for row in cursor] == [(0,), (1,), (2,), (3,)]


async def test_context_managers(conn):
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT 1")
        assert await cursor.fetchall() == [(1,)]


async def test_execute_after_close(conn):
    async with conn.cursor() as cursor:
        await cursor.close()
        with pytest.raises(InterfaceError):
            await cursor.execute("SELECT 1")


async def test_execute_fetch_before_query(conn):
    async with conn.cursor() as cursor:
        with pytest.raises(ProgrammingError):
            await cursor.fetchall()


async def test_dict_cursor_execute_fetchone(conn):
    async with conn.cursor(cursor=DictCursor) as cursor:
        await cursor.execute("SELECT number FROM system.numbers LIMIT 4")

        assert isinstance(cursor._rows, list)
        assert await cursor.fetchone() == {"number": 0}
        assert await cursor.fetchone() == {"number": 1}
        assert await cursor.fetchone() == {"number": 2}
        assert await cursor.fetchone() == {"number": 3}
        assert await cursor.fetchone() is None


async def test_dict_cursor_execute_fetchmany(conn):
    async with conn.cursor(cursor=DictCursor) as cursor:
        await cursor.execute("SELECT number FROM system.numbers LIMIT 4")

        assert isinstance(cursor._rows, list)
        assert await cursor.fetchmany() == [{"number": 0}]
        assert await cursor.fetchmany(None) == [{"number": 1}]
        assert await cursor.fetchmany(0) == []
        assert await cursor.fetchmany(-1) == [{"number": 2}, {"number": 3}]

        await cursor.execute("SELECT number FROM system.numbers LIMIT 4")
        assert await cursor.fetchmany(1) == [{"number": 0}]
        assert await cursor.fetchmany(2) == [{"number": 1}, {"number": 2}]
        assert await cursor.fetchmany(3) == [{"number": 3}]
        assert await cursor.fetchmany(3) == []

        cursor.arraysize = 2
        await cursor.execute("SELECT number FROM system.numbers LIMIT 4")
        assert await cursor.fetchmany() == [{"number": 0}, {"number": 1}]
        assert await cursor.fetchmany() == [{"number": 2}, {"number": 3}]


async def test_dict_cursor_execute_fetchall(conn):
    async with conn.cursor(cursor=DictCursor) as cursor:
        await cursor.execute("SELECT number FROM system.numbers LIMIT 4")
        assert cursor.rowcount == 4
        assert await cursor.fetchall() == [
            {"number": 0},
            {"number": 1},
            {"number": 2},
            {"number": 3},
        ]
