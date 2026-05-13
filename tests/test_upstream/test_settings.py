import pytest

from asynch.connection import Connection
from asynch.errors import ErrorCode, ServerException

pytestmark = pytest.mark.asyncio


def _connection_kwargs(config, settings=None, **kwargs):
    connection_settings = config.settings.copy()
    if settings:
        connection_settings.update(settings)

    return {
        "host": config.host,
        "port": config.port,
        "database": config.database,
        "user": config.user,
        "password": config.password,
        "settings": connection_settings,
        **kwargs,
    }


async def _execute(connection, query, args=None, settings=None):
    return await connection._connection.execute(query, args=args, settings=settings)


async def _drop_table(connection, table_name):
    await _execute(connection, f"DROP TABLE IF EXISTS {table_name}")


async def _create_table(connection, table_name, columns):
    await _execute(connection, f"CREATE TABLE {table_name} ({columns}) ENGINE=Memory")


async def test_settings_immutable(conn):
    settings = {"strings_encoding": "utf-8"}

    await _execute(conn, "SELECT 1", settings=settings)

    assert settings == {"strings_encoding": "utf-8"}


@pytest.mark.parametrize(
    "settings, expected",
    [
        ({"max_query_size": 142}, [("max_query_size", "142", 1)]),
        ({"totals_auto_threshold": 1.23}, [("totals_auto_threshold", "1.23", 1)]),
        ({"force_index_by_date": 1}, [("force_index_by_date", "1", 1)]),
        ({"format_csv_delimiter": "d"}, [("format_csv_delimiter", "d", 1)]),
    ],
    ids=["int", "float", "bool", "char"],
)
async def test_settings_apply(conn, settings, expected):
    setting_name = next(iter(settings))

    rv = await _execute(
        conn,
        "SELECT name, value, changed FROM system.settings "
        f"WHERE name = '{setting_name}'",
        settings=settings,
    )

    assert rv == expected


async def test_max_threads_apply(conn):
    rv = await _execute(
        conn,
        "SELECT name, value, changed FROM system.settings "
        "WHERE name = 'max_threads'",
        settings={"max_threads": 42},
    )

    assert rv == [("max_threads", "42", 1)]

    await _execute(
        conn,
        "SELECT name, value, changed FROM system.settings "
        "WHERE name = 'max_threads'",
        settings={"max_threads": "auto"},
    )


async def test_unknown_setting(conn):
    rv = await _execute(conn, "SELECT 1", settings={"unknown_setting": 100500})

    assert rv == [(1,)]


async def test_unknown_setting_is_important(config):
    async with Connection(**_connection_kwargs(config, settings_is_important=True)) as conn:
        with pytest.raises(ServerException) as exc:
            await _execute(conn, "SELECT 1", settings={"unknown_setting": 100500})

    assert exc.value.code == ErrorCode.UNKNOWN_SETTING


async def test_client_settings(config):
    async with Connection(**_connection_kwargs(config, {"max_query_size": 142})) as conn:
        rv = await _execute(
            conn,
            "SELECT name, value, changed FROM system.settings "
            "WHERE name = 'max_query_size'",
        )

    assert rv == [("max_query_size", "142", 1)]


async def test_query_settings_override_client_settings(config):
    async with Connection(**_connection_kwargs(config, {"max_query_size": 142})) as conn:
        rv = await _execute(
            conn,
            "SELECT name, value, changed FROM system.settings "
            "WHERE name = 'max_query_size'",
            settings={"max_query_size": 242},
        )

    assert rv == [("max_query_size", "242", 1)]


@pytest.mark.parametrize(
    "spec, data, expected",
    [
        ("a Int8, b String", [(None, None)], [(0, "")]),
        ("a LowCardinality(String)", [(None,)], [("",)]),
        ("a Tuple(Int32, Int32)", [(None,)], [((0, 0),)]),
        ("a Array(Array(Int32))", [(None,)], [([],)]),
        ("a Map(String, UInt64)", [(None,)], [({},)]),
        ("a Nested(i Int32)", [(None,)], [([],)]),
    ],
    ids=[
        "int_and_string",
        "low_cardinality_string",
        "tuple",
        "array",
        "map",
        "nested",
    ],
)
async def test_input_format_null_as_default(config, spec, data, expected):
    table_name = "test.upstream_settings_input_null_as_default"

    async with Connection(
        **_connection_kwargs(config, {"input_format_null_as_default": True})
    ) as conn:
        await _drop_table(conn, table_name)
        await _create_table(conn, table_name, spec)
        try:
            await _execute(conn, f"INSERT INTO {table_name} VALUES", data)

            inserted = await _execute(conn, f"SELECT * FROM {table_name}")
            assert inserted == expected
        finally:
            await _drop_table(conn, table_name)


async def test_max_result_rows_apply(conn):
    query = "SELECT number FROM system.numbers LIMIT 10"

    with pytest.raises(ServerException) as exc:
        await _execute(conn, query, settings={"max_result_rows": 5})
    assert exc.value.code in {
        ErrorCode.TOO_MANY_ROWS_OR_BYTES,
        ErrorCode.TOO_MANY_ROWS,
    }

    rv = await _execute(
        conn,
        query,
        settings={"max_result_rows": 5, "result_overflow_mode": "break"},
    )
    assert len(rv) == 10

    rv = await _execute(conn, query)
    assert len(rv) == 10
