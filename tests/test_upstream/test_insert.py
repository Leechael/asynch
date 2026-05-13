from contextlib import asynccontextmanager
from datetime import date

import pytest

from asynch.errors import ServerException, TypeMismatchError

pytestmark = pytest.mark.asyncio


TABLE_NAME = "test.upstream_insert"


async def _execute(conn, query, args=None, **kwargs):
    return await conn._connection.execute(query, args=args, **kwargs)


@asynccontextmanager
async def create_table(conn, columns):
    await _execute(conn, f"DROP TABLE IF EXISTS {TABLE_NAME}")
    await _execute(conn, f"CREATE TABLE {TABLE_NAME} ({columns}) ENGINE=Memory")
    try:
        yield
    finally:
        await _execute(conn, f"DROP TABLE IF EXISTS {TABLE_NAME}")


async def test_type_mismatch(conn):
    async with create_table(conn, "a Float32"):
        with pytest.raises(TypeMismatchError):
            await _execute(
                conn,
                f"INSERT INTO {TABLE_NAME} (a) VALUES",
                [(date(2012, 10, 25),)],
            )


async def test_no_such_column(conn):
    async with create_table(conn, "a Float32"):
        with pytest.raises(ServerException):
            await _execute(conn, f"INSERT INTO {TABLE_NAME} (b) VALUES", [(1,)])


async def test_data_malformed_rows(conn):
    async with create_table(conn, "a Int8"):
        with pytest.raises(TypeError):
            await _execute(conn, f"INSERT INTO {TABLE_NAME} (a) VALUES", [1])


async def test_data_less_columns_then_expected(conn):
    async with create_table(conn, "a Int8, b Int8"):
        with pytest.raises(ValueError) as exc:
            await _execute(conn, f"INSERT INTO {TABLE_NAME} (a, b) VALUES", [(1,)])

    assert str(exc.value) == "Expected 2 columns, got 1"


async def test_data_more_columns_then_expected(conn):
    async with create_table(conn, "a Int8, b Int8"):
        with pytest.raises(ValueError) as exc:
            await _execute(conn, f"INSERT INTO {TABLE_NAME} (a, b) VALUES", [(1, 2, 3)])

    assert str(exc.value) == "Expected 2 columns, got 3"


async def test_data_different_rows_length(conn):
    async with create_table(conn, "a Int8, b Int8"):
        with pytest.raises(ValueError) as exc:
            await _execute(conn, f"INSERT INTO {TABLE_NAME} (a, b) VALUES", [(1, 2), (3,)])

    assert str(exc.value) == "Different rows length"


async def test_data_different_rows_length_from_dicts(conn):
    async with create_table(conn, "a Int8, b Int8"):
        with pytest.raises(KeyError):
            await _execute(
                conn,
                f"INSERT INTO {TABLE_NAME} (a, b) VALUES",
                [{"a": 1, "b": 2}, {"a": 3}],
            )


async def test_data_unsupported_row_type(conn):
    async with create_table(conn, "a Int8"):
        with pytest.raises(TypeError) as exc:
            await _execute(
                conn,
                f"INSERT INTO {TABLE_NAME} (a) VALUES",
                [1],
                types_check=True,
            )

    assert "dict, list or tuple is expected" in str(exc.value)


async def test_data_dicts_ok(conn):
    async with create_table(conn, "a Int8, b Int8"):
        await _execute(
            conn,
            f"INSERT INTO {TABLE_NAME} (a, b) VALUES",
            [{"a": 1, "b": 2}, {"a": 3, "b": 4}],
        )

        inserted = await _execute(conn, f"SELECT * FROM {TABLE_NAME}")
        assert inserted == [(1, 2), (3, 4)]


async def test_data_generator_type(conn):
    async with create_table(conn, "a Int8"):
        data = ((x,) for x in range(3))
        await _execute(conn, f"INSERT INTO {TABLE_NAME} (a) VALUES", data)

        inserted = await _execute(conn, f"SELECT * FROM {TABLE_NAME}")
        assert inserted == [(0,), (1,), (2,)]


async def test_data_dicts_mixed_with_lists(conn):
    async with create_table(conn, "a Int8"):
        with pytest.raises(TypeError) as exc:
            await _execute(
                conn,
                f"INSERT INTO {TABLE_NAME} (a) VALUES",
                [{"a": 1}, (2,)],
                types_check=True,
            )
        assert "dict is expected" in str(exc.value)

        with pytest.raises(TypeError) as exc:
            await _execute(
                conn,
                f"INSERT INTO {TABLE_NAME} (a) VALUES",
                [(1,), {"a": 2}],
                types_check=True,
            )
        assert "list or tuple is expected" in str(exc.value)


async def test_empty_insert(conn):
    async with create_table(conn, "a Int8"):
        await _execute(conn, f"INSERT INTO {TABLE_NAME} (a) VALUES", [])

        inserted = await _execute(conn, f"SELECT * FROM {TABLE_NAME}")
        assert inserted == []


async def test_insert_from_select(conn):
    async with create_table(conn, "a UInt64"):
        inserted = await _execute(
            conn,
            f"INSERT INTO {TABLE_NAME} (a) SELECT number FROM system.numbers LIMIT 5",
        )

    assert inserted == []


async def test_insert_return(conn):
    async with create_table(conn, "a Int8"):
        rv = await _execute(conn, f"INSERT INTO {TABLE_NAME} (a) VALUES", [])
        assert rv == 0

        rv = await _execute(
            conn,
            f"INSERT INTO {TABLE_NAME} (a) VALUES",
            [(x,) for x in range(5)],
        )
        assert rv == 5


async def test_insert_from_input(conn):
    async with create_table(conn, "a Int8"):
        await _execute(
            conn,
            f"INSERT INTO {TABLE_NAME} (a) SELECT a FROM input ('a Int8') FORMAT Native",
            [{"a": 1}],
            settings={"session_timezone": "UTC"},
        )

        inserted = await _execute(conn, f"SELECT * FROM {TABLE_NAME}")
        assert inserted == [(1,)]


async def test_profile_events(conn):
    async with create_table(conn, "x Int32"):
        await _execute(conn, f"INSERT INTO {TABLE_NAME} (x) VALUES", [{"x": 1}])


async def test_insert_tuple_ok_columnar(conn):
    async with create_table(conn, "a Int8, b Int8"):
        data = [(1, 2, 3), (4, 5, 6)]
        await _execute(
            conn,
            f"INSERT INTO {TABLE_NAME} (a, b) VALUES",
            data,
            columnar=True,
        )

        inserted = await _execute(conn, f"SELECT * FROM {TABLE_NAME}")
        assert inserted == [(1, 4), (2, 5), (3, 6)]
        inserted = await _execute(conn, f"SELECT * FROM {TABLE_NAME}", columnar=True)
        assert inserted == [(1, 2, 3), (4, 5, 6)]


async def test_insert_data_different_column_length(conn):
    async with create_table(conn, "a Int8, b Int8"):
        with pytest.raises(ValueError) as exc:
            await _execute(
                conn,
                f"INSERT INTO {TABLE_NAME} (a, b) VALUES",
                [(1, 2, 3), (4, 5)],
                columnar=True,
            )

    assert str(exc.value) == "Expected 3 rows, got 2"


async def test_columnar_data_less_columns_then_expected(conn):
    async with create_table(conn, "a Int8, b Int8"):
        with pytest.raises(ValueError) as exc:
            await _execute(
                conn,
                f"INSERT INTO {TABLE_NAME} (a, b) VALUES",
                [(1, 2)],
                columnar=True,
            )

    assert str(exc.value) == "Expected 2 columns, got 1"


async def test_columnar_data_more_columns_then_expected(conn):
    async with create_table(conn, "a Int8, b Int8"):
        with pytest.raises(ValueError) as exc:
            await _execute(
                conn,
                f"INSERT INTO {TABLE_NAME} (a, b) VALUES",
                [(1, 2), (3, 4), (5, 6)],
                columnar=True,
            )

    assert str(exc.value) == "Expected 2 columns, got 3"


async def test_columnar_data_invalid_types(conn):
    async with create_table(conn, "a Int8, b Int8"):
        with pytest.raises(TypeError) as exc:
            await _execute(
                conn,
                f"INSERT INTO {TABLE_NAME} (a, b) VALUES",
                [(1, 2), {"a": 3, "b": 4}],
                types_check=True,
                columnar=True,
            )
        assert "list or tuple is expected" in str(exc.value)

        with pytest.raises(TypeError) as exc:
            await _execute(
                conn,
                f"INSERT INTO {TABLE_NAME} (a, b) VALUES",
                [(1, 2), 3],
                types_check=True,
                columnar=True,
            )
        assert "list or tuple is expected" in str(exc.value)
