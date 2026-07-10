import asyncio
import datetime
import inspect
from unittest.mock import AsyncMock

import pytest

import asynch
from asynch import Connection
from asynch.cursors import Cursor, DictCursor
from asynch.errors import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    ServerException,
)
from asynch.errors import (
    Warning as AsynchWarning,
)

pytestmark = pytest.mark.no_clickhouse


async def _fake_connect(self):
    self._opened = True


def test_module_exposes_dbapi_metadata():
    assert asynch.apilevel == "2.0"
    assert asynch.threadsafety == 1
    assert asynch.paramstyle == "pyformat"
    assert inspect.iscoroutinefunction(asynch.connect)


@pytest.mark.asyncio
async def test_module_connect_returns_opened_connection(monkeypatch):
    monkeypatch.setattr(Connection, "connect", _fake_connect)

    conn = await asynch.connect(host="example.com", port=9440)

    assert isinstance(conn, Connection)
    assert conn.opened is True
    assert conn.host == "example.com"
    assert conn.port == 9440


def test_module_exposes_dbapi_exceptions():
    assert issubclass(AsynchWarning, Exception)
    assert issubclass(Error, Exception)
    assert issubclass(InterfaceError, Error)
    assert issubclass(DatabaseError, Error)
    assert issubclass(DataError, DatabaseError)
    assert issubclass(OperationalError, DatabaseError)
    assert issubclass(IntegrityError, DatabaseError)
    assert issubclass(InternalError, DatabaseError)
    assert issubclass(ProgrammingError, DatabaseError)
    assert issubclass(NotSupportedError, DatabaseError)
    assert issubclass(ServerException, DatabaseError)


def test_type_constructors():
    ticks = datetime.datetime(2024, 1, 2, 3, 4, 5).timestamp()

    assert asynch.Date(2024, 1, 2) == datetime.date(2024, 1, 2)
    assert asynch.Time(3, 4, 5) == datetime.time(3, 4, 5)
    assert asynch.Timestamp(2024, 1, 2, 3, 4, 5) == datetime.datetime(2024, 1, 2, 3, 4, 5)
    assert asynch.DateFromTicks(ticks) == datetime.date(2024, 1, 2)
    assert asynch.TimeFromTicks(ticks) == datetime.time(3, 4, 5)
    assert asynch.TimestampFromTicks(ticks) == datetime.datetime(2024, 1, 2, 3, 4, 5)
    assert asynch.Binary(bytearray(b"abc")) == b"abc"
    assert asynch.Binary("abc") == b"abc"


def test_type_objects_compare_to_clickhouse_type_codes():
    assert asynch.STRING == "String"
    assert asynch.STRING == "LowCardinality(String)"
    assert asynch.BINARY == "FixedString"
    assert asynch.BINARY == "FixedString(16)"
    assert asynch.NUMBER == "UInt32"
    assert asynch.NUMBER == "Nullable(UInt32)"
    assert asynch.NUMBER == "Float64"
    assert asynch.NUMBER == "Decimal(10, 2)"
    assert asynch.DATETIME == "DateTime"
    assert asynch.DATETIME == "DateTime64(3, 'UTC')"
    assert asynch.ROWID != "UInt64"


@pytest.mark.asyncio
async def test_commit_is_noop_for_non_transactional_backend():
    conn = Connection()

    assert await conn.commit() is None


@pytest.mark.asyncio
async def test_cursor_description_and_rowcount_for_empty_result_set():
    cursor = Cursor()

    cursor._begin_query()
    await cursor._process_response(([], [("x", "UInt32")]))
    cursor._end_query()

    assert cursor.description == [
        ("x", "UInt32", None, None, None, None, True),
    ]
    assert cursor.rowcount == 0
    assert await cursor.fetchall() == []


@pytest.mark.asyncio
async def test_cursor_no_result_set_response_has_no_description_and_cannot_fetch():
    cursor = Cursor()

    cursor._begin_query()
    await cursor._process_response(([], []))
    cursor._end_query()

    assert cursor.description is None
    assert cursor.columns_with_types is None
    assert cursor.rowcount == -1
    with pytest.raises(ProgrammingError):
        await cursor.fetchall()


@pytest.mark.asyncio
async def test_cursor_insert_count_has_no_description_and_cannot_fetch():
    cursor = Cursor()

    cursor._begin_query()
    await cursor._process_response(3)
    cursor._end_query()

    assert cursor.description is None
    assert cursor.rowcount == 3
    with pytest.raises(ProgrammingError):
        await cursor.fetchone()


@pytest.mark.asyncio
async def test_dict_cursor_no_result_set_raises_programming_error():
    cursor = DictCursor()

    cursor._begin_query()
    await cursor._process_response(None)
    cursor._end_query()

    assert cursor.description is None
    with pytest.raises(ProgrammingError):
        await cursor.fetchall()


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["execute", "executemany"])
async def test_cancelled_cursor_operation_leaves_cursor_reusable(method):
    conn = Connection()
    conn._connection.execute = AsyncMock(side_effect=asyncio.CancelledError)
    cursor = conn.cursor()

    with pytest.raises(asyncio.CancelledError):
        await getattr(cursor, method)("SELECT 1")

    assert cursor.status == "finished"
