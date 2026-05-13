import os
import time as time_module
from contextlib import contextmanager
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum, IntEnum
from types import SimpleNamespace
from uuid import UUID

import pytest
from pytz import timezone, utc

from asynch.proto.connection import Connection as ProtoConnection

SINGLE_TPL = "SELECT %(x)s"
DOUBLE_TPL = "SELECT %(x)s, %(y)s"


@contextmanager
def patch_env_tz(tz_name: str):
    old_tz = os.environ.get("TZ")
    os.environ["TZ"] = tz_name
    if hasattr(time_module, "tzset"):
        time_module.tzset()

    try:
        yield
    finally:
        if old_tz is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = old_tz

        if hasattr(time_module, "tzset"):
            time_module.tzset()


def substitution_context(tz_name: str = "Europe/Moscow"):
    server_info = SimpleNamespace(get_timezone=lambda: tz_name)
    return SimpleNamespace(server_info=server_info)


def assert_subst(tpl, params, sql, tz_name: str = "Europe/Moscow"):
    assert ProtoConnection.substitute_params(tpl, params, substitution_context(tz_name)) == sql


async def execute(conn, query, params=None, settings=None, columnar=False):
    return await conn._connection.execute(
        query,
        args=params,
        settings=settings,
        columnar=columnar,
    )


@pytest.mark.asyncio
async def test_int(conn):
    params = {"x": 123}

    assert_subst(SINGLE_TPL, params, "SELECT 123")

    rv = await execute(conn, SINGLE_TPL, params)
    assert rv == [(123,)]


@pytest.mark.asyncio
async def test_null(conn):
    params = {"x": None}

    assert_subst(SINGLE_TPL, params, "SELECT NULL")

    rv = await execute(conn, SINGLE_TPL, params)
    assert rv == [(None,)]


@pytest.mark.asyncio
async def test_date(conn):
    d = date(2017, 10, 16)
    params = {"x": d}

    assert_subst(SINGLE_TPL, params, "SELECT '2017-10-16'")

    rv = await execute(conn, SINGLE_TPL, params)
    assert rv == [("2017-10-16",)]

    tpl = "SELECT CAST(%(x)s AS Date)"
    assert_subst(tpl, params, "SELECT CAST('2017-10-16' AS Date)")

    rv = await execute(conn, tpl, params)
    assert rv == [(d,)]


@pytest.mark.asyncio
async def test_time(conn):
    t = time(8, 20, 15)
    params = {"x": t}

    assert_subst(SINGLE_TPL, params, "SELECT '08:20:15'")

    rv = await execute(conn, SINGLE_TPL, params)
    assert rv == [("08:20:15",)]


@pytest.mark.asyncio
async def test_datetime(conn):
    dt = datetime(2017, 10, 16, 0, 18, 50)
    params = {"x": dt}

    assert_subst(SINGLE_TPL, params, "SELECT '2017-10-16 00:18:50'")

    rv = await execute(conn, SINGLE_TPL, params)
    assert rv == [("2017-10-16 00:18:50",)]

    tpl = "SELECT CAST(%(x)s AS DateTime)"
    assert_subst(tpl, params, "SELECT CAST('2017-10-16 00:18:50' AS DateTime)")

    rv = await execute(conn, tpl, params)
    assert rv == [(dt,)]


@pytest.mark.asyncio
async def test_datetime_with_timezone(conn, monkeypatch):
    dt = datetime(2017, 7, 14, 5, 40, 0)
    aware = timezone("Asia/Kamchatka").localize(dt)
    params = {"x": aware}

    assert_subst(SINGLE_TPL, params, "SELECT '2017-07-13 20:40:00'")

    tpl = "SELECT toDateTime(toInt32(toDateTime(%(x)s))), toInt32(toDateTime(%(x)s))"

    server_tz = timezone(conn._connection.server_info.get_timezone())
    instant = aware.astimezone(utc)
    expected_server_dt = instant.astimezone(server_tz).replace(tzinfo=None)
    expected_client_dt = instant.astimezone(timezone("Asia/Novosibirsk")).replace(tzinfo=None)
    expected_epoch = int(instant.timestamp())
    monkeypatch.setattr(
        "asynch.proto.columns.datetimecolumn.get_localzone",
        lambda: timezone("Asia/Novosibirsk"),
    )

    with patch_env_tz("Asia/Novosibirsk"):
        rv = await execute(conn, tpl, params, settings={"use_client_time_zone": False})
        assert rv == [(expected_server_dt, expected_epoch)]

        rv = await execute(conn, tpl, params, settings={"use_client_time_zone": True})
        assert rv == [(expected_client_dt, expected_epoch)]


@pytest.mark.asyncio
async def test_string(conn):
    params = {"x": "test\t\n\x16", "y": "тест\t\n\x16"}

    assert_subst(DOUBLE_TPL, params, "SELECT 'test\\t\\n\x16', 'тест\\t\\n\x16'")

    rv = await execute(conn, DOUBLE_TPL, params)
    assert rv == [("test\t\n\x16", "тест\t\n\x16")]

    params = {"x": "'"}
    assert_subst(SINGLE_TPL, params, "SELECT '\\''")

    rv = await execute(conn, SINGLE_TPL, params)
    assert rv == [("'",)]

    params = {"x": "\\"}
    assert_subst(SINGLE_TPL, params, "SELECT '\\\\'")

    rv = await execute(conn, SINGLE_TPL, params)
    assert rv == [("\\",)]


@pytest.mark.asyncio
async def test_array(conn):
    params = {"x": [1, None, 2]}

    assert_subst(SINGLE_TPL, params, "SELECT [1, NULL, 2]")

    rv = await execute(conn, SINGLE_TPL, params)
    assert rv == [([1, None, 2],)]

    params = {"x": [[1, 2, 3], [4, 5], [6, 7]]}

    assert_subst(SINGLE_TPL, params, "SELECT [[1, 2, 3], [4, 5], [6, 7]]")

    rv = await execute(conn, SINGLE_TPL, params)
    assert rv == [([[1, 2, 3], [4, 5], [6, 7]],)]


@pytest.mark.asyncio
async def test_tuple(conn):
    params = {"x": (1, None, 2)}

    assert_subst(
        "SELECT * FROM test WHERE a IN %(x)s",
        params,
        "SELECT * FROM test WHERE a IN (1, NULL, 2)",
    )

    await execute(conn, "DROP TABLE IF EXISTS test.upstream_substitution_tuple")
    await execute(conn, "CREATE TABLE test.upstream_substitution_tuple (a Int32) ENGINE=Memory")
    try:
        await execute(conn, "INSERT INTO test.upstream_substitution_tuple (a) VALUES", [(1,)])
        await execute(conn, "INSERT INTO test.upstream_substitution_tuple (a) VALUES", [(2,)])

        inserted = await execute(
            conn,
            "SELECT * FROM test.upstream_substitution_tuple WHERE a IN (1)",
            columnar=True,
        )
        assert inserted == [(1,)]
    finally:
        await execute(conn, "DROP TABLE IF EXISTS test.upstream_substitution_tuple")


@pytest.mark.asyncio
async def test_enum(conn):
    class A(IntEnum):
        hello = -1
        world = 2

    params = {"x": A.hello, "y": A.world}

    assert_subst(DOUBLE_TPL, params, "SELECT -1, 2")

    rv = await execute(conn, DOUBLE_TPL, params)
    assert rv == [(-1, 2)]

    class B(Enum):
        hello = "hello"
        world = "world"

    params = {"x": B.hello, "y": B.world}

    assert_subst(DOUBLE_TPL, params, "SELECT 'hello', 'world'")

    rv = await execute(conn, DOUBLE_TPL, params)
    assert rv == [("hello", "world")]


@pytest.mark.asyncio
async def test_float(conn):
    params = {"x": 1e-12, "y": 123.45}

    assert_subst(DOUBLE_TPL, params, "SELECT 1e-12, 123.45")

    rv = await execute(conn, DOUBLE_TPL, params)
    assert rv == [(params["x"], params["y"])]


@pytest.mark.asyncio
async def test_decimal(conn):
    params = {"x": Decimal("1e-2"), "y": Decimal("123.45")}

    assert_subst(DOUBLE_TPL, params, "SELECT 0.01, 123.45")

    rv = await execute(conn, DOUBLE_TPL, params)
    assert rv == [(0.01, 123.45)]


@pytest.mark.asyncio
async def test_uuid(conn):
    params = {"x": UUID("c0fcbba9-0752-44ed-a5d6-4dfb4342b89d")}

    assert_subst(SINGLE_TPL, params, "SELECT 'c0fcbba9-0752-44ed-a5d6-4dfb4342b89d'")

    rv = await execute(conn, SINGLE_TPL, params)
    assert rv == [("c0fcbba9-0752-44ed-a5d6-4dfb4342b89d",)]


def test_substitute_object():
    with pytest.raises(ValueError, match="Parameters are expected in dict form"):
        ProtoConnection.substitute_params(SINGLE_TPL, object(), substitution_context())


@pytest.mark.asyncio
async def test_server_side_int(conn):
    rv = await execute(
        conn,
        "SELECT {x:Int32}",
        {"x": 123},
        settings={"server_side_params": True},
    )
    assert rv == [(123,)]


@pytest.mark.asyncio
async def test_server_side_str(conn):
    rv = await execute(
        conn,
        "SELECT {x:Int32}",
        {"x": "123"},
        settings={"server_side_params": True},
    )
    assert rv == [(123,)]


@pytest.mark.asyncio
async def test_server_side_escaped_str(conn):
    rv = await execute(
        conn,
        "SELECT {x:String}, length({x:String})",
        {"x": "\t"},
        settings={"server_side_params": True},
    )
    assert rv == [("\t", 1)]

    rv = await execute(
        conn,
        "SELECT {x:String}, length({x:String})",
        {"x": "\\"},
        settings={"server_side_params": True},
    )
    assert rv == [("\\", 1)]

    rv = await execute(
        conn,
        "SELECT {x:String}, length({x:String})",
        {"x": "'"},
        settings={"server_side_params": True},
    )
    assert rv == [("'", 1)]


@pytest.mark.asyncio
async def test_reserved_keywords(conn):
    await execute(
        conn,
        "SELECT * FROM system.events LIMIT %(limit)s OFFSET %(offset)s",
        {"limit": 20, "offset": 30},
    )
