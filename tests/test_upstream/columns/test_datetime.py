import os
import time
from contextlib import contextmanager
from datetime import date, datetime
from unittest.mock import patch

import pytest
from pytz import UnknownTimeZoneError, timezone, utc

from tests.test_upstream.columns._helpers import create_table, execute

pytestmark = pytest.mark.asyncio

DT = datetime(2017, 7, 14, 5, 40)
DT_TZ = timezone("Asia/Kamchatka").localize(DT)
COL_TZ_NAME = "Asia/Novosibirsk"
COL_TZ = timezone(COL_TZ_NAME)


@contextmanager
def local_timezone(name):
    old_tz = os.environ.get("TZ")
    os.environ["TZ"] = name
    time.tzset()
    try:
        yield
    finally:
        if old_tz is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = old_tz
        time.tzset()


def patch_localzone(monkeypatch, name):
    monkeypatch.setattr(
        "asynch.proto.columns.datetimecolumn.get_localzone",
        lambda: timezone(name),
    )


def datetime_column(with_tz=False):
    if not with_tz:
        return "a DateTime"
    return f"a DateTime('{COL_TZ_NAME}')"


async def insert_datetime_literal(conn, table, expression, settings=None):
    await execute(conn, f"INSERT INTO {table} (a) VALUES ({expression})", settings=settings)


async def test_simple(conn):
    data = [(date(2012, 10, 25), datetime(2012, 10, 25, 14, 7, 19))]

    async with create_table(conn, "a Date, b DateTime") as table:
        await execute(conn, f"INSERT INTO {table} (a, b) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_nullable_date(conn):
    data = [
        (None,),
        (date(2012, 10, 25),),
        (None,),
        (date(2017, 6, 23),),
    ]

    async with create_table(conn, "a Nullable(Date)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_nullable_datetime(conn):
    data = [
        (None,),
        (datetime(2012, 10, 25, 14, 7, 19),),
        (None,),
        (datetime(2017, 6, 23, 19, 10, 15),),
    ]

    async with create_table(conn, "a Nullable(DateTime)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_handle_errors_from_tzlocal(conn):
    with patch("asynch.proto.columns.datetimecolumn.get_localzone") as mocked:
        mocked.side_effect = UnknownTimeZoneError()
        await execute(conn, "SELECT now()")


async def test_datetime64_frac_trunc(conn):
    data = [(datetime(2012, 10, 25, 14, 7, 19, 125600),)]

    async with create_table(conn, "a DateTime64") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == [(datetime(2012, 10, 25, 14, 7, 19, 125000),)]


async def test_insert_integers(conn):
    async with create_table(conn, "a DateTime") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", [(1530211034,)])

        inserted = await execute(conn, f"SELECT toUInt32(a) FROM {table}")

    assert inserted == [(1530211034,)]


async def test_insert_integers_datetime64(conn):
    async with create_table(conn, "a DateTime64") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", [(1530211034123,)])

        inserted = await execute(conn, f"SELECT toUInt32(a) FROM {table}")

    assert inserted == [(1530211034,)]


async def test_insert_integer_bounds(conn):
    data = [(0,), (1,), (1500000000,), (2**32 - 1,)]

    async with create_table(conn, "a DateTime") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT toUInt32(a) FROM {table} ORDER BY a")

    assert inserted == [(0,), (1,), (1500000000,), (2**32 - 1,)]


async def test_insert_datetime64_extended_range(conn):
    data = [(-1420077600,), (-1420077599,), (0,), (1,), (9877248000,)]

    async with create_table(conn, "a DateTime64(0)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT toInt64(a) FROM {table} ORDER BY a")

    assert inserted == [
        (-1420077600,),
        (-1420077599,),
        (0,),
        (1,),
        (9877248000,),
    ]


async def test_insert_datetime64_extended_range_pure_ints_out_of_range(conn):
    data = [(0,), (1,), (-(2**63),), (2**63 - 1,)]

    async with create_table(conn, "a DateTime64(0)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT toInt64(a) FROM {table} ORDER BY a")

    assert inserted == [(-(2**63),), (0,), (1,), (2**63 - 1,)]


async def test_use_server_timezone(conn, monkeypatch):
    server_tz_name = (await execute(conn, "SELECT timezone()"))[0][0]
    offset = timezone(server_tz_name).utcoffset(DT).total_seconds()
    timestamp = 1500010800 - int(offset)
    patch_localzone(monkeypatch, "Asia/Novosibirsk")

    with local_timezone("Asia/Novosibirsk"):
        async with create_table(conn, datetime_column()) as table:
            await execute(conn, f"INSERT INTO {table} (a) VALUES", [(DT,)])
            await insert_datetime_literal(conn, table, "'2017-07-14 05:40:00'")

            timestamps = await execute(conn, f"SELECT toInt32(a) FROM {table}")
            inserted = await execute(conn, f"SELECT * FROM {table}")

    assert timestamps == [(timestamp,), (timestamp,)]
    assert inserted == [(DT,), (DT,)]


async def test_use_client_timezone(conn, monkeypatch):
    settings = {"use_client_time_zone": True}
    patch_localzone(monkeypatch, "Asia/Novosibirsk")

    with local_timezone("Asia/Novosibirsk"):
        async with create_table(conn, datetime_column()) as table:
            await execute(
                conn,
                f"INSERT INTO {table} (a) VALUES",
                [(DT,)],
                settings=settings,
            )

            timestamps = await execute(
                conn, f"SELECT toInt32(a) FROM {table}", settings=settings
            )
            inserted = await execute(conn, f"SELECT * FROM {table}", settings=settings)

    assert timestamps == [(1499985600,)]
    assert inserted == [(DT,)]


async def test_datetime_with_timezone_use_server_timezone(conn, monkeypatch):
    server_tz_name = (await execute(conn, "SELECT timezone()"))[0][0]
    offset = timezone(server_tz_name).utcoffset(DT)
    patch_localzone(monkeypatch, "Asia/Novosibirsk")

    with local_timezone("Asia/Novosibirsk"):
        async with create_table(conn, datetime_column()) as table:
            await execute(conn, f"INSERT INTO {table} (a) VALUES", [(DT_TZ,)])
            await insert_datetime_literal(
                conn,
                table,
                "toDateTime('2017-07-14 05:40:00', 'Asia/Kamchatka')",
            )

            timestamps = await execute(conn, f"SELECT toInt32(a) FROM {table}")
            inserted = await execute(conn, f"SELECT * FROM {table}")

    expected = (DT_TZ.astimezone(utc) + offset).replace(tzinfo=None)
    assert timestamps == [(1499967600,), (1499967600,)]
    assert inserted == [(expected,), (expected,)]


async def test_datetime_with_timezone_use_client_timezone(conn, monkeypatch):
    settings = {"use_client_time_zone": True}
    patch_localzone(monkeypatch, "Asia/Novosibirsk")

    with local_timezone("Asia/Novosibirsk"):
        async with create_table(conn, datetime_column()) as table:
            await execute(
                conn,
                f"INSERT INTO {table} (a) VALUES",
                [(DT_TZ,)],
                settings=settings,
            )
            await insert_datetime_literal(
                conn,
                table,
                "toDateTime('2017-07-14 05:40:00', 'Asia/Kamchatka')",
                settings=settings,
            )

            timestamps = await execute(
                conn, f"SELECT toInt32(a) FROM {table}", settings=settings
            )
            inserted = await execute(conn, f"SELECT * FROM {table}", settings=settings)

    expected = datetime(2017, 7, 14, 0, 40)
    assert timestamps == [(1499967600,), (1499967600,)]
    assert inserted == [(expected,), (expected,)]


async def test_column_use_server_timezone(conn, monkeypatch):
    patch_localzone(monkeypatch, "Europe/Moscow")

    with local_timezone("Europe/Moscow"):
        async with create_table(conn, datetime_column(with_tz=True)) as table:
            await execute(conn, f"INSERT INTO {table} (a) VALUES", [(DT,)])
            await insert_datetime_literal(conn, table, "'2017-07-14 05:40:00'")

            timestamps = await execute(conn, f"SELECT toInt32(a) FROM {table}")
            inserted = await execute(conn, f"SELECT * FROM {table}")

    expected = COL_TZ.localize(DT)
    assert timestamps == [(1499985600,), (1499985600,)]
    assert inserted == [(expected,), (expected,)]


async def test_column_use_client_timezone(conn, monkeypatch):
    settings = {"use_client_time_zone": True}
    patch_localzone(monkeypatch, "Europe/Moscow")

    with local_timezone("Europe/Moscow"):
        async with create_table(conn, datetime_column(with_tz=True)) as table:
            await execute(
                conn,
                f"INSERT INTO {table} (a) VALUES",
                [(DT,)],
                settings=settings,
            )
            await insert_datetime_literal(
                conn, table, "'2017-07-14 05:40:00'", settings=settings
            )

            timestamps = await execute(
                conn, f"SELECT toInt32(a) FROM {table}", settings=settings
            )
            inserted = await execute(conn, f"SELECT * FROM {table}", settings=settings)

    expected = COL_TZ.localize(DT)
    assert timestamps == [(1499985600,), (1499985600,)]
    assert inserted == [(expected,), (expected,)]


async def test_datetime_with_timezone_column_use_server_timezone(conn, monkeypatch):
    patch_localzone(monkeypatch, "Europe/Moscow")

    with local_timezone("Europe/Moscow"):
        async with create_table(conn, datetime_column(with_tz=True)) as table:
            await execute(conn, f"INSERT INTO {table} (a) VALUES", [(DT_TZ,)])
            await insert_datetime_literal(
                conn,
                table,
                "toDateTime('2017-07-14 05:40:00', 'Asia/Kamchatka')",
            )

            timestamps = await execute(conn, f"SELECT toInt32(a) FROM {table}")
            inserted = await execute(conn, f"SELECT * FROM {table}")

    expected = COL_TZ.localize(datetime(2017, 7, 14, 0, 40))
    assert timestamps == [(1499967600,), (1499967600,)]
    assert inserted == [(expected,), (expected,)]


async def test_datetime_with_timezone_column_use_client_timezone(conn, monkeypatch):
    settings = {"use_client_time_zone": True}
    patch_localzone(monkeypatch, "Europe/Moscow")

    with local_timezone("Europe/Moscow"):
        async with create_table(conn, datetime_column(with_tz=True)) as table:
            await execute(
                conn,
                f"INSERT INTO {table} (a) VALUES",
                [(DT_TZ,)],
                settings=settings,
            )
            await insert_datetime_literal(
                conn,
                table,
                "toDateTime('2017-07-14 05:40:00', 'Asia/Kamchatka')",
                settings=settings,
            )

            timestamps = await execute(
                conn, f"SELECT toInt32(a) FROM {table}", settings=settings
            )
            inserted = await execute(conn, f"SELECT * FROM {table}", settings=settings)

    expected = COL_TZ.localize(datetime(2017, 7, 14, 0, 40))
    assert timestamps == [(1499967600,), (1499967600,)]
    assert inserted == [(expected,), (expected,)]
