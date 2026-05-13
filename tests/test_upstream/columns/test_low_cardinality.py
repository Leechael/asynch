from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID

import pytest

from tests.test_upstream.columns._helpers import create_table, execute

pytestmark = pytest.mark.asyncio

SUSPICIOUS_TYPE_SETTINGS = {"allow_suspicious_low_cardinality_types": 1}


def low_cardinality_table(conn, columns):
    return create_table(conn, columns, settings=SUSPICIOUS_TYPE_SETTINGS)


async def test_uint8(conn):
    data = [(x,) for x in range(255)]

    async with low_cardinality_table(conn, "a LowCardinality(UInt8)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_int8(conn):
    data = [(x - 127,) for x in range(255)]

    async with low_cardinality_table(conn, "a LowCardinality(Int8)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_nullable_int8(conn):
    data = [(None,), (-1,), (0,), (1,), (None,)]

    async with low_cardinality_table(conn, "a LowCardinality(Nullable(Int8))") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_date(conn):
    start = date(1970, 1, 1)
    data = [(start + timedelta(x),) for x in range(300)]

    async with low_cardinality_table(conn, "a LowCardinality(Date)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_nullable_date(conn):
    data = [(date(2023, 4, 1),), (None,), (date(1970, 1, 1),)]

    async with low_cardinality_table(conn, "a LowCardinality(Nullable(Date))") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_nullable_uuid(conn):
    data = [(UUID("2efcead4-ff55-4db5-bdb4-6b36a308d8e0"),), (None,)]

    async with low_cardinality_table(conn, "a LowCardinality(Nullable(UUID))") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_float(conn):
    data = [(float(x),) for x in range(300)]

    async with low_cardinality_table(conn, "a LowCardinality(Float)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_decimal(conn):
    data = [(Decimal(x),) for x in range(300)]

    async with low_cardinality_table(conn, "a LowCardinality(Float)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_array(conn):
    data = [([100, 500],)]

    async with low_cardinality_table(conn, "a Array(LowCardinality(Int16))") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_empty_array(conn):
    data = [([],)]

    async with low_cardinality_table(conn, "a Array(LowCardinality(Int16))") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_string(conn):
    data = [("test",), ("low",), ("cardinality",), ("test",), ("test",), ("",)]

    async with low_cardinality_table(conn, "a LowCardinality(String)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_fixed_string(conn):
    data = [("test",), ("low",), ("cardinality",), ("test",), ("test",), ("",)]

    async with low_cardinality_table(conn, "a LowCardinality(FixedString(12))") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_nullable_string(conn):
    data = [("test",), ("",), (None,)]

    async with low_cardinality_table(conn, "a LowCardinality(Nullable(String))") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data
