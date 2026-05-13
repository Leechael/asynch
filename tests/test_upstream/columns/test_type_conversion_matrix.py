from datetime import date, datetime
from decimal import Decimal
from ipaddress import IPv4Address, IPv6Address
from uuid import UUID

import pytest
from pytz import timezone

from tests.test_upstream.columns._helpers import create_table, execute

pytestmark = pytest.mark.asyncio


async def test_scalar_python_values_auto_convert_with_types_check(conn):
    utc = timezone("UTC")

    columns = """
        i8 Int8,
        u8 UInt8,
        i64 Int64,
        u64 UInt64,
        f32 Float32,
        f64 Float64,
        dec Decimal(9, 2),
        d Date,
        d32 Date32,
        dt DateTime('UTC'),
        dt64 DateTime64(3, 'UTC'),
        uuid UUID,
        ipv4 IPv4,
        ipv6 IPv6,
        b Bool,
        s String
    """
    data = [
        (
            -128,
            255,
            -(2**63),
            2**64 - 1,
            1,
            2.5,
            12.34,
            "2020-01-02",
            datetime(1925, 1, 1, 12, 0, 0),
            "2020-01-02 03:04:05",
            "2020-01-02 03:04:05.123456",
            "2efcead4-ff55-4db5-bdb4-6b36a308d8e0",
            "10.20.30.40",
            "2001:db8::1",
            True,
            "hello",
        )
    ]

    async with create_table(conn, columns) as table:
        await execute(
            conn,
            f"INSERT INTO {table} VALUES",
            data,
            types_check=True,
        )

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == [
        (
            -128,
            255,
            -(2**63),
            2**64 - 1,
            1.0,
            2.5,
            Decimal("12.34"),
            date(2020, 1, 2),
            date(1925, 1, 1),
            utc.localize(datetime(2020, 1, 2, 3, 4, 5)),
            utc.localize(datetime(2020, 1, 2, 3, 4, 5, 123000)),
            UUID("2efcead4-ff55-4db5-bdb4-6b36a308d8e0"),
            IPv4Address("10.20.30.40"),
            IPv6Address("2001:db8::1"),
            True,
            "hello",
        )
    ]


async def test_nullable_python_values_auto_convert_with_types_check(conn):
    utc = timezone("UTC")

    columns = """
        i Nullable(Int32),
        f Nullable(Float64),
        dec Nullable(Decimal(9, 2)),
        d Nullable(Date),
        dt Nullable(DateTime('UTC')),
        uuid Nullable(UUID),
        ipv4 Nullable(IPv4),
        b Nullable(Bool),
        s Nullable(String)
    """
    data = [
        (None, None, None, None, None, None, None, None, None),
        (
            42,
            3,
            1,
            "2021-02-03",
            "2021-02-03 04:05:06",
            "c0fcbba9-0752-44ed-a5d6-4dfb4342b89d",
            "127.0.0.1",
            False,
            "value",
        ),
    ]

    async with create_table(conn, columns) as table:
        await execute(conn, f"INSERT INTO {table} VALUES", data, types_check=True)

        inserted = await execute(conn, f"SELECT * FROM {table} ORDER BY i NULLS FIRST")

    assert inserted == [
        (None, None, None, None, None, None, None, None, None),
        (
            42,
            3.0,
            Decimal("1"),
            date(2021, 2, 3),
            utc.localize(datetime(2021, 2, 3, 4, 5, 6)),
            UUID("c0fcbba9-0752-44ed-a5d6-4dfb4342b89d"),
            IPv4Address("127.0.0.1"),
            False,
            "value",
        ),
    ]


async def test_composite_python_values_auto_convert_with_types_check(conn):
    columns = """
        uuids Array(UUID),
        decimals Array(Decimal(9, 2)),
        attrs Map(String, Decimal(9, 2)),
        nested Tuple(UUID, IPv4, Nullable(Date))
    """
    data = [
        (
            [
                "2efcead4-ff55-4db5-bdb4-6b36a308d8e0",
                UUID("c0fcbba9-0752-44ed-a5d6-4dfb4342b89d"),
            ],
            [1, Decimal("2.25"), 3.5],
            {"a": 1, "b": Decimal("2.25")},
            (
                "2efcead4-ff55-4db5-bdb4-6b36a308d8e0",
                "10.0.0.1",
                "2022-03-04",
            ),
        )
    ]

    async with create_table(conn, columns) as table:
        await execute(conn, f"INSERT INTO {table} VALUES", data, types_check=True)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == [
        (
            [
                UUID("2efcead4-ff55-4db5-bdb4-6b36a308d8e0"),
                UUID("c0fcbba9-0752-44ed-a5d6-4dfb4342b89d"),
            ],
            [Decimal("1"), Decimal("2.25"), Decimal("3.5")],
            {"a": Decimal("1"), "b": Decimal("2.25")},
            (
                UUID("2efcead4-ff55-4db5-bdb4-6b36a308d8e0"),
                IPv4Address("10.0.0.1"),
                date(2022, 3, 4),
            ),
        )
    ]
