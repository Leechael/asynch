from decimal import Decimal

import pytest

from asynch.errors import TypeMismatchError
from tests.test_upstream.columns._helpers import create_table, execute

pytestmark = pytest.mark.asyncio


async def test_simple(conn):
    data = [(Decimal("300.42"),), (300.42,), (-300,)]

    async with create_table(conn, "a Decimal(9, 5)") as table:
        await execute(
            conn,
            f"INSERT INTO {table} (a) VALUES",
            data,
            types_check=True,
        )

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == [(Decimal("300.42"),), (Decimal("300.42"),), (Decimal("-300"),)]


async def test_different_precisions(conn):
    data = [
        (
            Decimal("300.42"),
            Decimal("17179869484.42"),
            Decimal("1267650600228229401496703205676.42"),
        )
    ]

    async with create_table(
        conn, "a Decimal32(2), b Decimal64(2), c Decimal128(2)"
    ) as table:
        await execute(conn, f"INSERT INTO {table} (a, b, c) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_different_precisions_negative(conn):
    data = [
        (
            Decimal("-300.42"),
            Decimal("-17179869484.42"),
            Decimal("-1267650600228229401496703205676.42"),
        )
    ]

    async with create_table(
        conn, "a Decimal32(2), b Decimal64(2), c Decimal128(2)"
    ) as table:
        await execute(conn, f"INSERT INTO {table} (a, b, c) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_max_precisions(conn):
    data = [
        (Decimal(10**9 - 1), Decimal(10**18 - 1), Decimal(10**38 - 1)),
        (Decimal(-(10**9) + 1), Decimal(-(10**18) + 1), Decimal(-(10**38) + 1)),
    ]

    async with create_table(
        conn, "a Decimal32(0), b Decimal64(0), c Decimal128(0)"
    ) as table:
        await execute(conn, f"INSERT INTO {table} (a, b, c) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_nullable(conn):
    data = [(300.42,), (None,)]

    async with create_table(conn, "a Nullable(Decimal32(3))") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == [(Decimal("300.42"),), (None,)]


async def test_no_scale(conn):
    data = [(2147483647,)]

    async with create_table(conn, "a Decimal32(0)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == [(Decimal("2147483647"),)]


async def test_type_mismatch(conn):
    data = [(2147483649,)]

    async with create_table(conn, "a Decimal32(0)") as table:
        with pytest.raises(TypeMismatchError) as exc:
            await execute(
                conn,
                f"INSERT INTO {table} (a) VALUES",
                data,
                types_check=True,
            )

        assert '2147483649 for column "a"' in str(exc.value)

        with pytest.raises(TypeMismatchError) as exc:
            await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        assert "Column a" in str(exc.value)


async def test_preserve_precision(conn):
    data = [(1.66,), (1.15,)]

    async with create_table(conn, "a Decimal(18, 2)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == [(Decimal("1.66"),), (Decimal("1.15"),)]


async def test_precision_one_sign_after_point(conn):
    data = [(1.6,), (1.0,), (12312.0,), (999999.6,)]

    async with create_table(conn, "a Decimal(8, 1)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == [
        (Decimal("1.6"),),
        (Decimal("1.0"),),
        (Decimal("12312.0"),),
        (Decimal("999999.6"),),
    ]


async def test_truncates_scale(conn):
    data = [(3.14159265358,), (2.7182,)]

    async with create_table(conn, "a Decimal(9, 4)") as table:
        await execute(
            conn,
            f"INSERT INTO {table} (a) VALUES",
            data,
            types_check=True,
        )

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == [(Decimal("3.1415"),), (Decimal("2.7182"),)]


async def test_decimal256(conn):
    data = [
        (1.66,),
        (1.15,),
        (Decimal("1606938044258990275541962092341162602522202993782792835301676.42"),),
        (Decimal("-1606938044258990275541962092341162602522202993782792835301676.42"),),
    ]

    async with create_table(conn, "a Decimal256(2)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == [
        (Decimal("1.66"),),
        (Decimal("1.15"),),
        (Decimal("1606938044258990275541962092341162602522202993782792835301676.42"),),
        (Decimal("-1606938044258990275541962092341162602522202993782792835301676.42"),),
    ]
