import pytest

from asynch.errors import TypeMismatchError
from tests.test_upstream.columns._helpers import create_table, execute

pytestmark = pytest.mark.asyncio

INT_COLUMNS = "a Int8, b Int16, c Int32, d Int64, e UInt8, f UInt16, g UInt32, h UInt64"


async def test_chop_to_type(conn):
    async with create_table(conn, "a UInt8") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", [(300,)], types_check=True)

        inserted = await execute(conn, f"SELECT * FROM {table}")
        assert inserted == [(44,)]

    async with create_table(conn, "a Int8") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", [(-300,)], types_check=True)

        inserted = await execute(conn, f"SELECT * FROM {table}")
        assert inserted == [(-44,)]


async def test_raise_struct_error(conn):
    async with create_table(conn, "a UInt8") as table:
        with pytest.raises(TypeMismatchError) as exc:
            await execute(conn, f"INSERT INTO {table} (a) VALUES", [(300,)])

    assert "Column a" in str(exc.value)
    assert "types_check=True" in str(exc.value)


async def test_uint_type_mismatch(conn):
    data = [(-1,)]

    async with create_table(conn, "a UInt8") as table:
        with pytest.raises(TypeMismatchError) as exc:
            await execute(conn, f"INSERT INTO {table} (a) VALUES", data, types_check=True)
        assert '-1 for column "a"' in str(exc.value)

        with pytest.raises(TypeMismatchError) as exc:
            await execute(conn, f"INSERT INTO {table} (a) VALUES", data)
        assert "Column a" in str(exc.value)


async def test_all_sizes(conn):
    data = [
        (
            -10,
            -300,
            -123581321,
            -123581321345589144,
            10,
            300,
            123581321,
            123581321345589144,
        )
    ]

    async with create_table(conn, INT_COLUMNS) as table:
        await execute(
            conn,
            f"INSERT INTO {table} (a, b, c, d, e, f, g, h) VALUES",
            data,
        )

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_corner_cases(conn):
    data = [
        (
            -128,
            -32768,
            -2147483648,
            -9223372036854775808,
            255,
            65535,
            4294967295,
            18446744073709551615,
        ),
        (127, 32767, 2147483647, 9223372036854775807, 0, 0, 0, 0),
    ]

    async with create_table(conn, INT_COLUMNS) as table:
        await execute(
            conn,
            f"INSERT INTO {table} (a, b, c, d, e, f, g, h) VALUES",
            data,
        )

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_nullable(conn):
    data = [(2,), (None,), (4,), (None,), (8,)]

    async with create_table(conn, "a Nullable(Int32)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_int128(conn):
    data = [
        (-170141183460469231731687303715884105728,),
        (-111111111111111111111111111111111111111,),
        (123,),
        (111111111111111111111111111111111111111,),
        (170141183460469231731687303715884105727,),
    ]

    async with create_table(conn, "a Int128") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_uint128(conn):
    data = [(0,), (123,), (340282366920938463463374607431768211455,)]

    async with create_table(conn, "a UInt128") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_int256(conn):
    data = [
        (-57896044618658097711785492504343953926634992332820282019728792003956564819968,),
        (-11111111111111111111111111111111111111111111111111111111111111111111111111111,),
        (123,),
        (11111111111111111111111111111111111111111111111111111111111111111111111111111,),
        (57896044618658097711785492504343953926634992332820282019728792003956564819967,),
    ]

    async with create_table(conn, "a Int256") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_uint256(conn):
    data = [
        (0,),
        (123,),
        (111111111111111111111111111111111111111111111111111111111111111111111111111111,),
        (115792089237316195423570985008687907853269984665640564039457584007913129639935,),
    ]

    async with create_table(conn, "a UInt256") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data
