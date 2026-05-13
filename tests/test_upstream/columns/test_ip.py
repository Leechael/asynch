from ipaddress import IPv4Address, IPv6Address

import pytest

from asynch.errors import CannotParseDomainError, TypeMismatchError
from tests.test_upstream.columns._helpers import create_table, execute

pytestmark = pytest.mark.asyncio


async def test_ipv4_simple(conn):
    data = [(IPv4Address("10.0.0.1"),), (IPv4Address("192.168.253.42"),)]

    async with create_table(conn, "a IPv4") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_ipv4_from_int(conn):
    data = [(167772161,)]

    async with create_table(conn, "a IPv4") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data, types_check=True)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == [(IPv4Address("10.0.0.1"),)]


async def test_ipv4_from_str(conn):
    data = [("10.0.0.1",)]

    async with create_table(conn, "a IPv4") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data, types_check=True)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == [(IPv4Address("10.0.0.1"),)]


async def test_ipv4_type_mismatch(conn):
    async with create_table(conn, "a IPv4") as table:
        with pytest.raises(TypeMismatchError):
            await execute(
                conn,
                f"INSERT INTO {table} (a) VALUES",
                [(1025.2147,)],
                types_check=True,
            )


async def test_bad_ipv4(conn):
    async with create_table(conn, "a IPv4") as table:
        with pytest.raises(CannotParseDomainError):
            await execute(conn, f"INSERT INTO {table} (a) VALUES", [("985.512.12.0",)])


async def test_bad_ipv4_with_type_check(conn):
    async with create_table(conn, "a IPv4") as table:
        with pytest.raises(TypeMismatchError):
            await execute(
                conn,
                f"INSERT INTO {table} (a) VALUES",
                [("985.512.12.0",)],
                types_check=True,
            )


async def test_ipv4_nullable(conn):
    data = [(IPv4Address("10.10.10.10"),), (None,)]

    async with create_table(conn, "a Nullable(IPv4)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_ipv6_simple(conn):
    data = [
        (IPv6Address("79f4:e698:45de:a59b:2765:28e3:8d3a:35ae"),),
        (IPv6Address("a22:cc64:cf47:1653:4976:3c0c:ff8d:417c"),),
        (IPv6Address("12ff::1"),),
    ]

    async with create_table(conn, "a IPv6") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


async def test_ipv6_from_str(conn):
    data = [("79f4:e698:45de:a59b:2765:28e3:8d3a:35ae",)]

    async with create_table(conn, "a IPv6") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data, types_check=True)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == [(IPv6Address("79f4:e698:45de:a59b:2765:28e3:8d3a:35ae"),)]


async def test_ipv6_from_bytes(conn):
    data = [(b"y\xf4\xe6\x98E\xde\xa5\x9b'e(\xe3\x8d:5\xae",)]

    async with create_table(conn, "a IPv6") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data, types_check=True)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == [(IPv6Address("79f4:e698:45de:a59b:2765:28e3:8d3a:35ae"),)]


async def test_ipv6_type_mismatch(conn):
    async with create_table(conn, "a IPv6") as table:
        with pytest.raises(TypeMismatchError):
            await execute(
                conn,
                f"INSERT INTO {table} (a) VALUES",
                [(1025.2147,)],
                types_check=True,
            )


async def test_bad_ipv6(conn):
    async with create_table(conn, "a IPv6") as table:
        with pytest.raises(CannotParseDomainError):
            await execute(
                conn,
                f"INSERT INTO {table} (a) VALUES",
                [("ghjk:e698:45de:a59b:2765:28e3:8d3a:zzzz",)],
            )


async def test_bad_ipv6_with_type_check(conn):
    async with create_table(conn, "a IPv6") as table:
        with pytest.raises(TypeMismatchError):
            await execute(
                conn,
                f"INSERT INTO {table} (a) VALUES",
                [("ghjk:e698:45de:a59b:2765:28e3:8d3a:zzzz",)],
                types_check=True,
            )


async def test_ipv6_nullable(conn):
    data = [(IPv6Address("79f4:e698:45de:a59b:2765:28e3:8d3a:35ae"),), (None,)]

    async with create_table(conn, "a Nullable(IPv6)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data
