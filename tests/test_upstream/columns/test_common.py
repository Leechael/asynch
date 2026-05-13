import pytest

from tests.test_upstream.columns._helpers import create_table, execute

pytestmark = pytest.mark.asyncio


async def test_insert_block_size(conn, monkeypatch):
    data = [(x,) for x in range(4)]
    async with create_table(conn, "a UInt8") as table:
        send_block_count = 0
        old_send_block = conn._connection.send_block

        async def send_block(*args, **kwargs):
            nonlocal send_block_count
            send_block_count += 1
            return await old_send_block(*args, **kwargs)

        monkeypatch.setattr(conn._connection, "send_block", send_block)

        await execute(
            conn,
            f"INSERT INTO {table} (a) VALUES",
            data,
            settings={"insert_block_size": 1},
        )

        inserted = await execute(conn, f"SELECT * FROM {table}")

        assert send_block_count == 7
        assert inserted == data


async def test_columnar_insert_block_size(conn, monkeypatch):
    data = [(0, 1, 2, 3)]
    async with create_table(conn, "a UInt8") as table:
        send_block_count = 0
        old_send_block = conn._connection.send_block

        async def send_block(*args, **kwargs):
            nonlocal send_block_count
            send_block_count += 1
            return await old_send_block(*args, **kwargs)

        monkeypatch.setattr(conn._connection, "send_block", send_block)

        await execute(
            conn,
            f"INSERT INTO {table} (a) VALUES",
            data,
            settings={"insert_block_size": 1},
            columnar=True,
        )

        inserted = await execute(conn, f"SELECT * FROM {table}")

        assert send_block_count == 7
        assert inserted == [(0,), (1,), (2,), (3,)]
