from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from asynch.errors import UnexpectedPacketFromServerError
from asynch.proto.block import RowOrientedBlock
from asynch.proto.connection import SUBSTITUTE_PARAMS_STYLE_ENV
from asynch.proto.connection import Connection as ProtoConnection
from asynch.proto.protocol import ServerPacket


@pytest.fixture(scope="session", autouse=True)
async def initialize_tests():
    yield


@pytest.fixture(scope="function", autouse=True)
async def truncate_table():
    yield


def test_upstream_substitute_params_uses_pyformat_by_default(monkeypatch):
    monkeypatch.delenv(SUBSTITUTE_PARAMS_STYLE_ENV, raising=False)

    assert (
        ProtoConnection.substitute_params(
            "SELECT %(value)s, %(name)s",
            {"value": 1, "name": "hello"},
        )
        == "SELECT 1, 'hello'"
    )


def test_upstream_server_side_params_is_client_setting():
    conn = ProtoConnection(settings={"server_side_params": True})

    assert conn.client_settings["server_side_params"] is True
    assert "server_side_params" not in conn.settings


@pytest.mark.asyncio
async def test_upstream_server_side_params_defer_local_substitution():
    conn = ProtoConnection(settings={"server_side_params": True})
    conn.send_query = AsyncMock()
    conn.send_external_tables = AsyncMock()
    conn.receive_result = AsyncMock(return_value=[])

    await conn.process_ordinary_query(
        "SELECT {value:UInt8}",
        params={"value": 1},
    )

    conn.send_query.assert_awaited_once_with(
        "SELECT {value:UInt8}",
        query_id=None,
        params={"value": 1},
    )


@pytest.mark.asyncio
async def test_upstream_send_data_receives_profile_events_after_each_block():
    conn = ProtoConnection()
    conn.context.client_settings = {"insert_block_size": 1}
    conn.send_block = AsyncMock()
    conn.receive_profile_events = AsyncMock()
    sample_block = RowOrientedBlock(columns_with_types=[("value", "UInt8")])

    result = await conn.send_data(sample_block, [(1,), (2,)])

    assert result == 2
    assert conn.send_block.await_count == 3
    assert conn.receive_profile_events.await_count == 3


@pytest.mark.asyncio
async def test_upstream_insert_end_rejects_unexpected_packets():
    conn = ProtoConnection()
    sample_block = RowOrientedBlock(columns_with_types=[("value", "UInt8")])
    conn.send_query = AsyncMock()
    conn.send_external_tables = AsyncMock()
    conn.receive_sample_block = AsyncMock(return_value=sample_block)
    conn.send_data = AsyncMock(return_value=1)
    conn.receive_packet = AsyncMock(side_effect=[SimpleNamespace(type=ServerPacket.DATA), False])

    with pytest.raises(UnexpectedPacketFromServerError):
        await conn.process_insert_query(
            "INSERT INTO test.test VALUES",
            [(1,)],
        )
