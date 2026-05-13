import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import leb128
import pytest

from asynch.errors import UnexpectedPacketFromServerError
from asynch.proto import constants
from asynch.proto.block import ColumnOrientedBlock, RowOrientedBlock
from asynch.proto.connection import SUBSTITUTE_PARAMS_STYLE_ENV
from asynch.proto.connection import Connection as ProtoConnection
from asynch.proto.context import Context
from asynch.proto.protocol import ServerPacket
from asynch.proto.streams.block import BlockWriter
from asynch.proto.streams.buffered import BufferedReader, BufferedWriter
from asynch.proto.utils.escape import escape_params


def _read_varint(buffer: bytes, position: int) -> tuple[int, int]:
    chunk = bytearray()
    while True:
        if position >= len(buffer):
            raise AssertionError("Unexpected end of query packet")
        byte = buffer[position]
        position += 1
        chunk.append(byte)
        if byte < 0x80:
            break
    return leb128.u.decode(chunk), position


def _read_str(buffer: bytes, position: int) -> tuple[str, int]:
    length, position = _read_varint(buffer, position)
    value = buffer[position : position + length].decode()
    return value, position + length


def _read_settings_as_strings(buffer: bytes, position: int) -> tuple[dict[str, tuple[int, str]], int]:
    settings = {}
    while True:
        name, position = _read_str(buffer, position)
        if not name:
            break
        flags = buffer[position]
        position += 1
        value, position = _read_str(buffer, position)
        settings[name] = (flags, value)
    return settings, position


async def _send_query_to_buffer(
    monkeypatch,
    *,
    revision: int,
    settings: dict | None = None,
    params: dict | None = None,
) -> bytes:
    async def fake_write(self, revision):
        return None

    monkeypatch.setattr("asynch.proto.connection.ClientInfo.write", fake_write)

    conn = ProtoConnection(settings=settings or {})
    conn.connected = True
    conn.writer = BufferedWriter()
    conn.server_info = SimpleNamespace(revision=revision, used_revision=revision)

    await conn.send_query(
        "SELECT {value:String}",
        query_id="query-id",
        params=params,
    )
    return bytes(conn.writer.buffer)


def _skip_query_packet_to_parameters(buffer: bytes) -> tuple[dict[str, tuple[int, str]], int]:
    position = 0

    _, position = _read_varint(buffer, position)  # ClientPacket.QUERY
    _, position = _read_str(buffer, position)  # query_id
    _, position = _read_settings_as_strings(buffer, position)  # settings
    _, position = _read_str(buffer, position)  # interserver secret
    _, position = _read_varint(buffer, position)  # processing stage
    _, position = _read_varint(buffer, position)  # compression
    _, position = _read_str(buffer, position)  # query

    return _read_settings_as_strings(buffer, position)


def _skip_block_info(buffer: bytes, position: int) -> int:
    while True:
        field_num, position = _read_varint(buffer, position)
        if not field_num:
            return position
        if field_num == 1:
            position += 1
        elif field_num == 2:
            position += 4
        else:
            raise AssertionError(f"Unexpected block info field {field_num}")


async def _server_hello_packet(*, server_revision: int) -> bytes:
    writer = BufferedWriter()
    await writer.write_varint(ServerPacket.HELLO)
    await writer.write_str("ClickHouse")
    await writer.write_varint(24)
    await writer.write_varint(12)
    await writer.write_varint(server_revision)
    await writer.write_str("UTC")
    await writer.write_str("server")
    await writer.write_varint(1)
    await writer.write_varint(0)  # password complexity rules size
    await writer.write_uint64(42)  # interserver secret v2 nonce
    return bytes(writer.buffer)


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


def test_upstream_protocol_revision_matches_clickhouse_driver_0_2_10():
    assert constants.DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION == 54454
    assert constants.DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM == 54458
    assert constants.DBMS_MIN_PROTOCOL_VERSION_WITH_QUOTA_KEY == 54458
    assert constants.DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS == 54459
    assert constants.DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS == 54460
    assert constants.DBMS_MIN_PROTOCOL_VERSION_WITH_PASSWORD_COMPLEXITY_RULES == 54461
    assert constants.DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2 == 54462
    assert constants.DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS == 54463
    assert constants.DBMS_MIN_PROTOCOL_VERSION_WITH_TIMEZONE_UPDATES == 54464
    assert constants.DBMS_MIN_REVISION_WITH_SYSTEM_KEYWORDS_TABLE == 54468
    assert constants.CLIENT_REVISION == constants.DBMS_MIN_REVISION_WITH_SYSTEM_KEYWORDS_TABLE


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
async def test_upstream_receive_hello_tracks_used_revision():
    stream = asyncio.StreamReader()
    stream.feed_data(await _server_hello_packet(server_revision=54483))
    stream.feed_eof()

    conn = ProtoConnection()
    conn.reader = BufferedReader(stream)

    await conn.receive_hello()

    assert conn.server_info.revision == 54483
    assert conn.server_info.used_revision == constants.CLIENT_REVISION
    assert conn.reader.position == conn.reader.current_buffer_size


@pytest.mark.asyncio
async def test_upstream_addendum_writes_quota_key_for_revision_54458():
    conn = ProtoConnection(settings={"quota_key": "quota-1"})
    conn.writer = BufferedWriter()
    conn.server_info = SimpleNamespace(revision=54468, used_revision=54458)

    await conn.send_addendum()

    value, position = _read_str(bytes(conn.writer.buffer), 0)
    assert value == "quota-1"
    assert position == len(conn.writer.buffer)


@pytest.mark.asyncio
async def test_upstream_send_query_writes_empty_parameters_block_when_disabled(monkeypatch):
    buffer = await _send_query_to_buffer(
        monkeypatch,
        revision=54468,
        params={"value": "hello"},
    )

    settings, position = _skip_query_packet_to_parameters(buffer)

    assert settings == {}
    assert position == len(buffer)


@pytest.mark.asyncio
async def test_upstream_send_query_writes_server_side_parameters(monkeypatch):
    buffer = await _send_query_to_buffer(
        monkeypatch,
        revision=54468,
        settings={"server_side_params": True},
        params={"value": "hello"},
    )

    settings, position = _skip_query_packet_to_parameters(buffer)

    assert settings == {"value": (0x2, "'hello'")}
    assert position == len(buffer)


@pytest.mark.asyncio
async def test_upstream_send_query_omits_parameters_before_revision_54459(monkeypatch):
    buffer = await _send_query_to_buffer(
        monkeypatch,
        revision=54458,
        settings={"server_side_params": True},
        params={"value": "hello"},
    )

    position = 0
    _, position = _read_varint(buffer, position)  # ClientPacket.QUERY
    _, position = _read_str(buffer, position)  # query_id
    _, position = _read_settings_as_strings(buffer, position)  # settings
    _, position = _read_str(buffer, position)  # interserver secret
    _, position = _read_varint(buffer, position)  # processing stage
    _, position = _read_varint(buffer, position)  # compression
    _, position = _read_str(buffer, position)  # query

    assert position == len(buffer)


@pytest.mark.asyncio
async def test_upstream_block_writer_emits_custom_serialization_marker():
    context = Context()
    context.server_info = SimpleNamespace(used_revision=54468)
    context.client_settings = {"input_format_null_as_default": False}
    writer = BufferedWriter()
    block_writer = BlockWriter(None, writer, context)
    block = ColumnOrientedBlock(
        columns_with_types=[("value", "UInt8")],
        data=[(1,)],
    )

    await block_writer.write(block)

    buffer = bytes(writer.buffer)
    position = _skip_block_info(buffer, 0)
    _, position = _read_varint(buffer, position)  # n_columns
    _, position = _read_varint(buffer, position)  # n_rows
    _, position = _read_str(buffer, position)  # column name
    _, position = _read_str(buffer, position)  # column type

    assert buffer[position] == 0


def test_upstream_server_side_params_use_double_escaping():
    assert escape_params({"value": "\t"}, for_server=True) == {"value": "'\\\\t'"}
    assert escape_params({"value": "\\"}, for_server=True) == {"value": "'\\\\\\\\'"}
    assert escape_params({"value": "'"}, for_server=True) == {"value": "'\\\\\\''"}


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
