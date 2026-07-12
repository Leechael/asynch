import asyncio
import builtins
import importlib.util
import struct
from unittest.mock import AsyncMock, Mock

import pytest

from asynch.errors import ChecksumDoesntMatchError, SocketTimeoutError
from asynch.proto.compression import import_cityhash
from asynch.proto.compression.lz4 import Compressor as LZ4Compressor
from asynch.proto.connection import Connection as ProtoConnection
from asynch.proto.protocol import ClientPacket, CompressionMethodByte, ServerPacket
from asynch.proto.streams.buffered import (
    BufferedReader,
    BufferedWriter,
    CompressedBufferedReader,
)

pytestmark = pytest.mark.no_clickhouse


async def test_send_cancel_writes_cancel_packet():
    conn = ProtoConnection()
    conn.writer = BufferedWriter()

    await conn.send_cancel()

    assert conn.writer.buffer == bytes([ClientPacket.CANCEL])


async def test_cancelled_packet_generator_disconnects_connection():
    conn = ProtoConnection()
    conn.receive_packet = AsyncMock(side_effect=asyncio.CancelledError)
    conn.disconnect = AsyncMock()

    generator = conn.packet_generator()
    with pytest.raises(asyncio.CancelledError):
        await generator.__anext__()

    conn.disconnect.assert_awaited_once()


async def test_receive_packet_honors_send_receive_timeout():
    conn = ProtoConnection(send_receive_timeout=0.001)

    async def never_returns():
        await asyncio.sleep(1)

    conn._receive_packet_impl = never_returns

    with pytest.raises(SocketTimeoutError) as exc_info:
        await conn._receive_packet()

    assert isinstance(exc_info.value.__cause__, asyncio.TimeoutError)


async def test_ping_honors_sync_request_timeout():
    conn = ProtoConnection(sync_request_timeout=0.001)
    conn.connected = True
    conn.reader = Mock()
    conn.reader.reader.at_eof.return_value = False

    async def never_returns():
        await asyncio.sleep(1)

    conn.reader.read_varint = never_returns
    conn.writer = Mock()
    conn.writer.write_varint = AsyncMock()
    conn.writer.flush = AsyncMock()
    conn.writer.close = AsyncMock()

    assert await conn.ping() is False
    assert conn.connected is False


async def test_ping_timeout_covers_progress_payload():
    conn = ProtoConnection(sync_request_timeout=0.001)
    conn.connected = True
    conn.reader = Mock()
    conn.reader.reader.at_eof.return_value = False
    conn.reader.read_varint = AsyncMock(return_value=ServerPacket.PROGRESS)

    async def never_returns():
        await asyncio.sleep(1)

    conn.receive_progress = never_returns
    conn.writer = Mock()
    conn.writer.write_varint = AsyncMock()
    conn.writer.flush = AsyncMock()
    conn.writer.close = AsyncMock()

    assert await conn.ping() is False
    assert conn.connected is False


async def _compressed_frame(data: bytes) -> bytearray:
    compressor_writer = BufferedWriter()
    await compressor_writer.write_bytes(data)
    compressor = LZ4Compressor(compressor_writer)
    payload = bytearray([CompressionMethodByte.LZ4])
    payload.extend(await compressor.get_compressed_data(extra_header_size=1))

    frame = BufferedWriter()
    await frame.write_uint128(import_cityhash()(payload))
    await frame.write_bytes(payload)
    return frame.buffer


async def test_corrupt_compressed_checksum_is_rejected():
    frame = await _compressed_frame(b"checksum-protected payload")
    frame[0] ^= 0xFF
    stream = asyncio.StreamReader()
    stream.feed_data(frame)
    stream.feed_eof()
    raw_reader = BufferedReader(stream)
    reader = CompressedBufferedReader(raw_reader, stream)

    with pytest.raises(ChecksumDoesntMatchError):
        await reader.read_bytes(1)


async def test_truncated_fixed_width_value_is_rejected():
    stream = asyncio.StreamReader()
    stream.feed_data(b"\x01\x02")
    stream.feed_eof()
    reader = BufferedReader(stream)

    with pytest.raises(struct.error):
        await reader.read_uint64()


def test_cityhash_dependency_contract(monkeypatch):
    expected = __import__("os").environ.get("ASYNCH_EXPECT_CITYHASH", "1") == "1"
    if expected:
        assert callable(import_cityhash())
        return

    assert importlib.util.find_spec("clickhouse_cityhash") is None

    real_import = builtins.__import__

    def import_without_cityhash(name, *args, **kwargs):
        if name.startswith("clickhouse_cityhash"):
            raise ModuleNotFoundError(name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", import_without_cityhash)
    with pytest.raises(ImportError, match="install clickhouse-cityhash"):
        import_cityhash()
