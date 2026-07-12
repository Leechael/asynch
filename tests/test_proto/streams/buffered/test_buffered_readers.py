from asyncio import StreamReader

import pytest

from asynch.proto.compression import import_cityhash
from asynch.proto.compression.lz4 import Compressor as LZ4Compressor
from asynch.proto.protocol import CompressionMethodByte
from asynch.proto.result import ClientTimings
from asynch.proto.streams.buffered import (
    BufferedReader,
    BufferedWriter,
    CompressedBufferedReader,
    ReaderMetrics,
)

pytestmark = pytest.mark.no_clickhouse


@pytest.mark.parametrize(
    ("stream_data", "answer"),
    [
        (b"9", 57),
        (b"32", 51),
    ],
)
async def test_read_varint(stream_data: bytes, answer: bytes):
    """When `(b"", 0)`, the reading gets stuck."""

    stream_reader = StreamReader()
    stream_reader.feed_data(stream_data)
    reader = BufferedReader(stream_reader)

    result = await reader.read_varint()

    assert answer == result


@pytest.mark.parametrize(
    ("stream_data", "bytes_to_read", "answer"),
    [
        (b"", 0, b""),
        (b"02", 1, b"0"),
        (b"3456", 4, b"3456"),
    ],
)
async def test_read_bytes(stream_data: bytes, bytes_to_read: int, answer: bytes):
    """If `bytes_to_read > len(stream_data)`, the reading gets stuck."""

    stream_reader = StreamReader()
    stream_reader.feed_data(stream_data)
    reader = BufferedReader(stream_reader, 1)

    result = await reader.read_bytes(bytes_to_read)

    assert answer == result


async def test_reader_metrics_count_socket_reads_and_bytes():
    stream_reader = StreamReader()
    stream_reader.feed_data(b"abc")
    metrics = ReaderMetrics()
    reader = BufferedReader(stream_reader, buffer_max_size=1, metrics=metrics)

    assert await reader.read_bytes(3) == b"abc"
    assert metrics.socket_reads == 3
    assert metrics.bytes_read == 3
    assert metrics.network_wait >= 0


async def test_compressed_reader_does_not_double_count_raw_socket_reads():
    data = b"compressed metrics"
    compressor_writer = BufferedWriter()
    await compressor_writer.write_bytes(data)
    compressor = LZ4Compressor(compressor_writer)
    payload = bytearray([CompressionMethodByte.LZ4])
    payload.extend(await compressor.get_compressed_data(extra_header_size=1))

    frame_writer = BufferedWriter()
    await frame_writer.write_uint128(import_cityhash()(payload))
    await frame_writer.write_bytes(payload)
    frame = frame_writer.buffer

    stream_reader = StreamReader()
    stream_reader.feed_data(frame)
    stream_reader.feed_eof()
    metrics = ReaderMetrics()
    raw_reader = BufferedReader(stream_reader, buffer_max_size=len(frame), metrics=metrics)
    reader = CompressedBufferedReader(raw_reader, stream_reader)

    assert await reader.read_bytes(len(data)) == data
    assert reader.metrics is None
    assert metrics.socket_reads == 1
    assert metrics.bytes_read == len(frame)
    assert metrics.network_wait >= 0


async def test_compressed_reader_records_compressed_timing_and_sizes():
    data = b"compressed timing metrics"
    compressor_writer = BufferedWriter()
    await compressor_writer.write_bytes(data)
    compressor = LZ4Compressor(compressor_writer)
    payload = bytearray([CompressionMethodByte.LZ4])
    payload.extend(await compressor.get_compressed_data(extra_header_size=1))

    frame_writer = BufferedWriter()
    await frame_writer.write_uint128(import_cityhash()(payload))
    await frame_writer.write_bytes(payload)
    frame = frame_writer.buffer

    stream_reader = StreamReader()
    stream_reader.feed_data(frame)
    stream_reader.feed_eof()
    raw_metrics = ReaderMetrics()
    raw_reader = BufferedReader(stream_reader, buffer_max_size=len(frame), metrics=raw_metrics)
    reader = CompressedBufferedReader(raw_reader, stream_reader)
    timings = ClientTimings()
    reader.timings = timings

    assert await reader.read_bytes(len(data)) == data
    assert timings.decompress >= 0
    assert timings.bytes_compressed == len(payload) - 5
    assert timings.bytes_raw == len(data)
    assert raw_metrics.network_wait >= 0
