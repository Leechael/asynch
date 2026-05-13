import asyncio

import pytest

from asynch.proto.streams.buffered import BufferedReader, BufferedWriter


@pytest.fixture(scope="session", autouse=True)
async def initialize_tests():
    yield


@pytest.fixture(scope="function", autouse=True)
async def truncate_table():
    yield


@pytest.mark.asyncio
async def test_check_not_negative():
    value = 0x9FFFFFFF

    writer = BufferedWriter()
    await writer.write_varint(value)
    data = bytes(writer.buffer)
    assert data == b"\xff\xff\xff\xff\t"

    stream = asyncio.StreamReader()
    stream.feed_data(data)
    stream.feed_eof()
    reader = BufferedReader(stream)
    assert await reader.read_varint() == value
