import asyncio

import pytest

from asynch.proto import constants
from asynch.proto.progress import Progress
from asynch.proto.streams.buffered import BufferedReader, BufferedWriter


async def _progress_reader(*values: int) -> BufferedReader:
    writer = BufferedWriter()
    for value in values:
        await writer.write_varint(value)

    stream = asyncio.StreamReader()
    stream.feed_data(bytes(writer.buffer))
    stream.feed_eof()
    return BufferedReader(stream)


@pytest.fixture(scope="session", autouse=True)
async def initialize_tests():
    yield


@pytest.fixture(scope="function", autouse=True)
async def truncate_table():
    yield


@pytest.mark.asyncio
async def test_progress_reads_revision_54468_fields():
    progress = Progress(await _progress_reader(1, 2, 3, 4, 5, 6, 7))

    await progress.read(constants.CLIENT_REVISION)

    assert progress.rows == 1
    assert progress.bytes == 2
    assert progress.total_rows == 3
    assert progress.total_bytes == 4
    assert progress.written_rows == 5
    assert progress.written_bytes == 6
    assert progress.elapsed_ns == 7
