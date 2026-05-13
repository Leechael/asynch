import pytest

from asynch.proto import constants
from asynch.proto.streams.buffered import BufferedWriter
from tests.test_proto.protocol_helpers import (
    assert_reader_exhausted,
    assert_reader_has_unread,
    get_writer_bytes,
    make_buffered_reader,
    make_revision_connection,
    revisions_around,
)


@pytest.fixture(scope="session", autouse=True)
async def initialize_tests():
    yield


@pytest.fixture(scope="function", autouse=True)
async def truncate_table():
    yield


@pytest.mark.asyncio
async def test_make_revision_connection_forces_used_revision():
    conn = make_revision_connection(54470, client_revision=999999)

    assert conn.client_revision == constants.CLIENT_REVISION
    assert conn.server_info.used_revision == 54470
    assert conn.context.server_info.used_revision == 54470


@pytest.mark.asyncio
async def test_assert_reader_exhausted_accepts_aligned_stream():
    reader = make_buffered_reader(b"\x01")

    assert await reader.read_uint8() == 1
    await assert_reader_exhausted(reader)


@pytest.mark.asyncio
async def test_assert_reader_exhausted_catches_unread_bytes():
    reader = make_buffered_reader(b"\x01\x02")

    assert await reader.read_uint8() == 1

    with pytest.raises(AssertionError, match="1 unread byte"):
        await assert_reader_exhausted(reader)

    await assert_reader_has_unread(reader, 1)


@pytest.mark.asyncio
async def test_get_writer_bytes_returns_serialized_client_packet_bytes():
    writer = BufferedWriter()

    await writer.write_varint(1)
    await writer.write_str("query-id")

    assert get_writer_bytes(writer) == b"\x01\x08query-id"


def test_revisions_around_returns_old_and_new_boundary_values():
    assert revisions_around(54470) == (54469, 54470)
