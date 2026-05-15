from __future__ import annotations

import asyncio
from types import SimpleNamespace

from asynch.proto import constants
from asynch.proto.connection import Connection as ProtoConnection
from asynch.proto.streams.buffered import BufferedReader, BufferedWriter


def make_buffered_reader(payload: bytes) -> BufferedReader:
    stream = asyncio.StreamReader()
    stream.feed_data(payload)
    stream.feed_eof()
    return BufferedReader(stream)


def make_revision_connection(
    revision: int,
    *,
    client_revision: int | None = None,
    settings: dict | None = None,
) -> ProtoConnection:
    conn = ProtoConnection(client_revision=client_revision, settings=settings or {})
    conn.connected = True
    conn.reader = make_buffered_reader(b"")
    conn.writer = BufferedWriter()
    conn.server_info = SimpleNamespace(revision=revision, used_revision=revision)
    conn.context.server_info = conn.server_info
    return conn


async def assert_reader_exhausted(reader: BufferedReader):
    await reader._refill_buffer()
    unread = reader.current_buffer_size - reader.position
    if unread:
        raise AssertionError(f"Reader has {unread} unread byte(s)")


async def assert_reader_has_unread(reader: BufferedReader, unread: int):
    await reader._refill_buffer()
    actual = reader.current_buffer_size - reader.position
    if actual != unread:
        raise AssertionError(f"Expected {unread} unread byte(s), got {actual}")


def get_writer_bytes(writer: BufferedWriter) -> bytes:
    return bytes(writer.buffer)


def revisions_around(gate: int) -> tuple[int, int]:
    return gate - 1, gate


def latest_revision() -> int:
    return constants.CLIENT_REVISION
