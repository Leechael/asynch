import asyncio
import logging
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import cast
from unittest.mock import AsyncMock, Mock, patch

import pytest

from asynch.errors import ErrorCode, NetworkError, ServerException, SocketTimeoutError
from asynch.proto.block import RowOrientedBlock
from asynch.proto.connection import METRICS_ENV, SUBSTITUTE_PARAMS_STYLE_ENV, Packet
from asynch.proto.connection import (
    Connection as ProtoConnection,
)
from asynch.proto.cs import ServerInfo
from asynch.proto.protocol import ServerPacket
from asynch.proto.result import ClientTimings, QueryInfo
from asynch.proto.streams.buffered import ReaderMetrics


@pytest.fixture()
async def proto_conn(config) -> AsyncIterator[ProtoConnection]:
    _conn = ProtoConnection(
        user=config.user,
        password=config.password,
        host=config.host,
        port=config.port,
        database=config.database,
        settings=config.settings,
    )
    await _conn.connect()
    yield _conn
    await _conn.disconnect()


@pytest.mark.asyncio
async def test_connect(proto_conn: ProtoConnection):
    assert proto_conn.connected

    server_info = cast(ServerInfo, proto_conn.server_info)
    assert server_info.name == "ClickHouse"
    assert server_info.timezone == "UTC"
    assert re.match(r"\w+", server_info.display_name)
    assert isinstance(server_info.version_patch, int)


@pytest.mark.asyncio
async def test_ping(proto_conn: ProtoConnection):
    await proto_conn.connect()
    assert await proto_conn.ping() is True


@pytest.mark.asyncio
async def test_ping_processing_with_invalid_package_size(proto_conn: ProtoConnection):
    with patch.object(
        proto_conn.reader, "_read_one", side_effect=IndexError("Empty bytes array")
    ) as mock:
        result = await proto_conn.ping()
        mock.assert_called_once()
        assert result is False


@pytest.mark.no_clickhouse
@pytest.mark.asyncio
async def test_ping_skips_socket_for_executing_query_inv_s6(caplog):
    caplog.set_level(logging.DEBUG, logger="asynch.proto.connection")
    conn = ProtoConnection()
    conn.connected = True
    conn.is_query_executing = True
    conn.writer = Mock()
    conn.writer.write_varint = AsyncMock()
    conn.writer.flush = AsyncMock()
    conn.reader = Mock(reader=Mock(at_eof=Mock(return_value=False)))
    conn.reader.read_varint = AsyncMock(return_value=ServerPacket.PONG)

    assert await conn.ping() is False
    conn.writer.write_varint.assert_not_awaited()
    assert "query is executing" in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exception",
    [
        pytest.param(ConnectionError("Any connection error"), id="any ConnectionError"),
        pytest.param(OSError("Any OS error"), id="any OSError"),
        pytest.param(
            RuntimeError(
                "RuntimeError: TCPTransport closed=True: localhost"
            ),  # Check parsing exc message
            id="RuntimeError with TCPTransport closed",
        ),
    ],
)
async def test_ping_catch_connection_error(proto_conn: ProtoConnection, exception: Exception):
    with patch.object(proto_conn.reader, "read_varint", side_effect=exception) as mock:
        result = await proto_conn.ping()
        mock.assert_called_once()
        assert result is False


@pytest.mark.asyncio
async def test_ping_raise_other_runtime_errors(proto_conn: ProtoConnection):
    with patch.object(
        proto_conn.reader, "read_varint", side_effect=RuntimeError("Any exception")
    ) as mock:
        with pytest.raises(RuntimeError, match="Any exception"):
            await proto_conn.ping()
        mock.assert_called_once()


@pytest.mark.no_clickhouse
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (OSError("network unavailable"), NetworkError),
        (asyncio.TimeoutError("connect timed out"), SocketTimeoutError),
    ],
)
async def test_connect_remaps_network_errors_inv_e1(error, expected):
    conn = ProtoConnection()
    conn._init_connection = AsyncMock(side_effect=error)

    with pytest.raises(expected) as exc_info:
        await conn.connect()

    assert exc_info.value.__cause__ is error


@pytest.mark.no_clickhouse
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (OSError("network unavailable"), NetworkError),
        (asyncio.TimeoutError("read timed out"), SocketTimeoutError),
    ],
)
async def test_receive_packet_remaps_network_errors_inv_e1(error, expected):
    conn = ProtoConnection()
    conn._receive_packet_impl = AsyncMock(side_effect=error)
    conn.disconnect = AsyncMock()

    with pytest.raises(expected) as exc_info:
        await conn._receive_packet()

    assert exc_info.value.__cause__ is error
    conn.disconnect.assert_awaited_once()


@pytest.mark.asyncio
async def test_execute(proto_conn: ProtoConnection):
    query = "SELECT 1"
    ret = await proto_conn.execute(query)
    assert ret == [(1,)]


@pytest.mark.asyncio
async def test_execute_with_args(proto_conn: ProtoConnection, monkeypatch):
    monkeypatch.setenv(SUBSTITUTE_PARAMS_STYLE_ENV, "format")

    query = "SELECT {val}"
    ret = await proto_conn.execute(query, args={"val": 2})
    assert ret == [(2,)]


@pytest.mark.asyncio
async def test_execute_with_pyformat_args(proto_conn: ProtoConnection, monkeypatch):
    monkeypatch.setenv(SUBSTITUTE_PARAMS_STYLE_ENV, "pyformat")

    query = "SELECT %(val)s"
    ret = await proto_conn.execute(query, args={"val": 2})
    assert ret == [(2,)]


@pytest.mark.asyncio
async def test_execute_with_missing_arg(proto_conn: ProtoConnection):
    query = "SELECT %(var)s"
    with pytest.raises(KeyError, match="'var'"):
        await proto_conn.execute(query, args={"foo": 1})


@pytest.mark.no_clickhouse
@pytest.mark.parametrize(
    ("environment", "expected"),
    [("1", True), ("true", True), ("ON", True), ("0", False)],
)
def test_metrics_environment_enablement(monkeypatch, environment, expected):
    monkeypatch.setenv(METRICS_ENV, environment)

    assert ProtoConnection().metrics_enabled is expected
    assert ProtoConnection(metrics=not expected).metrics_enabled is not expected


@pytest.mark.no_clickhouse
@pytest.mark.parametrize("buffer_size", [0, -1, True, "not-an-integer"])
def test_buffer_size_rejects_invalid_values(buffer_size):
    with pytest.raises(ValueError, match="buffer_size"):
        ProtoConnection(buffer_size=buffer_size)


@pytest.mark.no_clickhouse
def test_buffer_size_uses_environment_when_kwarg_is_omitted(monkeypatch):
    monkeypatch.setenv("ASYNCH_BUFFER_SIZE", "4096")

    assert ProtoConnection().buffer_size == 4096
    assert ProtoConnection(buffer_size=512).buffer_size == 512


@pytest.mark.no_clickhouse
@pytest.mark.parametrize("environment", ["0", "-1", "not-an-integer"])
def test_buffer_size_rejects_invalid_environment(monkeypatch, environment):
    monkeypatch.setenv("ASYNCH_BUFFER_SIZE", environment)

    with pytest.raises(ValueError, match="buffer_size"):
        ProtoConnection()


@pytest.mark.no_clickhouse
async def test_buffer_size_flows_to_all_connection_streams():
    conn = ProtoConnection(compression=True, buffer_size=256)
    conn.server_info = Mock(used_revision=0)
    stream_reader = asyncio.StreamReader()
    stream_writer = Mock()
    stream_writer.get_extra_info.return_value = None
    conn.send_hello = AsyncMock()
    conn.receive_hello = AsyncMock()

    with patch(
        "asynch.proto.connection.asyncio.open_connection",
        new=AsyncMock(return_value=(stream_reader, stream_writer)),
    ):
        await conn._init_connection("localhost", 9000)

    assert conn.reader.buffer_max_size == 256
    assert conn.writer.max_buffer_size == 256
    assert conn.block_reader.reader.buffer_max_size == 256
    assert conn.block_writer.writer.max_buffer_size == 256


@pytest.mark.no_clickhouse
async def test_receive_data_records_client_timings():
    conn = ProtoConnection(metrics=True)
    reader_metrics = ReaderMetrics()
    conn.reader = Mock(metrics=reader_metrics)
    conn.server_info = Mock(used_revision=0)
    block = RowOrientedBlock(
        columns_with_types=[("value", "UInt8")],
        data=[(1,), (2,)],
    )

    async def read_block():
        reader_metrics.network_wait += 0.1
        reader_metrics.bytes_read += 9
        return block

    conn.block_reader = Mock(read=AsyncMock(side_effect=read_block))
    conn.last_query = QueryInfo(conn.reader, client_timings=ClientTimings())

    assert await conn.receive_data() is block
    timings = conn.last_query.client_timings
    assert timings.network_wait == 0.1
    assert timings.blocks == 1
    assert timings.rows == 2
    assert timings.bytes_raw == 9
    assert timings.decode >= 0
    assert timings.max_block_decode == timings.decode


@pytest.mark.no_clickhouse
async def test_execute_client_timings_do_not_exceed_elapsed():
    conn = ProtoConnection(metrics=True)
    conn.reader = Mock()
    conn.force_connect = AsyncMock()
    conn.process_ordinary_query = AsyncMock(return_value=[])

    assert await conn.execute("SELECT 1") == []
    timings = conn.last_query.client_timings
    assert timings is not None
    assert timings.network_wait + timings.decode <= conn.last_query.elapsed


@pytest.mark.no_clickhouse
async def test_packet_generator_yields_to_the_event_loop_after_data_blocks():
    conn = ProtoConnection()
    packet = Packet()
    packet.type = ServerPacket.DATA
    conn.receive_packet = AsyncMock(side_effect=[packet, False])

    with patch("asynch.proto.connection.asyncio.sleep", new=AsyncMock()) as sleep:
        assert [item async for item in conn.packet_generator()] == [packet]

    sleep.assert_awaited_once_with(0)


@pytest.mark.no_clickhouse
async def test_first_data_packet_records_ttfb():
    conn = ProtoConnection(metrics=True)
    conn.reader = Mock(read_varint=AsyncMock(return_value=ServerPacket.DATA))
    conn.last_query = QueryInfo(conn.reader, client_timings=ClientTimings())
    conn._metrics_query_sent_at = 0.0
    block = RowOrientedBlock()
    conn.receive_data = AsyncMock(return_value=block)

    packet = await conn._receive_packet_impl()

    assert packet.block is block
    assert conn.last_query.client_timings.ttfb >= 0
    assert conn._metrics_query_sent_at is None


def test_substitute_params_supports_format_style(monkeypatch):
    monkeypatch.setenv(SUBSTITUTE_PARAMS_STYLE_ENV, "format")

    query = "SELECT {value}, {name}"
    params = {"value": 1, "name": "hello"}

    assert ProtoConnection.substitute_params(query, params) == "SELECT 1, 'hello'"


def test_substitute_params_supports_pyformat_style(monkeypatch):
    monkeypatch.delenv(SUBSTITUTE_PARAMS_STYLE_ENV, raising=False)

    query = "SELECT %(value)s, %(name)s"
    params = {"value": 1, "name": "hello"}

    assert ProtoConnection.substitute_params(query, params) == "SELECT 1, 'hello'"


def test_substitute_params_rejects_unknown_style(monkeypatch):
    monkeypatch.setenv(SUBSTITUTE_PARAMS_STYLE_ENV, "unknown")

    with pytest.raises(ValueError, match=SUBSTITUTE_PARAMS_STYLE_ENV):
        ProtoConnection.substitute_params("SELECT {value}", {"value": 1})


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        (
            "INSERT INTO test.test (a, b) VALUES (%(a)s, %(b)s)",
            "INSERT INTO test.test (a, b) VALUES",
        ),
        (
            "insert into test.test (a, b) values (:a, :b);",
            "insert into test.test (a, b) values",
        ),
        (
            "INSERT INTO test.test (a, b) VALUES (?, ?)",
            "INSERT INTO test.test (a, b) VALUES",
        ),
        (
            "INSERT INTO test.test (a, b) VALUES (1, %(b)s)",
            "INSERT INTO test.test (a, b) VALUES (1, %(b)s)",
        ),
    ],
)
def test_normalize_insert_query_for_data_strips_dbapi_placeholder_template(
    query,
    expected,
):
    assert ProtoConnection.normalize_insert_query_for_data(query) == expected


@pytest.mark.asyncio
async def test_process_insert_query_drains_packets_until_end_of_stream():
    proto_conn = ProtoConnection()
    sample_block = RowOrientedBlock(columns_with_types=[("value", "UInt8")])

    proto_conn.send_query = AsyncMock()
    proto_conn.send_external_tables = AsyncMock()
    proto_conn.receive_sample_block = AsyncMock(return_value=sample_block)
    proto_conn.send_data = AsyncMock(return_value=1)
    proto_conn.receive_packet = AsyncMock(side_effect=[True, True, False])

    result = await proto_conn.process_insert_query(
        "INSERT INTO test.test VALUES",
        [(1,)],
    )

    assert result == 1
    assert proto_conn.receive_packet.await_count == 3
    proto_conn.send_data.assert_awaited_once_with(
        sample_block, [(1,)], types_check=False, columnar=False
    )


@pytest.mark.asyncio
async def test_process_insert_query_sends_normalized_dbapi_placeholder_template():
    proto_conn = ProtoConnection()
    sample_block = RowOrientedBlock(columns_with_types=[("a", "UInt8"), ("b", "String")])

    proto_conn.send_query = AsyncMock()
    proto_conn.send_external_tables = AsyncMock()
    proto_conn.receive_sample_block = AsyncMock(return_value=sample_block)
    proto_conn.send_data = AsyncMock(return_value=1)
    proto_conn.receive_packet = AsyncMock(return_value=False)

    result = await proto_conn.process_insert_query(
        "INSERT INTO test.test (a, b) VALUES (%(a)s, %(b)s)",
        [{"a": 1, "b": "one"}],
    )

    assert result == 1
    proto_conn.send_query.assert_awaited_once_with(
        "INSERT INTO test.test (a, b) VALUES",
        query_id=None,
    )
    proto_conn.send_data.assert_awaited_once_with(
        sample_block,
        [{"a": 1, "b": "one"}],
        types_check=False,
        columnar=False,
    )


@pytest.mark.asyncio
async def test_process_insert_query_returns_after_end_of_stream():
    proto_conn = ProtoConnection()
    sample_block = RowOrientedBlock(columns_with_types=[("value", "UInt8")])

    proto_conn.send_query = AsyncMock()
    proto_conn.send_external_tables = AsyncMock()
    proto_conn.receive_sample_block = AsyncMock(return_value=sample_block)
    proto_conn.send_data = AsyncMock(return_value=1)
    proto_conn.receive_packet = AsyncMock(return_value=False)

    result = await proto_conn.process_insert_query(
        "INSERT INTO test.test VALUES",
        [(1,)],
    )

    assert result == 1
    proto_conn.receive_packet.assert_awaited_once()


@pytest.mark.asyncio
async def test_process_insert_query_propagates_server_exception_while_draining():
    proto_conn = ProtoConnection()
    sample_block = RowOrientedBlock(columns_with_types=[("value", "UInt8")])
    exception = RuntimeError("insert failed")

    proto_conn.send_query = AsyncMock()
    proto_conn.send_external_tables = AsyncMock()
    proto_conn.receive_sample_block = AsyncMock(return_value=sample_block)
    proto_conn.send_data = AsyncMock(return_value=1)
    proto_conn.receive_packet = AsyncMock(side_effect=[True, exception])

    with pytest.raises(RuntimeError, match="insert failed"):
        await proto_conn.process_insert_query(
            "INSERT INTO test.test VALUES",
            [(1,)],
        )

    assert proto_conn.receive_packet.await_count == 2


@pytest.mark.asyncio
async def test_process_insert_query_without_sample_block_does_not_send_data():
    proto_conn = ProtoConnection()

    proto_conn.send_query = AsyncMock()
    proto_conn.send_external_tables = AsyncMock()
    proto_conn.receive_sample_block = AsyncMock(return_value=None)
    proto_conn.send_data = AsyncMock()
    proto_conn.receive_packet = AsyncMock()

    result = await proto_conn.process_insert_query(
        "INSERT INTO test.test VALUES",
        [(1,)],
    )

    assert result is None
    proto_conn.send_data.assert_not_awaited()
    proto_conn.receive_packet.assert_not_awaited()


@asynccontextmanager
async def create_table(connection, spec):
    await connection.execute("DROP TABLE IF EXISTS test.test")
    await connection.execute(f"CREATE TABLE test.test ({spec}) engine=Memory")

    try:
        yield
    finally:
        await connection.execute("DROP TABLE test.test")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "spec, data, expected",
    [
        ("a Int8, b String", [(None, None)], [(0, "")]),
        ("a LowCardinality(String)", [(None,)], [("",)]),
        ("a Tuple(Int32, Int32)", [(None,)], [((0, 0),)]),
        ("a Array(Array(Int32))", [(None,)], [([],)]),
        ("a Map(String, UInt64)", [(None,)], [({},)]),
        ("a Nested(i Int32)", [(None,)], [([],)]),
    ],
    ids=[
        "int and string",
        "lowcardinaly string",
        "tuple",
        "array",
        "map",
        "nested",
    ],
)
async def test_input_format_null_as_default(proto_conn, spec, data, expected):
    for enabled in (True, False):
        proto_conn.client_settings["input_format_null_as_default"] = enabled

        async with create_table(proto_conn, spec):
            try:
                await proto_conn.execute("INSERT INTO test.test VALUES", data)
            except:  # noqa
                assert not enabled
                return

            assert await proto_conn.execute("SELECT * FROM test.test") == expected


@pytest.mark.asyncio
async def test_live_view_requires_explicit_enablement(proto_conn: ProtoConnection) -> None:
    await proto_conn.execute("DROP TABLE IF EXISTS test.test")
    await proto_conn.execute("CREATE TABLE test.test (x Int8) ENGINE=Memory;")
    await proto_conn.execute("SET allow_experimental_live_view = 0")
    await proto_conn.execute("DROP VIEW IF EXISTS lv")
    try:
        await proto_conn.execute("CREATE LIVE VIEW lv AS SELECT sum(x) FROM test.test")
    except ServerException as exc:
        if exc.code == ErrorCode.SYNTAX_ERROR and "LIVE VIEW" in exc.message:
            pytest.skip("ClickHouse no longer supports LIVE VIEW")
        assert exc.code == ErrorCode.SUPPORT_IS_DISABLED
    else:
        pytest.fail("LIVE VIEW should require explicit enablement")


@pytest.mark.asyncio
async def test_watch_zero_limit(proto_conn: ProtoConnection) -> None:
    await proto_conn.execute("DROP TABLE IF EXISTS test.test")
    await proto_conn.execute("CREATE TABLE test.test (x Int8) ENGINE=Memory;")
    await proto_conn.execute("SET allow_experimental_live_view = 1")
    await proto_conn.execute("DROP VIEW IF EXISTS lv")
    try:
        await proto_conn.execute("CREATE LIVE VIEW lv AS SELECT sum(x) FROM test.test")
    except ServerException as exc:
        if exc.code == ErrorCode.SYNTAX_ERROR and "LIVE VIEW" in exc.message:
            pytest.skip("ClickHouse no longer supports LIVE VIEW")
        raise
    await proto_conn.execute("INSERT INTO test.test VALUES (10)")

    results = await proto_conn.execute_iter("WATCH lv LIMIT 0")
    async for data in results:
        assert data == (10, 1)
