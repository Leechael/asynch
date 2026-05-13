import asyncio
import ssl
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import leb128
import pytest

from asynch.errors import UnexpectedPacketFromServerError
from asynch.proto import constants
from asynch.proto.block import BlockInfo, ColumnOrientedBlock, RowOrientedBlock
from asynch.proto.columns.datetimecolumn import create_datetime_column
from asynch.proto.connection import SUBSTITUTE_PARAMS_STYLE_ENV
from asynch.proto.connection import Connection as ProtoConnection
from asynch.proto.context import Context
from asynch.proto.cs import ClientInfo, QueryKind
from asynch.proto.protocol import ClientPacket, ServerPacket
from asynch.proto.streams.block import BlockWriter
from asynch.proto.streams.buffered import BufferedReader, BufferedWriter
from asynch.proto.utils.dsn import parse_dsn
from asynch.proto.utils.escape import escape_params
from tests.test_proto.protocol_helpers import assert_reader_exhausted


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


def _read_settings_as_strings(
    buffer: bytes, position: int
) -> tuple[dict[str, tuple[int, str]], int]:
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


async def _client_info_bytes(revision: int) -> bytes:
    conn = ProtoConnection()
    writer = BufferedWriter()
    client_info = ClientInfo(conn.client_name, writer, conn.context)
    client_info.query_kind = QueryKind.INITIAL_QUERY

    await client_info.write(revision)

    return bytes(writer.buffer)


def _skip_client_info_to_script_numbers(buffer: bytes) -> int:
    position = 0

    position += 1  # query_kind
    _, position = _read_str(buffer, position)  # initial_user
    _, position = _read_str(buffer, position)  # initial_query_id
    _, position = _read_str(buffer, position)  # initial_address
    position += 8  # initial_query_start_time_microseconds
    position += 1  # interface
    _, position = _read_str(buffer, position)  # os_user
    _, position = _read_str(buffer, position)  # client_hostname
    _, position = _read_str(buffer, position)  # client_name
    _, position = _read_varint(buffer, position)  # client_version_major
    _, position = _read_varint(buffer, position)  # client_version_minor
    _, position = _read_varint(buffer, position)  # client_revision
    _, position = _read_str(buffer, position)  # quota_key
    _, position = _read_varint(buffer, position)  # distributed_depth
    _, position = _read_varint(buffer, position)  # client_version_patch
    position += 1  # have_opentelemetry
    _, position = _read_varint(buffer, position)  # collaborate_with_initiator
    _, position = _read_varint(buffer, position)  # count_participating_replicas
    _, position = _read_varint(buffer, position)  # number_of_current_replica
    return position


async def _server_hello_packet(
    *,
    server_revision: int,
    used_revision: int | None = None,
) -> bytes:
    used_revision = constants.CLIENT_REVISION if used_revision is None else used_revision
    writer = BufferedWriter()
    await writer.write_varint(ServerPacket.HELLO)
    await writer.write_str("ClickHouse")
    await writer.write_varint(24)
    await writer.write_varint(12)
    await writer.write_varint(server_revision)
    if used_revision >= constants.DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL:
        await writer.write_varint(constants.DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION)
    await writer.write_str("UTC")
    await writer.write_str("server")
    await writer.write_varint(1)
    if used_revision >= constants.DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS:
        await writer.write_str("notchunked")
        await writer.write_str("notchunked")
    await writer.write_varint(0)  # password complexity rules size
    await writer.write_uint64(42)  # interserver secret v2 nonce
    if used_revision >= constants.DBMS_MIN_REVISION_WITH_SERVER_SETTINGS:
        await writer.write_str("max_threads")
        await writer.write_uint8(0)
        await writer.write_str("8")
        await writer.write_str("")
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
    assert constants.DBMS_MIN_REVISION_WITH_TABLES_STATUS == 54226
    assert constants.DBMS_MIN_REVISION_WITH_TIME_ZONE_PARAMETER_IN_DATETIME_DATA_TYPE == 54337
    assert constants.DBMS_MIN_REVISION_WITH_LOW_CARDINALITY_TYPE == 54405
    assert constants.DBMS_MIN_REVISION_WITH_X_FORWARDED_FOR_IN_CLIENT_INFO == 54443
    assert constants.DBMS_MIN_REVISION_WITH_REFERER_IN_CLIENT_INFO == 54447
    assert constants.DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION == 54454
    assert constants.DBMS_MIN_PROTOCOL_VERSION_WITH_VIEW_IF_PERMITTED == 54457
    assert constants.DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM == 54458
    assert constants.DBMS_MIN_PROTOCOL_VERSION_WITH_QUOTA_KEY == 54458
    assert constants.DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS == 54459
    assert constants.DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS == 54460
    assert constants.DBMS_MIN_PROTOCOL_VERSION_WITH_PASSWORD_COMPLEXITY_RULES == 54461
    assert constants.DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2 == 54462
    assert constants.DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS == 54463
    assert constants.DBMS_MIN_PROTOCOL_VERSION_WITH_TIMEZONE_UPDATES == 54464
    assert constants.DBMS_MIN_REVISION_WITH_SPARSE_SERIALIZATION == 54465
    assert constants.DBMS_MIN_REVISION_WITH_SSH_AUTHENTICATION == 54466
    assert constants.DBMS_MIN_REVISION_WITH_TABLE_READ_ONLY_CHECK == 54467
    assert constants.DBMS_MIN_REVISION_WITH_SYSTEM_KEYWORDS_TABLE == 54468
    assert constants.DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS == 54470
    assert constants.DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION == 7
    assert constants.DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL == 54471
    assert constants.DBMS_MIN_PROTOCOL_VERSION_WITH_INTERSERVER_EXTERNALLY_GRANTED_ROLES == 54472
    assert constants.DBMS_MIN_REVISION_WITH_V2_DYNAMIC_AND_JSON_SERIALIZATION == 54473
    assert constants.DBMS_MIN_REVISION_WITH_SERVER_SETTINGS == 54474
    assert constants.DBMS_MIN_REVISION_WITH_QUERY_AND_LINE_NUMBERS == 54475
    assert constants.DBMS_MIN_REVISON_WITH_JWT_IN_INTERSERVER == 54476
    assert constants.CLIENT_REVISION == constants.DBMS_MIN_REVISION_WITH_SYSTEM_KEYWORDS_TABLE


def test_upstream_packet_type_names_cover_protocol_54468_packets():
    assert ClientPacket.to_str(ClientPacket.KEEP_ALIVE) == "KeepAlive"
    assert ClientPacket.to_str(ClientPacket.SCALAR) == "Scalar"
    assert ClientPacket.to_str(ClientPacket.IGNORED_PART_UUIDS) == "IgnoredPartUUIDs"
    assert ClientPacket.to_str(ClientPacket.READ_TASK_RESPONSE) == "ReadTaskResponse"
    assert (
        ClientPacket.to_str(ClientPacket.MERGE_TREE_READ_TASK_RESPONSE)
        == "MergeTreeReadTaskResponse"
    )
    assert ClientPacket.to_str(ClientPacket.SSH_CHALLENGE_REQUEST) == "SSHChallengeRequest"
    assert ClientPacket.to_str(ClientPacket.SSH_CHALLENGE_RESPONSE) == "SSHChallengeResponse"
    assert ServerPacket.to_str(ServerPacket.SSH_CHALLENGE) == "SSHChallenge"


def test_upstream_client_revision_is_capped_to_supported_revision():
    conn = ProtoConnection(client_revision=999999)

    assert conn.client_revision == constants.CLIENT_REVISION


def test_upstream_dsn_connection_options_are_not_server_settings():
    config = parse_dsn(
        "clickhouse://host?"
        "tcp_keepalive=10,20,30&"
        "round_robin=1&"
        "check_hostname=false&"
        "server_hostname=db.internal&"
        "keyfile=/tmp/key&"
        "keypass=pw&"
        "certfile=/tmp/cert&"
        "use_numpy=true&"
        "max_block_size=123"
    )

    assert config["tcp_keepalive"] == (10, 20, 30)
    assert config["round_robin"] is True
    assert config["check_hostname"] is False
    assert config["server_hostname"] == "db.internal"
    assert config["keyfile"] == "/tmp/key"
    assert config["keypass"] == "pw"
    assert config["certfile"] == "/tmp/cert"
    assert config["settings"] == {"use_numpy": True, "max_block_size": "123"}


def test_upstream_server_side_params_is_client_setting():
    conn = ProtoConnection(settings={"server_side_params": True})

    assert conn.client_settings["server_side_params"] is True
    assert "server_side_params" not in conn.settings


@pytest.mark.filterwarnings("ignore:ssl.PROTOCOL_TLSv1_2 is deprecated:DeprecationWarning")
def test_upstream_ssl_context_uses_configured_protocol_version():
    conn = ProtoConnection(
        secure=True,
        verify=False,
        check_hostname=True,
        ssl_version=ssl.PROTOCOL_TLSv1_2,
    )

    context = conn._get_ssl_context()

    assert context.protocol == ssl.PROTOCOL_TLSv1_2
    assert context.verify_mode == ssl.CERT_NONE
    assert context.check_hostname is False


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
async def test_upstream_client_info_omits_query_numbers_before_revision_54475():
    buffer = await _client_info_bytes(constants.DBMS_MIN_REVISION_WITH_SERVER_SETTINGS)

    position = _skip_client_info_to_script_numbers(buffer)

    assert position == len(buffer)


@pytest.mark.asyncio
async def test_upstream_client_info_writes_query_numbers_at_revision_54475():
    buffer = await _client_info_bytes(constants.DBMS_MIN_REVISION_WITH_QUERY_AND_LINE_NUMBERS)

    position = _skip_client_info_to_script_numbers(buffer)
    script_query_number, position = _read_varint(buffer, position)
    script_line_number, position = _read_varint(buffer, position)

    assert script_query_number == 0
    assert script_line_number == 0
    assert position == len(buffer)


@pytest.mark.asyncio
async def test_upstream_client_info_omits_jwt_marker_before_revision_54476():
    buffer = await _client_info_bytes(constants.DBMS_MIN_REVISION_WITH_QUERY_AND_LINE_NUMBERS)

    position = _skip_client_info_to_script_numbers(buffer)
    _, position = _read_varint(buffer, position)  # script_query_number
    _, position = _read_varint(buffer, position)  # script_line_number

    assert position == len(buffer)


@pytest.mark.asyncio
async def test_upstream_client_info_writes_empty_jwt_marker_at_revision_54476():
    buffer = await _client_info_bytes(constants.DBMS_MIN_REVISON_WITH_JWT_IN_INTERSERVER)

    position = _skip_client_info_to_script_numbers(buffer)
    _, position = _read_varint(buffer, position)  # script_query_number
    _, position = _read_varint(buffer, position)  # script_line_number
    jwt_marker = buffer[position]
    position += 1

    assert jwt_marker == 0
    assert position == len(buffer)


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
async def test_upstream_receive_hello_reads_chunked_capabilities_at_revision_54470():
    stream = asyncio.StreamReader()
    stream.feed_data(
        await _server_hello_packet(
            server_revision=54470,
            used_revision=constants.DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS,
        )
    )
    stream.feed_eof()

    conn = ProtoConnection()
    conn.client_revision = constants.DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS
    conn.reader = BufferedReader(stream)

    await conn.receive_hello()

    assert conn.server_info.used_revision == constants.DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS
    await assert_reader_exhausted(conn.reader)


@pytest.mark.asyncio
async def test_upstream_receive_hello_reads_parallel_replicas_version_at_revision_54471():
    stream = asyncio.StreamReader()
    stream.feed_data(
        await _server_hello_packet(
            server_revision=constants.DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL,
            used_revision=constants.DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL,
        )
    )
    stream.feed_eof()

    conn = ProtoConnection()
    conn.client_revision = constants.DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL
    conn.reader = BufferedReader(stream)

    await conn.receive_hello()

    assert (
        conn.server_info.used_revision
        == constants.DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL
    )
    await assert_reader_exhausted(conn.reader)


@pytest.mark.asyncio
async def test_upstream_receive_hello_reads_server_settings_at_revision_54474():
    stream = asyncio.StreamReader()
    stream.feed_data(
        await _server_hello_packet(
            server_revision=constants.DBMS_MIN_REVISION_WITH_SERVER_SETTINGS,
            used_revision=constants.DBMS_MIN_REVISION_WITH_SERVER_SETTINGS,
        )
    )
    stream.feed_eof()

    conn = ProtoConnection()
    conn.client_revision = constants.DBMS_MIN_REVISION_WITH_SERVER_SETTINGS
    conn.reader = BufferedReader(stream)

    await conn.receive_hello()

    assert conn.server_info.used_revision == constants.DBMS_MIN_REVISION_WITH_SERVER_SETTINGS
    await assert_reader_exhausted(conn.reader)


@pytest.mark.asyncio
async def test_upstream_connect_tries_alt_hosts_after_failure():
    conn = ProtoConnection(host="primary", alt_hosts="secondary:9001")
    attempts = []

    async def fake_init(host, port):
        attempts.append((host, port))
        if host == "primary":
            raise OSError("primary unavailable")
        conn.connected = True
        return "connected"

    conn._init_connection = fake_init

    assert await conn.connect() == "connected"
    assert attempts == [("primary", 9000), ("secondary", 9001)]


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
async def test_upstream_addendum_omits_chunked_capabilities_before_revision_54470():
    conn = ProtoConnection(settings={"quota_key": "quota-1"})
    conn.writer = BufferedWriter()
    conn.server_info = SimpleNamespace(revision=54469, used_revision=54469)

    await conn.send_addendum()

    value, position = _read_str(bytes(conn.writer.buffer), 0)
    assert value == "quota-1"
    assert position == len(conn.writer.buffer)


@pytest.mark.asyncio
async def test_upstream_addendum_writes_chunked_capabilities_at_revision_54470():
    conn = ProtoConnection(settings={"quota_key": "quota-1"})
    conn.writer = BufferedWriter()
    conn.server_info = SimpleNamespace(
        revision=constants.DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS,
        used_revision=constants.DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS,
    )

    await conn.send_addendum()

    position = 0
    value, position = _read_str(bytes(conn.writer.buffer), position)
    send_capability, position = _read_str(bytes(conn.writer.buffer), position)
    recv_capability, position = _read_str(bytes(conn.writer.buffer), position)

    assert value == "quota-1"
    assert send_capability == "notchunked"
    assert recv_capability == "notchunked"
    assert position == len(conn.writer.buffer)


@pytest.mark.asyncio
async def test_upstream_addendum_writes_parallel_replicas_version_at_revision_54471():
    conn = ProtoConnection(settings={"quota_key": "quota-1"})
    conn.writer = BufferedWriter()
    conn.server_info = SimpleNamespace(
        revision=constants.DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL,
        used_revision=constants.DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL,
    )

    await conn.send_addendum()

    position = 0
    value, position = _read_str(bytes(conn.writer.buffer), position)
    send_capability, position = _read_str(bytes(conn.writer.buffer), position)
    recv_capability, position = _read_str(bytes(conn.writer.buffer), position)
    protocol_version, position = _read_varint(bytes(conn.writer.buffer), position)

    assert value == "quota-1"
    assert send_capability == "notchunked"
    assert recv_capability == "notchunked"
    assert protocol_version == constants.DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION
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
async def test_upstream_send_query_omits_external_roles_before_revision_54472(monkeypatch):
    buffer = await _send_query_to_buffer(
        monkeypatch,
        revision=constants.DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL,
    )

    position = 0
    _, position = _read_varint(buffer, position)  # ClientPacket.QUERY
    _, position = _read_str(buffer, position)  # query_id
    _, position = _read_settings_as_strings(buffer, position)  # settings
    interserver_secret, position = _read_str(buffer, position)
    stage, position = _read_varint(buffer, position)

    assert interserver_secret == ""
    assert stage == 2


@pytest.mark.asyncio
async def test_upstream_send_query_writes_external_roles_at_revision_54472(monkeypatch):
    buffer = await _send_query_to_buffer(
        monkeypatch,
        revision=constants.DBMS_MIN_PROTOCOL_VERSION_WITH_INTERSERVER_EXTERNALLY_GRANTED_ROLES,
    )

    position = 0
    _, position = _read_varint(buffer, position)  # ClientPacket.QUERY
    _, position = _read_str(buffer, position)  # query_id
    _, position = _read_settings_as_strings(buffer, position)  # settings
    external_roles, position = _read_str(buffer, position)
    interserver_secret, position = _read_str(buffer, position)
    stage, position = _read_varint(buffer, position)

    assert external_roles == ""
    assert interserver_secret == ""
    assert stage == 2


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


@pytest.mark.asyncio
async def test_upstream_block_info_reads_bucket_num_as_signed_int32():
    writer = BufferedWriter()
    info = BlockInfo()
    info.bucket_num = -1

    await info.write(writer)

    stream = asyncio.StreamReader()
    stream.feed_data(bytes(writer.buffer))
    stream.feed_eof()
    reader = BufferedReader(stream)
    decoded = BlockInfo()

    await decoded.read(reader)

    assert decoded.bucket_num == -1


@pytest.mark.asyncio
async def test_upstream_receive_timezone_update_packet():
    writer = BufferedWriter()
    await writer.write_varint(ServerPacket.TIMEZONE_UPDATE)
    await writer.write_str("Asia/Taipei")
    stream = asyncio.StreamReader()
    stream.feed_data(bytes(writer.buffer))
    stream.feed_eof()

    conn = ProtoConnection()
    conn.reader = BufferedReader(stream)
    conn.server_info = SimpleNamespace(session_timezone=None)

    packet = await conn._receive_packet()

    assert packet.type == ServerPacket.TIMEZONE_UPDATE
    assert conn.server_info.session_timezone == "Asia/Taipei"


@pytest.mark.asyncio
async def test_upstream_part_uuids_packet_reads_uuid_vector():
    uuids = [
        UUID("c0fcbba9-0752-44ed-a5d6-4dfb4342b89d"),
        UUID("2efcead4-ff55-4db5-bdb4-6b36a308d8e0"),
    ]
    writer = BufferedWriter()
    await writer.write_varint(ServerPacket.PART_UUIDS)
    await writer.write_varint(len(uuids))
    for uuid in uuids:
        await writer.write_uint128(uuid.int)

    stream = asyncio.StreamReader()
    stream.feed_data(bytes(writer.buffer))
    stream.feed_eof()

    conn = ProtoConnection()
    conn.reader = BufferedReader(stream)

    packet = await conn._receive_packet()

    assert packet.type == ServerPacket.PART_UUIDS
    assert packet.part_uuids == uuids
    assert packet.block is None


@pytest.mark.asyncio
async def test_upstream_read_task_request_packet_has_no_block_payload():
    writer = BufferedWriter()
    await writer.write_varint(ServerPacket.READ_TASK_REQUEST)

    stream = asyncio.StreamReader()
    stream.feed_data(bytes(writer.buffer))
    stream.feed_eof()

    conn = ProtoConnection()
    conn.reader = BufferedReader(stream)

    packet = await conn._receive_packet()

    assert packet.type == ServerPacket.READ_TASK_REQUEST
    assert packet.read_task_request is True
    assert packet.block is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "packet_type",
    [
        ServerPacket.MERGE_TREE_ALL_RANGES_ANNOUNCEMENT,
        ServerPacket.MERGE_TREE_READ_TASK_REQUEST,
    ],
)
async def test_upstream_parallel_read_packets_fail_before_payload_is_misread(packet_type):
    writer = BufferedWriter()
    await writer.write_varint(packet_type)

    stream = asyncio.StreamReader()
    stream.feed_data(bytes(writer.buffer))
    stream.feed_eof()

    conn = ProtoConnection()
    conn.reader = BufferedReader(stream)

    with pytest.raises(UnexpectedPacketFromServerError, match="Unsupported packet"):
        await conn._receive_packet()


def test_upstream_datetime_column_uses_session_timezone(monkeypatch):
    monkeypatch.setattr(
        "asynch.proto.columns.datetimecolumn.get_localzone",
        lambda: "Local/Zone",
    )
    context = Context()
    context.settings = {}
    context.client_settings = {"input_format_null_as_default": False}
    context.server_info = SimpleNamespace(
        timezone="UTC",
        get_timezone=lambda: "Europe/Berlin",
    )

    column = create_datetime_column(
        "DateTime",
        {"context": context, "reader": None, "writer": None},
    )

    assert column.timezone.zone == "Europe/Berlin"


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
