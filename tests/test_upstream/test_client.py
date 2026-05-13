import ssl

import pytest

from asynch.connection import Connection
from asynch.proto.compression.lz4 import Compressor as LZ4Compressor
from asynch.proto.compression.lz4hc import Compressor as LZ4HCCompressor
from asynch.proto.compression.zstd import Compressor as ZSTDCompressor
from asynch.proto.protocol import Compression
from asynch.proto.utils.dsn import DSNError, parse_dsn


@pytest.fixture(autouse=True)
async def initialize_tests():
    yield


@pytest.fixture(autouse=True)
async def truncate_table():
    yield


def assert_hosts_equal(conn, expected):
    assert list(conn._connection.hosts) == expected


def test_simple():
    config = parse_dsn("clickhouse://host")
    assert config["host"] == "host"
    assert "database" not in config

    config = parse_dsn("clickhouse://host/db")
    assert config["host"] == "host"
    assert config["database"] == "db"


def test_credentials():
    c = Connection(dsn="clickhouse://host/db")
    assert c._connection.user == "default"
    assert c._connection.password == ""

    c = Connection(dsn="clickhouse://admin:secure@host/db")
    assert c._connection.user == "admin"
    assert c._connection.password == "secure"

    c = Connection(dsn="clickhouse://user:@host/db")
    assert c._connection.user == "user"
    assert c._connection.password == ""


def test_credentials_unquoting():
    c = Connection(dsn="clickhouse://ad%3Amin:se%2Fcure@host/db")

    assert c._connection.user == "ad:min"
    assert c._connection.password == "se/cure"


def test_schema():
    c = Connection(dsn="clickhouse://host")
    assert not c._connection.secure_socket

    c = Connection(dsn="clickhouses://host")
    assert c._connection.secure_socket

    with pytest.raises(DSNError):
        parse_dsn("test://host")


def test_port():
    c = Connection(dsn="clickhouse://host")
    assert_hosts_equal(c, [("host", 9000)])

    c = Connection(dsn="clickhouses://host")
    assert_hosts_equal(c, [("host", 9440)])

    c = Connection(dsn="clickhouses://host:1234")
    assert_hosts_equal(c, [("host", 1234)])


def test_secure():
    c = Connection(dsn="clickhouse://host?secure=n")
    assert_hosts_equal(c, [("host", 9000)])
    assert not c._connection.secure_socket

    c = Connection(dsn="clickhouse://host?secure=y")
    assert_hosts_equal(c, [("host", 9440)])
    assert c._connection.secure_socket

    c = Connection(dsn="clickhouse://host:1234?secure=y")
    assert_hosts_equal(c, [("host", 1234)])
    assert c._connection.secure_socket

    with pytest.raises(ValueError):
        parse_dsn("clickhouse://host:1234?secure=nonono")


def test_compression():
    c = Connection(dsn="clickhouse://host?compression=n")
    assert c._connection.compression == Compression.DISABLED
    assert c._connection.compressor_cls is None

    c = Connection(dsn="clickhouse://host?compression=y")
    assert c._connection.compression == Compression.ENABLED
    assert c._connection.compressor_cls is LZ4Compressor

    c = Connection(dsn="clickhouse://host?compression=lz4")
    assert c._connection.compression == Compression.ENABLED
    assert c._connection.compressor_cls is LZ4Compressor

    c = Connection(dsn="clickhouse://host?compression=lz4hc")
    assert c._connection.compression == Compression.ENABLED
    assert c._connection.compressor_cls is LZ4HCCompressor

    c = Connection(dsn="clickhouse://host?compression=zstd")
    assert c._connection.compression == Compression.ENABLED
    assert c._connection.compressor_cls is ZSTDCompressor

    with pytest.raises(ValueError):
        parse_dsn("clickhouse://host:1234?compression=custom")


def test_client_name():
    c = Connection(dsn="clickhouse://host?client_name=native")
    assert c._connection.client_name == "ClickHouse native"


def test_timeouts():
    with pytest.raises(ValueError):
        parse_dsn("clickhouse://host?connect_timeout=test")

    c = Connection(dsn="clickhouse://host?connect_timeout=1.2")
    assert c._connection.connect_timeout == 1.2

    c = Connection(dsn="clickhouse://host?send_receive_timeout=1.2")
    assert c._connection.send_receive_timeout == 1.2

    c = Connection(dsn="clickhouse://host?sync_request_timeout=1.2")
    assert c._connection.sync_request_timeout == 1.2


def test_compress_block_size():
    with pytest.raises(ValueError):
        parse_dsn("clickhouse://host?compress_block_size=test")

    c = Connection(dsn="clickhouse://host?compress_block_size=100500")
    assert c._connection.compress_block_size is None

    c = Connection(dsn="clickhouse://host?compress_block_size=100500&compression=1")
    assert c._connection.compress_block_size == 100500


def test_settings():
    c = Connection(dsn="clickhouse://host?send_logs_level=trace&max_block_size=123")
    assert c._connection.settings == {
        "send_logs_level": "trace",
        "max_block_size": "123",
    }


def test_ssl():
    c = Connection(
        dsn="clickhouses://host?"
        "verify=false&"
        "ssl_version=PROTOCOL_SSLv23&"
        "ca_certs=/tmp/certs&"
        "ciphers=HIGH:-aNULL:-eNULL:-PSK:RC4-SHA:RC4-MD5"
    )
    assert c._connection.verify is False
    assert c._connection.ssl_options == {
        "ssl_version": ssl.PROTOCOL_SSLv23,
        "ca_certs": "/tmp/certs",
        "ciphers": "HIGH:-aNULL:-eNULL:-PSK:RC4-SHA:RC4-MD5",
    }


def test_alt_hosts():
    c = Connection(dsn="clickhouse://host?alt_hosts=host2:1234")
    assert_hosts_equal(c, [("host", 9000), ("host2", 1234)])

    c = Connection(dsn="clickhouse://host?alt_hosts=host2")
    assert_hosts_equal(c, [("host", 9000), ("host2", 9000)])


def test_parameters_cast():
    c = Connection(dsn="clickhouse://host?insert_block_size=123")
    assert c._connection.context.client_settings["insert_block_size"] == 123


def test_settings_is_important():
    c = Connection(dsn="clickhouse://host?settings_is_important=1")
    assert c._connection.settings_is_important is True

    with pytest.raises(ValueError):
        parse_dsn("clickhouse://host?settings_is_important=2")

    c = Connection(dsn="clickhouse://host?settings_is_important=0")
    assert c._connection.settings_is_important is False


def test_use_numpy():
    c = Connection(dsn="clickhouse://host?use_numpy=true")
    assert c._connection.context.client_settings["use_numpy"]


def test_opentelemetry():
    c = Connection(
        dsn="clickhouse://host?opentelemetry_traceparent="
        "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-00"
    )
    assert (
        c._connection.context.client_settings["opentelemetry_traceparent"]
        == "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-00"
    )
    assert c._connection.context.client_settings["opentelemetry_tracestate"] == ""

    c = Connection(
        dsn="clickhouse://host?opentelemetry_traceparent="
        "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-00&"
        "opentelemetry_tracestate=state"
    )
    assert (
        c._connection.context.client_settings["opentelemetry_traceparent"]
        == "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-00"
    )
    assert c._connection.context.client_settings["opentelemetry_tracestate"] == "state"


def test_quota_key():
    c = Connection(dsn="clickhouse://host?quota_key=myquota")
    assert c._connection.context.client_settings["quota_key"] == "myquota"

    c = Connection(dsn="clickhouse://host")
    assert c._connection.context.client_settings["quota_key"] == ""


def test_client_revision():
    c = Connection(dsn="clickhouse://host?client_revision=54032")
    assert c._connection.client_revision == 54032
