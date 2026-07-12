import asyncio
import os

import pytest

from asynch.connection import Connection
from asynch.errors import SocketTimeoutError
from tests.test_upstream.columns._helpers import execute

pytestmark = pytest.mark.asyncio


def _connection_kwargs(config, **kwargs):
    return {
        "host": config.host,
        "port": config.port,
        "database": config.database,
        "user": config.user,
        "password": config.password,
        "settings": config.settings.copy(),
        **kwargs,
    }


async def _type_families(conn):
    rows = await execute(conn, "SELECT name FROM system.data_type_families")
    return {row[0] for row in rows}


async def test_matrix_negotiates_expected_protocol_revision(config):
    expected = os.environ.get("ASYNCH_EXPECT_SERVER_REVISION")
    minimum = os.environ.get("ASYNCH_MIN_SERVER_REVISION")
    client_revision = os.environ.get("ASYNCH_CLIENT_REVISION")
    if not any((expected, minimum, client_revision)):
        pytest.skip("compatibility matrix contract is only enabled by CI")

    kwargs = _connection_kwargs(config)
    if client_revision:
        kwargs["client_revision"] = int(client_revision)
    async with Connection(**kwargs) as conn:
        await execute(conn, "SELECT 1")
        server_revision = conn._connection.server_info.revision
        used_revision = conn._connection.server_info.used_revision

    if expected:
        assert server_revision == int(expected)
    if minimum:
        assert server_revision >= int(minimum)
    if client_revision:
        assert used_revision == min(server_revision, int(client_revision))


@pytest.mark.latest_clickhouse
async def test_latest_server_exposes_revision_bound_type_families(conn):
    required = os.environ.get("ASYNCH_REQUIRED_TYPE_FAMILIES")
    if not required:
        pytest.skip("latest-server type contract is only enabled by CI")

    available = await _type_families(conn)
    required_families = set(required.split(","))

    assert required_families <= available


async def test_cancelled_query_disconnects_and_reconnects(config):
    async with Connection(**_connection_kwargs(config)) as conn:
        task = asyncio.create_task(execute(conn, "SELECT sleepEachRow(0.05) FROM numbers(20)"))
        await asyncio.sleep(0.1)
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        assert not conn._connection.connected
        assert await execute(conn, "SELECT 1") == [(1,)]


async def test_receive_timeout_disconnects_and_reconnects(config):
    async with Connection(**_connection_kwargs(config, send_receive_timeout=0.05)) as conn:
        with pytest.raises(SocketTimeoutError) as exc_info:
            await execute(conn, "SELECT sleep(1)")

        assert isinstance(exc_info.value.__cause__, asyncio.TimeoutError)
        assert not conn._connection.connected
        assert await execute(conn, "SELECT 1") == [(1,)]


async def test_alt_hosts_fails_over_to_live_server(config):
    async with Connection(
        **_connection_kwargs(
            config,
            port=1,
            alt_hosts=f"{config.host}:{config.port}",
            connect_timeout=0.2,
        )
    ) as conn:
        assert await execute(conn, "SELECT 1") == [(1,)]
        assert conn._connection.port == config.port


@pytest.mark.no_clickhouse
async def test_real_tls_handshake():
    dsn = os.environ.get("CLICKHOUSE_TLS_DSN")
    if not dsn:
        pytest.skip("TLS smoke is only enabled by the dedicated CI job")

    async with Connection(dsn=dsn, compression="lz4") as conn:
        async with conn.cursor() as cursor:
            assert await cursor.execute("SELECT 1") == 1
