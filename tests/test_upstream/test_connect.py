import pytest

from asynch.connection import Connection
from asynch.errors import PartiallyConsumedQueryError, ServerException
from asynch.proto import constants
from asynch.proto.protocol import ClientPacket, ServerPacket

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


async def _execute(conn, query, args=None):
    return await conn._connection.execute(query, args=args)


async def test_packets_to_str():
    assert ClientPacket.to_str(2) == "Data"
    assert ClientPacket.to_str(6) == "KeepAlive"
    assert ClientPacket.to_str(42) == "Unknown packet"

    assert ServerPacket.to_str(4) == "Pong"
    assert ServerPacket.to_str(18) == "SSHChallenge"
    assert ServerPacket.to_str(42) == "Unknown packet"


async def test_exception_on_hello_packet(config):
    with pytest.raises(ServerException) as exc:
        async with Connection(**_connection_kwargs(config, user="wrong_user", stack_track=True)):
            pass

    assert "Code:" in str(exc.value)
    assert "Stack trace:" in str(exc.value)


async def test_remember_current_database(config):
    async with Connection(**_connection_kwargs(config)) as conn:
        await _execute(conn, "CREATE DATABASE IF NOT EXISTS system")
        await _execute(conn, "   USE     system   ; ")
        await conn._connection.disconnect()

        rv = await _execute(conn, "SELECT currentDatabase()")

    assert rv == [("system",)]


async def test_context_manager(config):
    async with Connection(**_connection_kwargs(config)) as conn:
        await _execute(conn, "SELECT 1")
        assert conn._connection.connected

    assert not conn._connection.connected


async def test_partially_consumed_query(config):
    async with Connection(**_connection_kwargs(config)) as conn:
        result = await conn._connection.execute_iter("SELECT 1")

        with pytest.raises(PartiallyConsumedQueryError) as exc:
            await conn._connection.execute_iter("SELECT 1")

        assert str(exc.value) == "Simultaneous queries on single connection detected"
        assert [row async for row in result] == [(1,)]

        rv = await _execute(conn, "SELECT 1")
        assert rv == [(1,)]


async def test_read_all_packets_on_execute_iter(config):
    async with Connection(**_connection_kwargs(config)) as conn:
        result = await conn._connection.execute_iter("SELECT 1")
        assert [row async for row in result] == [(1,)]

        result = await conn._connection.execute_iter("SELECT 1")
        assert [row async for row in result] == [(1,)]


async def test_client_revision(config):
    async with Connection(
        **_connection_kwargs(
            config, client_revision=constants.DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE
        )
    ) as conn:
        rv = await _execute(conn, "SELECT 1")

    assert rv == [(1,)]


async def test_default_client_revision_negotiates_server_cap(config):
    async with Connection(**_connection_kwargs(config)) as conn:
        rv = await _execute(conn, "SELECT 1")

        assert conn._connection.client_revision == constants.CLIENT_REVISION
        assert conn._connection.server_info.used_revision == min(
            conn._connection.server_info.revision,
            constants.CLIENT_REVISION,
        )

    assert rv == [(1,)]
