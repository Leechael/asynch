import asyncio
import ssl
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from asynch.connection import Connection
from asynch.errors import NetworkError, PartiallyConsumedQueryError, ServerException
from asynch.proto import constants
from asynch.proto.models.enums import ConnectionStatus
from benchmark import chaos_memory_watch, memory_watch

HOST = "192.168.15.103"
PORT = 10000
USER = "ch_user"
PASSWORD = "So~ePa55w0rd"
DATABASE = "db"


def _test_connection_credentials(
    conn: Connection,
    *,
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
) -> None:
    __tracebackhide__ = True

    assert conn.host == host
    assert conn.port == port
    assert conn.user == user
    assert conn.password == password
    assert conn.database == database


def _test_connectivity_invariant(
    conn: Connection,
    *,
    is_connected: bool = False,
    is_closed: bool = False,
) -> None:
    __tracebackhide__ = True

    assert conn.opened is is_connected
    assert conn.closed is is_closed


def test_dsn():
    dsn = f"clickhouse://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
    conn = Connection(dsn=dsn)

    _test_connection_credentials(
        conn, host=HOST, port=PORT, user=USER, password=PASSWORD, database=DATABASE
    )
    _test_connectivity_invariant(conn=conn)


def test_secure_dsn():
    dsn = (
        f"clickhouses://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
        "?verify=true"
        "&ssl_version=PROTOCOL_TLSv1"
        "&ca_certs=path/to/CA.crt"
        "&ciphers=AES"
    )
    conn = Connection(dsn=dsn)

    _test_connection_credentials(
        conn, host=HOST, port=PORT, user=USER, password=PASSWORD, database=DATABASE
    )
    _test_connectivity_invariant(conn=conn)
    assert conn._connection.secure_socket
    assert conn._connection.verify
    assert conn._connection.ssl_options.get("ssl_version") is ssl.PROTOCOL_TLSv1
    assert conn._connection.ssl_options.get("ca_certs") == "path/to/CA.crt"
    assert conn._connection.ssl_options.get("ciphers") == "AES"


def test_secure_connection():
    conn = Connection(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
        secure=True,
        verify=True,
        ssl_version=ssl.PROTOCOL_TLSv1,
        ca_certs="path/to/CA.crt",
        ciphers="AES",
    )

    _test_connection_credentials(
        conn, host=HOST, port=PORT, user=USER, password=PASSWORD, database=DATABASE
    )
    _test_connectivity_invariant(conn=conn)
    assert conn._connection.secure_socket
    assert conn._connection.verify
    assert conn._connection.ssl_options.get("ssl_version") is ssl.PROTOCOL_TLSv1
    assert conn._connection.ssl_options.get("ca_certs") == "path/to/CA.crt"
    assert conn._connection.ssl_options.get("ciphers") == "AES"


def test_secure_connection_check_ssl_context():
    conn = Connection(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
        secure=True,
        ciphers="AES",
        ssl_version=ssl.OP_NO_TLSv1,
    )

    _test_connection_credentials(
        conn, host=HOST, port=PORT, user=USER, password=PASSWORD, database=DATABASE
    )
    _test_connectivity_invariant(conn=conn)
    assert conn._connection.secure_socket
    assert conn._connection.verify
    assert conn._connection.ssl_options.get("ssl_version") is ssl.OP_NO_TLSv1
    assert conn._connection.ssl_options.get("ca_certs") is None
    assert conn._connection.ssl_options.get("ciphers") == "AES"
    ssl_ctx = conn._connection._get_ssl_context()
    assert ssl_ctx
    assert ssl.OP_NO_TLSv1 in ssl_ctx.options


def test_connection_status_offline():
    conn = Connection()
    repstr = f"<Connection object at 0x{id(conn):x}; status: created>"

    assert repr(conn) == repstr
    assert not conn.opened
    assert not conn.closed


@pytest.mark.no_clickhouse
def test_connection_passes_buffer_size_to_protocol_connection():
    assert Connection(buffer_size=512)._connection.buffer_size == 512


@pytest.mark.no_clickhouse
def test_connection_exposes_last_query():
    conn = Connection()
    last_query = object()
    conn._connection.last_query = last_query

    assert conn.last_query is last_query


@pytest.mark.no_clickhouse
def test_connection_exposes_read_only_settings():
    conn = Connection(settings={"max_threads": 2})

    assert dict(conn.settings) == {"max_threads": 2}
    with pytest.raises(TypeError):
        conn.settings["max_threads"] = 4


@pytest.mark.no_clickhouse
def test_connection_wire_liveness_contract_inv_s1_s2_s5():
    conn = Connection()
    conn._opened = True
    conn._connection.connected = False
    conn._connection.is_query_executing = True

    assert conn.connected is False
    assert conn.is_query_executing is True
    assert conn.status == ConnectionStatus.closed


@pytest.mark.no_clickhouse
@pytest.mark.asyncio
async def test_connect_rebuilds_wire_dead_connection_inv_s4():
    conn = Connection()
    conn._opened = True
    conn._connection.connected = False
    conn._connection.connect = AsyncMock()

    await conn.connect()

    conn._connection.connect.assert_awaited_once()


@pytest.mark.no_clickhouse
@pytest.mark.asyncio
async def test_close_disconnects_lazily_connected_connection_inv_s3():
    conn = Connection()
    conn._connection.connected = True
    conn._connection.disconnect = AsyncMock()

    await conn.close()

    conn._connection.disconnect.assert_awaited_once()
    assert conn.closed is True


@pytest.mark.no_clickhouse
@pytest.mark.asyncio
async def test_refresh_reconnects_wire_dead_connection_inv_s7():
    conn = Connection()
    conn._opened = True
    conn._connection.connected = False
    conn._connection.disconnect = AsyncMock()
    conn._connection.connect = AsyncMock()

    await conn._refresh()

    conn._connection.disconnect.assert_awaited_once()
    conn._connection.connect.assert_awaited_once()


@pytest.mark.no_clickhouse
def test_terminate_aborts_connected_transport_inv_s8():
    conn = Connection()
    transport = Mock()
    conn._connection.connected = True
    conn._connection.writer = Mock(writer=Mock(transport=transport))

    conn.terminate()
    conn.terminate()

    transport.abort.assert_called_once()
    assert conn.connected is False
    assert conn.closed is True


@pytest.mark.no_clickhouse
@pytest.mark.asyncio
async def test_disconnect_swallows_broken_pipe_and_resets_wire_state():
    conn = Connection()
    proto = conn._connection
    proto.connected = True
    proto.is_query_executing = True
    proto.writer = Mock(close=AsyncMock(side_effect=BrokenPipeError("peer closed")))

    await proto.disconnect()

    assert proto.connected is False
    assert proto.is_query_executing is False
    assert proto.writer is None


@pytest.mark.no_clickhouse
@pytest.mark.asyncio
async def test_disconnect_resets_wire_state_when_writer_close_fails():
    conn = Connection()
    proto = conn._connection
    proto.connected = True
    proto.is_query_executing = True
    proto.writer = Mock(close=AsyncMock(side_effect=OSError("close failed")))
    proto.reader = Mock()
    proto.block_reader = Mock()
    proto.block_reader_raw = Mock()
    proto.block_writer = Mock()
    proto.server_info = Mock()

    with pytest.raises(OSError, match="close failed"):
        await proto.disconnect()

    assert proto.connected is False
    assert proto.is_query_executing is False
    assert proto.reader is None
    assert proto.writer is None
    assert proto.block_reader is None
    assert proto.block_reader_raw is None
    assert proto.block_writer is None
    assert proto.server_info is None


@pytest.mark.no_clickhouse
@pytest.mark.asyncio
async def test_close_marks_connection_closed_when_wire_cleanup_fails():
    conn = Connection()
    conn._opened = True
    conn._connection.connected = True
    conn._connection.disconnect = AsyncMock(side_effect=OSError("close failed"))

    with pytest.raises(OSError, match="close failed"):
        await conn.close()

    assert conn.opened is False
    assert conn.closed is True


@pytest.mark.no_clickhouse
@pytest.mark.asyncio
@pytest.mark.parametrize("restart_failure", [False, True])
async def test_server_kill_watch_cleans_and_reconnects_without_a_process_restart(
    monkeypatch,
    restart_failure,
):
    kill_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    class Resource:
        pass

    class ReaderResource(Resource):
        def __init__(self):
            self.reader = Resource()

    class WriterResource(Resource):
        def __init__(self):
            self.writer = Resource()

    class FakeProto:
        def __init__(self):
            self.connect_count = 0
            self.connected = False
            self.is_query_executing = False
            self.last_query = None
            self._open_wire()

        def _open_wire(self):
            self.reader = ReaderResource()
            self.writer = WriterResource()
            self.block_reader = Resource()
            self.block_reader_raw = Resource()
            self.block_writer = Resource()
            self.server_info = Resource()

        async def execute(self, query, args=None):
            if query == "SELECT 1":
                return [(1,)]
            self.is_query_executing = True
            await kill_event.wait()
            self.connected = False
            self.is_query_executing = False
            self.reader = None
            self.writer = None
            self.block_reader = None
            self.block_reader_raw = None
            self.block_writer = None
            self.server_info = None
            self.last_query = None
            raise ConnectionResetError("server exited")

    class FakeConnection:
        def __init__(self, dsn):
            self.dsn = dsn
            self._connection = FakeProto()
            self.close_count = 0

        @property
        def is_query_executing(self):
            return self._connection.is_query_executing

        async def connect(self):
            self._connection.connect_count += 1
            if not self._connection.connected:
                self._connection._open_wire()
                self._connection.connected = True

        async def close(self):
            self.close_count += 1
            self._connection.connected = False

    connection = FakeConnection("clickhouse://test")

    def fake_docker(*args, check=True):
        if args[0] == "kill":
            loop.call_soon_threadsafe(kill_event.set)
        elif args[0] == "start" and restart_failure:
            raise RuntimeError("restart failed")
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(chaos_memory_watch, "Connection", lambda dsn: connection)
    monkeypatch.setattr(chaos_memory_watch, "run_docker", fake_docker)
    monkeypatch.setattr(chaos_memory_watch, "wait_for_clickhouse", AsyncMock())
    args = SimpleNamespace(
        clickhouse_container_id="container-id",
        query_start_timeout=1.0,
        query_failure_timeout=1.0,
        server_restart_timeout=1.0,
    )

    if restart_failure:
        with pytest.raises(RuntimeError, match="restart failed"):
            await chaos_memory_watch.server_kill_during_query(
                "clickhouse://test",
                args,
            )
        assert connection._connection.connect_count == 1
        assert connection.close_count == 1
    else:
        outcome = await chaos_memory_watch.server_kill_during_query(
            "clickhouse://test",
            args,
        )
        assert outcome == "server_kill_recovered"
        assert connection._connection.connect_count == 2
        assert connection.close_count == 2


@pytest.mark.no_clickhouse
def test_memory_watch_markdown_reports_include_release_metrics(tmp_path):
    baseline = {
        "rss_mib": 100.0,
        "py_current_mib": 10.0,
        "fd_count": 5,
        "pending_tasks": 0,
    }
    final = {
        "rss_mib": 101.0,
        "py_current_mib": 10.5,
        "fd_count": 5,
        "pending_tasks": 0,
    }
    common = {
        "rss_growth_mib": 1.0,
        "python_heap_growth_mib": 0.5,
        "fd_growth": 0,
        "pending_task_growth": 0,
        "baseline": baseline,
        "final": final,
    }
    normal_path = tmp_path / "normal.md"
    chaos_path = tmp_path / "chaos.md"

    memory_watch.write_markdown_report(
        str(normal_path),
        {**common, "baseline_cycle": 5, "final_cycle": 50},
    )
    chaos_memory_watch.write_markdown_report(
        str(chaos_path),
        {
            **common,
            "baseline_operation": 30,
            "final_operation": 310,
            "outcomes": {"server_kill_recovered": 10},
        },
    )

    assert "| RSS | 100.00 MiB | 101.00 MiB | +1.00 MiB |" in normal_path.read_text()
    assert "| `server_kill_recovered` | 10 |" in chaos_path.read_text()


@pytest.mark.asyncio
async def test_connection_status_online(config):
    conn = Connection(dsn=config.dsn)
    conn_id = id(conn)

    repstr = f"<{conn.__class__.__name__} object at 0x{conn_id:x}"

    try:
        await conn.connect()
        assert repr(conn) == f"{repstr}; status: opened>"
        assert conn.opened
        assert conn.closed is False

        await conn.close()
        assert repr(conn) == f"{repstr}; status: closed>"
        assert conn.opened is False
        assert conn.closed
    finally:
        await conn.close()
        assert repr(conn) == f"{repstr}; status: closed>"
        assert conn.opened is False
        assert conn.closed


@pytest.mark.asyncio
async def test_async_context_manager_interface(config):
    conn = Connection(dsn=config.dsn)
    _test_connectivity_invariant(conn=conn)

    async with conn:
        _test_connectivity_invariant(conn=conn, is_connected=True, is_closed=False)
        await conn.ping()

    _test_connectivity_invariant(conn=conn, is_connected=False, is_closed=True)
    try:
        await conn.ping()
    except NetworkError:
        pass

    async with conn:
        _test_connectivity_invariant(conn=conn, is_connected=True, is_closed=False)
        await conn.ping()


@pytest.mark.no_clickhouse
@pytest.mark.asyncio
async def test_connection_ping(config):
    conn = Connection(dsn=config.dsn)

    with pytest.raises(NetworkError):
        await conn.ping()


@pytest.mark.asyncio
async def test_single_connection_rejects_simultaneous_execute(config):
    """A connection is intentionally single-in-flight; pools provide concurrency."""

    async with Connection(dsn=config.dsn) as conn:
        loop = asyncio.get_running_loop()
        second_failure_delay = None

        async def first_execute():
            return await conn._connection.execute("SELECT sleep(0.2)")

        async def second_execute():
            nonlocal second_failure_delay
            for _ in range(100):
                if conn._connection.is_query_executing:
                    break
                await asyncio.sleep(0)
            else:
                raise AssertionError("first execute did not become in-flight")
            started = loop.time()
            with pytest.raises(PartiallyConsumedQueryError):
                await conn._connection.execute("SELECT 1")
            second_failure_delay = loop.time() - started

        first_result, second_result = await asyncio.gather(
            first_execute(), second_execute(), return_exceptions=True
        )

    assert first_result == [(0,)]
    assert second_result is None
    assert second_failure_delay is not None
    assert second_failure_delay < 0.1

    async with conn:
        await conn.ping()

    with pytest.raises(NetworkError):
        await conn.ping()

    conn = Connection(dsn="clickhouse://inval:9000/non-existent")
    with pytest.raises(NetworkError):
        await conn.ping()


@pytest.mark.asyncio
async def test_connection_cleanup(config, get_tcp_connections):
    """Test a connection to be properly closed.

    A connection is properly closed if it releases resources,
    especially breaking the TCP channel, leaving no dangling
    connections on a ClickHouse server.

    Plan:
    1. get the number of TCP connections before the test
    2. open N connections, each should execute a query, then closing
    3. assert that the number of TCP connections equals to the initial value
    """

    # get the number of total TCP connections to the ClickHouse
    init_tcps = 0
    conn = Connection(dsn=config.dsn)
    async with conn as cn:
        init_tcps = await get_tcp_connections(cn)

    # open-execute-close connections
    for _ in range(100):
        async with Connection(dsn=config.dsn) as cn:
            async with cn.cursor() as cur:
                await cur.execute("SELECT 1")
                ret = await cur.fetchone()
                assert ret == (1,)

    final_tcps = 0
    async with conn as cn:
        final_tcps = await get_tcp_connections(cn)

    assert final_tcps <= init_tcps


@pytest.mark.asyncio
async def test_connection_close(config):
    conn = Connection()

    # it does not break
    await conn.close()

    assert not conn.opened
    assert conn.closed

    async with Connection(dsn=config.dsn) as conn:
        assert conn.opened

        await conn.close()

        assert not conn.opened
        assert conn.closed


@pytest.mark.asyncio
async def test_session_timezone_does_not_outlive_the_query_that_carried_it(conn: Connection):
    """A datetime keeps meaning what the server says it means.

    ClickHouse announces the ``session_timezone`` of a statement that feeds its
    rows through ``input()``, and announces nothing on any other statement. A
    connection that holds on to the announcement therefore reads every later
    result in a timezone the server is no longer using, and a pooled connection
    hands that on to whoever borrows it next.
    """

    proto = conn._connection
    if proto.server_info.used_revision < constants.DBMS_MIN_PROTOCOL_VERSION_WITH_TIMEZONE_UPDATES:
        pytest.skip("ClickHouse server does not announce a session timezone")

    table = f"test.session_tz_{uuid4().hex[:8]}"
    async with conn.cursor() as cursor:
        await cursor.execute(f"CREATE TABLE {table} (a Int8) ENGINE = Memory")

    try:
        before = await proto.execute("SELECT toDateTime(0)")

        try:
            await proto.execute(
                f"INSERT INTO {table} (a) SELECT a FROM input('a Int8') FORMAT Native",
                args=[{"a": 1}],
                settings={"session_timezone": "Asia/Kolkata"},
            )
        except ServerException:
            pytest.skip("ClickHouse server does not support session_timezone")

        assert await proto.execute("SELECT toDateTime(0)") == before
        assert proto.server_info.session_timezone is None
    finally:
        async with conn.cursor() as cursor:
            await cursor.execute(f"DROP TABLE IF EXISTS {table}")
