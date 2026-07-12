import asyncio
from typing import Any
from unittest.mock import AsyncMock

import pytest

from asynch.connection import Connection
from asynch.errors import AsynchPoolError
from asynch.pool import Pool
from asynch.proto import constants
from asynch.proto.connection import METRICS_ENV


def _get_pool_size(pool: Pool) -> int:
    return pool.acquired_connections + pool.free_connections


@pytest.mark.no_clickhouse
@pytest.mark.asyncio
async def test_release_discards_poisoned_connection_without_raising_inv_s7():
    pool = Pool(minsize=0, maxsize=1)
    conn = Connection()
    conn._connection.connected = True
    conn._connection.is_query_executing = True

    conn.close = AsyncMock()
    pool._acquired_connections.add(conn)

    await pool._release_connection(conn)

    assert pool.acquired_connections == 0
    assert pool.free_connections == 0
    conn.close.assert_awaited_once()


@pytest.mark.no_clickhouse
@pytest.mark.parametrize(
    ("environment", "expected"),
    [("1", True), ("true", True), ("ON", True), ("0", False)],
)
def test_pool_metrics_environment_enablement(monkeypatch, environment, expected):
    monkeypatch.setenv(METRICS_ENV, environment)

    assert (Pool().metrics is not None) is expected
    assert (Pool(metrics=not expected).metrics is not None) is not expected


@pytest.mark.no_clickhouse
async def test_pool_metrics_record_acquisition_wait():
    pool = Pool(minsize=0, maxsize=1, metrics=True)
    conn = Connection()
    pool._acquire_connection = AsyncMock(return_value=conn)
    pool._release_connection = AsyncMock()

    async with pool.connection() as acquired:
        assert acquired is conn

    assert pool.metrics is not None
    assert pool.metrics.acquisitions == 1
    assert pool.metrics.acquire_wait_total >= 0
    assert pool.metrics.acquire_wait_max == pool.metrics.acquire_wait_total


@pytest.mark.no_clickhouse
@pytest.mark.asyncio
async def test_acquire_replaces_synchronously_dead_connection():
    pool = Pool(minsize=0, maxsize=1)
    dead = Connection()
    dead._connection.connected = False
    dead.close = AsyncMock()
    replacement = Connection()
    replacement._connection.connected = True
    pool._free_connections.append(dead)
    pool._idle_since[dead] = 0.0
    pool._new_connection = AsyncMock(return_value=replacement)

    acquired = await pool._acquire_connection()

    assert acquired is replacement
    assert pool.acquired_connections == 1
    assert pool.free_connections == 0
    dead.close.assert_awaited_once()


@pytest.mark.no_clickhouse
@pytest.mark.asyncio
async def test_idle_ping_only_runs_after_pool_recycle_threshold():
    pool = Pool(minsize=0, maxsize=2, pool_recycle=10)
    pool._clock = lambda: 15.0
    fresh = Connection()
    fresh._connection.connected = True
    fresh.ping = AsyncMock()
    stale = Connection()
    stale._connection.connected = True
    stale.ping = AsyncMock()
    pool._free_connections.extend((fresh, stale))
    pool._idle_since[fresh] = 10.0
    pool._idle_since[stale] = 5.0

    assert await pool._acquire_connection() is fresh
    assert await pool._acquire_connection() is stale

    fresh.ping.assert_not_awaited()
    stale.ping.assert_awaited_once()


@pytest.mark.no_clickhouse
@pytest.mark.asyncio
async def test_failed_idle_ping_discards_connection_and_tries_the_next_one():
    pool = Pool(minsize=0, maxsize=1, pool_recycle=10)
    pool._clock = lambda: 10.0
    stale = Connection()
    stale._connection.connected = True
    stale.ping = AsyncMock(side_effect=ConnectionError("lost peer"))
    stale.close = AsyncMock()
    replacement = Connection()
    replacement._connection.connected = True
    pool._free_connections.append(stale)
    pool._idle_since[stale] = 0.0
    pool._new_connection = AsyncMock(return_value=replacement)

    assert await pool._acquire_connection() is replacement

    stale.ping.assert_awaited_once()
    stale.close.assert_awaited_once()


@pytest.mark.no_clickhouse
@pytest.mark.asyncio
async def test_healthy_release_performs_no_network_maintenance():
    pool = Pool(minsize=0, maxsize=1)
    conn = Connection()
    conn._connection.connected = True
    conn.ping = AsyncMock()
    conn.close = AsyncMock()
    pool._acquired_connections.add(conn)

    await pool._release_connection(conn)

    assert pool.acquired_connections == 0
    assert pool.free_connections == 1
    conn.ping.assert_not_awaited()
    conn.close.assert_not_awaited()


@pytest.mark.no_clickhouse
@pytest.mark.asyncio
async def test_concurrent_borrowing_keeps_pool_size_bounded_without_leaks():
    maxsize = 4
    pool = Pool(minsize=0, maxsize=maxsize, pool_recycle=-1)
    connections = [Connection() for _ in range(maxsize)]
    for conn in connections:
        conn._connection.connected = True
        pool._free_connections.append(conn)
        pool._idle_since[conn] = 0.0

    seen = set()

    async def borrow() -> None:
        async with pool.connection() as conn:
            seen.add(conn)
            assert pool.acquired_connections <= maxsize
            assert pool.free_connections + pool.acquired_connections <= maxsize
            await asyncio.sleep(0)

    await asyncio.gather(*(borrow() for _ in range(40)))

    assert seen == set(connections)
    assert pool.acquired_connections == 0
    assert pool.free_connections == maxsize
    assert _get_pool_size(pool) == maxsize


@pytest.mark.asyncio
async def test_pool_size_boundary_values():
    """If not marked as asyncio, then `RuntimeError: no running event loop` occurs."""

    Pool(minsize=0)
    with pytest.raises(ValueError, match=r"minsize is expected to be greater or equal to zero"):
        Pool(minsize=-1)

    Pool(minsize=0, maxsize=1)
    with pytest.raises(ValueError, match=r"maxsize is expected to be greater than zero"):
        Pool(maxsize=0)

    Pool(minsize=1, maxsize=1)
    with pytest.raises(ValueError, match=r"minsize is greater than maxsize"):
        Pool(minsize=2, maxsize=1)

    with pytest.raises(ValueError, match=r"pool_recycle is expected to be greater or equal to -1"):
        Pool(pool_recycle=-2)


@pytest.mark.asyncio
async def test_pool_repr(config):
    pool = Pool()
    repstr = (
        f"<Pool(minsize={constants.POOL_MIN_SIZE}, maxsize={constants.POOL_MAX_SIZE})"
        f" object at 0x{id(pool):x}; status: created>"
    )
    assert repr(pool) == repstr

    min_size, max_size = 2, 3
    pool = Pool(minsize=min_size, maxsize=max_size, dsn=config.dsn)
    async with pool:
        repstr = (
            f"<Pool(minsize={min_size}, maxsize={max_size}) "
            f"object at 0x{id(pool):x}; status: opened>"
        )
        assert repr(pool) == repstr

    repstr = (
        f"<Pool(minsize={min_size}, maxsize={max_size}) object at 0x{id(pool):x}; status: closed>"
    )
    assert repr(pool) == repstr


@pytest.mark.asyncio
async def test_pool_connection_attributes(config):
    pool = Pool(dsn=config.dsn)
    assert pool.minsize == constants.POOL_MIN_SIZE
    assert pool.maxsize == constants.POOL_MAX_SIZE
    assert _get_pool_size(pool) == 0
    assert pool.free_connections == 0
    assert pool.acquired_connections == 0

    async with pool:
        assert _get_pool_size(pool) == constants.POOL_MIN_SIZE
        assert pool.free_connections == constants.POOL_MIN_SIZE
        assert pool.acquired_connections == 0

        async with pool.connection():
            assert _get_pool_size(pool) == constants.POOL_MIN_SIZE
            assert pool.free_connections == 0
            assert pool.acquired_connections == constants.POOL_MIN_SIZE

        assert _get_pool_size(pool) == constants.POOL_MIN_SIZE
        assert pool.free_connections == constants.POOL_MIN_SIZE
        assert pool.acquired_connections == 0

    assert _get_pool_size(pool) == 0
    assert pool.free_connections == 0
    assert pool.acquired_connections == 0


@pytest.mark.asyncio
async def test_pool_connection_management(config, get_tcp_connections):
    """Tests connection cleanup when leaving a pool context.

    No dangling/unclosed connections must leave behind.
    """

    async def _get_pool_connection(pool: Pool):
        async with pool.connection():
            pass

    async with Connection(dsn=config.dsn) as conn:
        init_tcps = await get_tcp_connections(conn)

    async with Pool(minsize=1, maxsize=2, dsn=config.dsn) as pool:
        async with pool.connection():
            assert pool.free_connections == 0
            assert pool.acquired_connections == 1
        assert pool.free_connections == 1
        assert pool.acquired_connections == 0

        async with pool.connection() as cn1:
            assert pool.free_connections == 0
            assert pool.acquired_connections == 1

            async with pool.connection() as cn2:
                assert pool.free_connections == 0
                assert pool.acquired_connections == 2

                # It is possible to acquire more than pool.maxsize property.
                # But the caller gets stuck while waiting for a free connection
                with pytest.raises(asyncio.TimeoutError):
                    await asyncio.wait_for(_get_pool_connection(pool), timeout=1.0)

                # the returned connections are functional
                async with cn1.cursor() as cur:
                    await cur.execute("SELECT 21")
                    ret = await cur.fetchone()
                    assert ret == (21,)
                async with cn2.cursor() as cur:
                    await cur.execute("SELECT 42")
                    ret = await cur.fetchone()
                    assert ret == (42,)

                # the status quo has remained
                assert pool.free_connections == 0
                assert pool.acquired_connections == 2

            assert pool.free_connections == 1
            assert pool.acquired_connections == 1

            async with pool.connection() as cn3:
                assert pool.free_connections == 0
                assert pool.acquired_connections == 2

                async with cn3.cursor() as cur:
                    await cur.execute("SELECT 84")
                    ret = await cur.fetchone()
                    assert ret == (84,)

            assert pool.free_connections == 1
            assert pool.acquired_connections == 1

        assert pool.free_connections == 2
        assert pool.acquired_connections == 0

    async with Connection(dsn=config.dsn) as conn:
        assert await get_tcp_connections(conn) <= init_tcps


@pytest.mark.asyncio
async def test_pool_concurrent_connection_management(config, get_tcp_connections):
    """Tests pool connection managements on concurrent connections.

    A pool must not be broken when connections are acquired from concurrent tasks.
    When leaving the pool, all acquired connections become invalidated.
    No dangling/unclosed connections must remain.
    """

    async def _test_pool_connection(pool: Pool, *, selectee: Any = 42):
        async with pool.connection() as conn_ctx:
            async with conn_ctx.cursor() as cur:
                await cur.execute(f"SELECT {selectee}")
                ret = await cur.fetchone()
                assert ret == (selectee,)
                return selectee

    async with Connection(dsn=config.dsn) as conn:
        init_tcps = await get_tcp_connections(conn)

    min_size, max_size = 10, 21
    selectees = list(range(min_size, max_size + 1))  # exceeding the maxsize
    answers = []
    async with Pool(minsize=min_size, maxsize=max_size, dsn=config.dsn) as pool:
        tasks = [
            asyncio.create_task(_test_pool_connection(pool=pool, selectee=selectee))
            for selectee in selectees
        ]
        answers = await asyncio.gather(*tasks)

    async with Connection(dsn=config.dsn) as conn:
        noc = await get_tcp_connections(conn)
        assert noc <= init_tcps

    assert selectees == answers


@pytest.mark.asyncio
async def test_pool_broken_connection_handling(config):
    async def _get_answer(pool: Pool, *, raise_exc: bool = True):
        async with pool.connection() as conn_ctx:
            assert pool.free_connections == 0
            assert pool.acquired_connections == 1

            async with conn_ctx.cursor() as cur:
                if raise_exc:
                    raise AsynchPoolError("good bye")
                await cur.execute("SELECT 21 + 21;")
                ret = await cur.fetchone()
                assert ret == 42
                return ret

    min_size, max_size = 1, 1
    pool = Pool(minsize=min_size, maxsize=max_size, dsn=config.dsn)
    async with pool:
        async with pool.connection() as conn:
            await conn.ping()

            # he connection is invalidated
            await conn.close()
            with pytest.raises(ConnectionError):
                await conn.ping()

            # but does not influence the pool state
            assert pool.free_connections == 0
            assert pool.acquired_connections == 1

        # Returning a dead connection drops it without a lock-held reconnect.
        # The next checkout replenishes the temporarily empty pool lazily.
        assert pool.free_connections == 0
        assert pool.acquired_connections == 0

        async with pool.connection() as conn:
            await conn.ping()
            assert pool.free_connections == 0
            assert pool.acquired_connections == 1

        seq = list(range(10))
        tasks = [asyncio.create_task(_get_answer(pool=pool, raise_exc=bool(i % 2))) for i in seq]
        # no blockade and no inconsistency
        await asyncio.gather(*tasks, return_exceptions=True)

        assert pool.free_connections == 1
        assert pool.acquired_connections == 0
