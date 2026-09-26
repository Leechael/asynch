"""Regression tests for connections reused across event loops.

A connection binds its TCP reader/writer to the event loop where connect()
ran. Connection pools (SQLAlchemy's AsyncAdaptedQueuePool) and task runners
(Celery async_to_sync, one fresh loop per task) can hand the same Connection
object to a *new* loop after the old one closed. Before the fix, force_connect
trusted ``connected=True``, ping() did I/O on the dead loop, and the recovery
disconnect() raised ``RuntimeError: Event loop is closed`` from
``writer.close()``. Now force_connect detects the foreign/closed loop and
reconnects transparently, and disconnect() tolerates the dead loop.
"""

import asyncio

from asynch.connection import Connection


def _connect_and_query(conn: Connection) -> None:
    async def _run() -> None:
        await conn.connect()
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1")
            row = await cur.fetchone()
        assert row == (1,)

    asyncio.run(_run())


def test_connection_reused_after_creating_loop_is_closed(config):
    """A pooled connection handed to a new loop must transparently reconnect."""
    conn = Connection(dsn=config.dsn)

    _connect_and_query(conn)  # loop A runs and closes; conn stays "connected"

    # Loop B: before the fix this raised RuntimeError: Event loop is closed.
    _connect_and_query(conn)
    assert conn.connected

    asyncio.run(conn.close())


def test_disconnect_tolerates_a_closed_event_loop(config):
    """disconnect() must reset the object even when the creating loop is gone."""
    conn = Connection(dsn=config.dsn)
    _connect_and_query(conn)

    # Before the fix this raised RuntimeError: Event loop is closed.
    asyncio.run(conn.close())

    assert not conn.connected
    assert conn._connection.writer is None
