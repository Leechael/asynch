import asyncio
import logging
from collections import deque
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from time import perf_counter
from typing import Optional

from asynch.connection import Connection
from asynch.errors import AsynchPoolError
from asynch.proto import constants
from asynch.proto.connection import resolve_metrics_enabled
from asynch.proto.models.enums import PoolStatus

logger = logging.getLogger(__name__)


class PoolMetrics:
    __slots__ = ("acquisitions", "acquire_wait_total", "acquire_wait_max")

    def __init__(self):
        self.acquisitions = 0
        self.acquire_wait_total = 0.0
        self.acquire_wait_max = 0.0


class Pool:
    def __init__(
        self,
        minsize: int = constants.POOL_MIN_SIZE,
        maxsize: int = constants.POOL_MAX_SIZE,
        pool_recycle: float = constants.POOL_RECYCLE,
        metrics: Optional[bool] = None,
        **kwargs,
    ):
        if maxsize < 1:
            raise ValueError("maxsize is expected to be greater than zero")
        if minsize < 0:
            raise ValueError("minsize is expected to be greater or equal to zero")
        if minsize > maxsize:
            raise ValueError("minsize is greater than maxsize")
        if pool_recycle < -1:
            raise ValueError("pool_recycle is expected to be greater or equal to -1")

        self._maxsize = maxsize
        self._minsize = minsize
        self._pool_recycle = pool_recycle
        self.metrics = PoolMetrics() if resolve_metrics_enabled(metrics) else None
        self._connection_kwargs = kwargs
        self._sem = asyncio.Semaphore(maxsize)
        self._lock = asyncio.Lock()
        self._acquired_connections: set[Connection] = set()
        self._free_connections: deque[Connection] = deque(maxlen=maxsize)
        self._idle_since: dict[Connection, float] = {}
        self._pending_connections = 0
        self._generation = 0
        self._state_changed = asyncio.Event()
        self._startup_event: Optional[asyncio.Event] = None
        self._clock: Callable[[], float] = perf_counter
        self._opened: bool = False
        self._closed: bool = False

    async def __aenter__(self) -> "Pool":
        await self.startup()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.shutdown()

    def __repr__(self) -> str:
        cls_name = type(self).__name__
        status = self.status
        return (
            f"<{cls_name}(minsize={self._minsize}, maxsize={self._maxsize})"
            f" object at 0x{id(self):x}; status: {status}>"
        )

    @property
    def opened(self) -> bool:
        """Returns True if the pool is opened."""

        return self._opened

    @property
    def closed(self) -> bool:
        """Return True if the pool is closed."""

        return self._closed

    @property
    def status(self) -> str:
        """Return the Pool object status."""

        if not (self._opened or self._closed):
            return PoolStatus.created.value
        if self._opened and not self._closed:
            return PoolStatus.opened.value
        if self._closed and not self._opened:
            return PoolStatus.closed.value
        raise AsynchPoolError(f"{self} is in an unknown state")

    @property
    def acquired_connections(self) -> int:
        """Return the number of connections borrowed from the pool."""

        return len(self._acquired_connections)

    @property
    def free_connections(self) -> int:
        """Return the number of idle connections in the pool."""

        return len(self._free_connections)

    @property
    def _pool_size(self) -> int:
        """Return the number of connections currently owned by the pool."""

        return self.acquired_connections + self.free_connections

    @property
    def maxsize(self) -> int:
        return self._maxsize

    @property
    def minsize(self) -> int:
        return self._minsize

    @property
    def pool_recycle(self) -> float:
        """Return the idle age after which a connection is pinged on checkout.

        A value of ``-1`` disables idle pings.
        """

        return self._pool_recycle

    def _take_free_connection(self) -> Optional[tuple[Connection, float]]:
        """Move one free connection to acquired state while the lock is held."""

        if not self._free_connections:
            return None
        conn = self._free_connections.popleft()
        self._acquired_connections.add(conn)
        return conn, self._idle_since.pop(conn)

    def _return_free_connection(self, conn: Connection) -> None:
        """Return a checked connection to the free queue while the lock is held."""

        self._free_connections.append(conn)
        self._idle_since[conn] = self._clock()

    def _notify_state_changed(self) -> None:
        """Wake capacity waiters while the lock is held."""

        event = self._state_changed
        self._state_changed = asyncio.Event()
        event.set()

    def _reserve_connection_slots(self, n: int) -> Optional[int]:
        """Reserve physical-connection capacity while the lock is held."""

        if self._closed:
            raise AsynchPoolError(f"{self} is closed")
        if (self._pool_size + self._pending_connections + n) > self.maxsize:
            return None
        self._pending_connections += n
        return self._generation

    def _finish_connection_reservation(self, n: int) -> None:
        """Release a completed reservation while the lock is held."""

        self._pending_connections -= n
        self._notify_state_changed()

    def _remove_acquired_connection(self, conn: Connection) -> bool:
        """Remove a borrowed connection while the lock is held."""

        if conn not in self._acquired_connections:
            return False
        self._acquired_connections.remove(conn)
        return True

    async def _discard_connection(self, conn: Connection) -> None:
        """Close a connection without exposing pool-maintenance failures."""

        try:
            await conn.close()
        except Exception:
            logger.warning("failed to discard %s", conn, exc_info=True)

    async def _discard_acquired_connection(self, conn: Connection) -> None:
        async with self._lock:
            self._remove_acquired_connection(conn)
        await self._discard_connection(conn)

    async def _new_connection(self) -> Connection:
        """Create a connected connection outside the pool lock."""

        conn = Connection(**self._connection_kwargs)
        try:
            await conn.connect()
        except Exception as exc:
            await self._discard_connection(conn)
            msg = f"failed to create a {conn} for {self}"
            raise AsynchPoolError(msg) from exc
        return conn

    def _needs_idle_ping(self, idle_since: float) -> bool:
        return self.pool_recycle >= 0 and (self._clock() - idle_since) >= self.pool_recycle

    async def _acquire_connection(self) -> Connection:
        while True:
            # This critical section is deliberately synchronous: it only moves
            # ownership between the free deque and acquired set.  Network I/O
            # below must never be performed while holding the pool lock.
            async with self._lock:
                candidate = self._take_free_connection()
                candidate_generation = self._generation
                reservation = None
                wait_event = None
                if candidate is None:
                    reservation = self._reserve_connection_slots(1)
                    if reservation is None:
                        wait_event = self._state_changed

            if candidate is None:
                if wait_event is not None:
                    await wait_event.wait()
                    continue
                try:
                    conn = await self._new_connection()
                except Exception:
                    async with self._lock:
                        self._finish_connection_reservation(1)
                    raise
                async with self._lock:
                    self._finish_connection_reservation(1)
                    if reservation == self._generation and not self._closed:
                        self._acquired_connections.add(conn)
                        return conn
                await self._discard_connection(conn)
                raise AsynchPoolError(f"{self} was closed while creating a connection")

            conn, idle_since = candidate
            if not conn.connected or conn.is_query_executing:
                await self._discard_acquired_connection(conn)
                continue

            if self._needs_idle_ping(idle_since):
                try:
                    await conn.ping()
                except Exception:
                    logger.debug("idle connection %s failed its checkout ping", conn, exc_info=True)
                    await self._discard_acquired_connection(conn)
                    continue
                async with self._lock:
                    if (
                        candidate_generation == self._generation
                        and not self._closed
                        and conn in self._acquired_connections
                    ):
                        return conn
                await self._discard_connection(conn)
                raise AsynchPoolError(f"{self} was closed while acquiring a connection")

            # The common checkout path has no await after the synchronous
            # ownership transfer, so shutdown cannot interleave here.
            return conn

    async def _release_connection(self, conn: Connection) -> None:
        # As on acquire, the lock only protects synchronous ownership updates.
        # Replenishment is deliberately lazy on a later acquire, so a borrower
        # never waits for connection creation while returning its connection.
        async with self._lock:
            if not self._remove_acquired_connection(conn):
                raise AsynchPoolError(f"the connection {conn} does not belong to {self}")

            if conn.connected and not conn.is_query_executing:
                self._return_free_connection(conn)
                return

        await self._discard_connection(conn)

    async def _init_connections(self, n: int, *, strict: bool = False) -> None:
        if n < 0:
            msg = f"cannot create a negative number ({n}) of connections for {self}"
            raise ValueError(msg)
        async with self._lock:
            reservation = self._reserve_connection_slots(n)
            if reservation is None:
                msg = f"adding {n} connections will exceed {self}'s maxsize ({self.maxsize})"
                raise AsynchPoolError(msg)
        if not n:
            return

        results = await asyncio.gather(
            *(self._new_connection() for _ in range(n)), return_exceptions=True
        )
        connections = [result for result in results if isinstance(result, Connection)]
        failures = [result for result in results if isinstance(result, Exception)]
        async with self._lock:
            self._finish_connection_reservation(n)
            keep_connections = (
                reservation == self._generation
                and not self._closed
                and (not strict or not failures)
            )
            if keep_connections:
                for conn in connections:
                    self._return_free_connection(conn)

        if not keep_connections:
            for conn in connections:
                await self._discard_connection(conn)
        if strict and failures:
            msg = f"failed to create the {n} connection(s) for the {self}"
            raise AsynchPoolError(msg) from failures[0]
        if reservation != self._generation or self._closed:
            raise AsynchPoolError(f"{self} was closed while creating connections")

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[Connection]:
        """Borrow an exclusive connection and return it when the context exits."""

        metrics = self.metrics
        if metrics is not None:
            start_time = perf_counter()

        async with self._sem:
            conn = await self._acquire_connection()
            if metrics is not None:
                acquire_wait = perf_counter() - start_time
                metrics.acquisitions += 1
                metrics.acquire_wait_total += acquire_wait
                metrics.acquire_wait_max = max(metrics.acquire_wait_max, acquire_wait)
            try:
                yield conn
            finally:
                try:
                    await self._release_connection(conn)
                except AsynchPoolError as exc:
                    logger.warning(exc)

    async def startup(self) -> "Pool":
        """Fill the pool to ``minsize`` before marking it open."""

        async with self._lock:
            self._closed = False
            if self._opened:
                return self
            if self._startup_event is not None:
                startup_event = self._startup_event
                initiator = False
            else:
                startup_event = asyncio.Event()
                self._startup_event = startup_event
                generation = self._generation
                initiator = True

        if not initiator:
            await startup_event.wait()
            async with self._lock:
                startup_succeeded = self.opened
            if startup_succeeded:
                return self
            raise AsynchPoolError(f"{self} startup was interrupted")

        try:
            await self._init_connections(self.minsize, strict=True)
        except Exception:
            async with self._lock:
                if self._startup_event is startup_event:
                    self._startup_event = None
                    startup_event.set()
            raise

        async with self._lock:
            if generation != self._generation or self._closed:
                self._startup_event = None
                startup_event.set()
                raise AsynchPoolError(f"{self} startup was interrupted by shutdown")
            self._opened = True
            self._startup_event = None
            startup_event.set()
        return self

    async def shutdown(self) -> None:
        """Detach all connections, then close them outside the pool lock."""

        # The lock covers only the synchronous container drain.  Closing the
        # sockets outside it lets concurrent borrowers finish their cleanup.
        async with self._lock:
            connections = [*self._free_connections, *self._acquired_connections]
            self._free_connections.clear()
            self._acquired_connections.clear()
            self._idle_since.clear()
            self._generation += 1
            self._opened = False
            self._closed = True
            self._notify_state_changed()

        for conn in connections:
            await self._discard_connection(conn)
