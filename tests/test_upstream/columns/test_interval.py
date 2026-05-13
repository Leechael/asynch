import pytest

from tests.test_upstream.columns._helpers import execute

pytestmark = pytest.mark.asyncio


async def test_all(conn):
    interval = [
        ("YEAR", 1),
        ("MONTH", 2),
        ("WEEK", 3),
        ("DAY", 4),
        ("HOUR", 5),
        ("MINUTE", 6),
        ("SECOND", 7),
    ]
    columns = ", ".join([f"INTERVAL {value} {kind}" for kind, value in interval])

    rv = await execute(conn, f"SELECT {columns}")

    assert rv == [(1, 2, 3, 4, 5, 6, 7)]
