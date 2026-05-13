import pytest

from tests.test_upstream.columns._helpers import execute

pytestmark = pytest.mark.asyncio


async def test_select_null(conn):
    rv = await execute(conn, "SELECT NULL")

    assert rv == [(None,)]
