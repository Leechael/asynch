import pytest

from asynch.errors import ErrorCode, ServerException
from tests.test_upstream.columns._helpers import create_table, execute

pytestmark = pytest.mark.asyncio


async def test_simple(conn):
    data = [(3,), (None,), (2,)]

    async with create_table(conn, "a Nullable(Int32)") as table:
        await execute(conn, f"INSERT INTO {table} (a) VALUES", data)

        inserted = await execute(conn, f"SELECT * FROM {table}")

    assert inserted == data


@pytest.mark.parametrize(
    "columns",
    [
        "a Nullable(Nullable(Int32))",
        "a Nullable(Array(Nullable(Array(Nullable(Int32)))))",
    ],
    ids=["nullable_inside_nullable", "nullable_array"],
)
async def test_illegal_nullable_types(conn, columns):
    with pytest.raises(ServerException) as exc:
        await execute(conn, f"CREATE TABLE test.upstream_bad_nullable ({columns}) ENGINE = Memory")

    assert exc.value.code == ErrorCode.ILLEGAL_TYPE_OF_ARGUMENT
