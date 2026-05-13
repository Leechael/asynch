from contextlib import asynccontextmanager

TABLE_NAME = "test.upstream_columns"


async def execute(conn, query, args=None, **kwargs):
    return await conn._connection.execute(query, args=args, **kwargs)


@asynccontextmanager
async def create_table(conn, columns):
    await execute(conn, f"DROP TABLE IF EXISTS {TABLE_NAME}")
    await execute(conn, f"CREATE TABLE {TABLE_NAME} ({columns}) ENGINE=Memory")
    try:
        yield TABLE_NAME
    finally:
        await execute(conn, f"DROP TABLE IF EXISTS {TABLE_NAME}")
