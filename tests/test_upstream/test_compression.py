from datetime import date, datetime

import pytest

from asynch import errors
from asynch.connection import Connection
from asynch.proto.compression import get_compressor_cls
from asynch.proto.compression.lz4 import Compressor as LZ4Compressor

pytestmark = pytest.mark.asyncio


async def _execute(connection, query, args=None, settings=None):
    return await connection._connection.execute(query, args=args, settings=settings)


async def _drop_table(connection, table_name):
    await _execute(connection, f"DROP TABLE IF EXISTS {table_name}")


async def _create_table(connection, table_name, columns):
    await _execute(connection, f"CREATE TABLE {table_name} ({columns}) ENGINE=Memory")


def _server_compression_settings(compression):
    if compression is True:
        method = "lz4"
    else:
        method = compression

    return {"network_compression_method": method.upper()}


def _connection_kwargs(config, compression):
    settings = config.settings.copy()
    settings.update(_server_compression_settings(compression))
    return {
        "host": config.host,
        "port": config.port,
        "database": config.database,
        "user": config.user,
        "password": config.password,
        "compression": compression,
        "settings": settings,
    }


@pytest.mark.parametrize("compression", ["lz4", "lz4hc", "zstd"])
async def test_compressed_connection_reads_and_writes_inserted_rows(config, compression):
    table_name = f"test.upstream_compression_{compression}"
    data = [(date(2012, 10, 25), datetime(2012, 10, 25, 14, 7, 19))]

    async with Connection(**_connection_kwargs(config, compression)) as conn:
        await _drop_table(conn, table_name)
        await _create_table(conn, table_name, "a Date, b DateTime")
        try:
            await _execute(conn, f"INSERT INTO {table_name} (a, b) VALUES", data)

            inserted = await _execute(conn, f"SELECT * FROM {table_name}")
            assert inserted == data
        finally:
            await _drop_table(conn, table_name)


async def test_default_compression_uses_lz4():
    conn = Connection("clickhouse://localhost", compression=True)

    assert conn._connection.compressor_cls is LZ4Compressor


async def test_unknown_compressor():
    with pytest.raises(errors.UnknownCompressionMethod) as exc:
        get_compressor_cls("hello")

    assert exc.value.code == errors.ErrorCode.UNKNOWN_COMPRESSION_METHOD


async def test_compressed_read_by_blocks(config):
    table_name = "test.upstream_compression_read_by_blocks"
    data = [(x % 200,) for x in range(100_000)]

    async with Connection(**_connection_kwargs(config, "lz4")) as conn:
        await _drop_table(conn, table_name)
        await _create_table(conn, table_name, "a Int32")
        try:
            await _execute(conn, f"INSERT INTO {table_name} (a) VALUES", data)

            inserted = await _execute(
                conn,
                f"SELECT * FROM {table_name}",
                settings={"max_block_size": 10_000},
            )
            assert inserted == data
        finally:
            await _drop_table(conn, table_name)
