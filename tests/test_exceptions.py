import pytest

from asynch.errors import OperationalError, ServerException
from asynch.pool import Pool


@pytest.mark.no_clickhouse
def test_server_exception_is_not_a_network_operational_error_inv_e2():
    assert not issubclass(ServerException, OperationalError)


@pytest.mark.asyncio
async def test_database_exists(config):
    async with Pool(dsn=config.dsn) as pool:
        async with pool.connection() as conn:
            async with conn.cursor() as cursor:
                with pytest.raises(ServerException):
                    await cursor.execute("create database test")
