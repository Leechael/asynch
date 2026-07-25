import pytest

from asynch.errors import (
    ErrorCode,
    OperationalError,
    ServerCannotParseTextError,
    ServerException,
    ServerTypeMismatchError,
    server_exception_for,
)
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


@pytest.mark.no_clickhouse
def test_actionable_server_codes_get_their_own_class():
    """Callers can catch these specifically without losing `except ServerException`."""

    assert server_exception_for(ErrorCode.CANNOT_PARSE_TEXT) is ServerCannotParseTextError
    assert server_exception_for(ErrorCode.TYPE_MISMATCH) is ServerTypeMismatchError
    assert issubclass(ServerCannotParseTextError, ServerException)
    assert issubclass(ServerTypeMismatchError, ServerException)


@pytest.mark.no_clickhouse
def test_unmapped_server_codes_stay_plain():
    assert server_exception_for(ErrorCode.SYNTAX_ERROR) is ServerException


@pytest.mark.no_clickhouse
def test_hint_follows_the_server_message_without_replacing_it():
    rendered = str(ServerCannotParseTextError("Cannot parse string", ErrorCode.CANNOT_PARSE_TEXT))

    assert "Cannot parse string" in rendered
    assert "date_time_input_format='best_effort'" in rendered
    assert "docs/datetime-parameters.md" in rendered
