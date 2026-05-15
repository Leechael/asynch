import logging
from io import StringIO

import pytest

from asynch.connection import Connection

pytestmark = pytest.mark.asyncio

TRACEPARENT = "00-1af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
SERVER_TRACE_ID = "8448eb211c80319c1af7651916cd43dd"


def _connection_kwargs(config, settings):
    connection_settings = config.settings.copy()
    connection_settings.update(settings)
    return {
        "host": config.host,
        "port": config.port,
        "database": config.database,
        "user": config.user,
        "password": config.password,
        "settings": connection_settings,
    }


async def _execute(connection, query, settings=None):
    return await connection._connection.execute(query, settings=settings)


class CapturedConnectionLogs:
    def __enter__(self):
        self.buffer = StringIO()
        self.handler = logging.StreamHandler(self.buffer)
        self.logger = logging.getLogger("asynch.proto.connection")
        self.original_level = self.logger.level
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(self.handler)
        return self.buffer

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logger.removeHandler(self.handler)
        self.logger.setLevel(self.original_level)


@pytest.mark.parametrize(
    "settings",
    [
        {
            "opentelemetry_tracestate": "tracestate",
            "opentelemetry_traceparent": TRACEPARENT,
        },
        {"opentelemetry_traceparent": TRACEPARENT},
    ],
    ids=["tracestate", "no_tracestate"],
)
async def test_server_logs(config, settings):
    async with Connection(**_connection_kwargs(config, settings)) as conn:
        with CapturedConnectionLogs() as buffer:
            await _execute(conn, "SELECT 1", settings={"send_logs_level": "trace"})

    value = buffer.getvalue()
    assert "OpenTelemetry" in value
    assert SERVER_TRACE_ID in value or TRACEPARENT in value


@pytest.mark.parametrize(
    "traceparent, expected",
    [
        ("bad", "unexpected length 3, expected 55"),
        (
            "00-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-yyyyyyyyyyyyyyyy-01",
            "Malformed traceparant header: 00-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-yyyyyyyyyyyyyyyy-01",
        ),
        (
            "01-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
            "unexpected version 01, expected 00",
        ),
    ],
    ids=["bad_length", "malformed", "bad_version"],
)
async def test_bad_traceparent(config, traceparent, expected):
    settings = {"opentelemetry_traceparent": traceparent}

    async with Connection(**_connection_kwargs(config, settings)) as conn:
        with pytest.raises(ValueError) as exc:
            await _execute(conn, "SELECT 1")

    assert str(exc.value) == expected
