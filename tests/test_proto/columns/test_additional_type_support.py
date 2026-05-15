from datetime import time, timedelta
from types import SimpleNamespace

import pytest

from asynch.proto.columns import get_column_by_spec
from asynch.proto.columns.arraycolumn import ArrayColumn
from asynch.proto.columns.bfloat16column import BFloat16Column
from asynch.proto.columns.boolcolumn import BoolColumn
from asynch.proto.columns.datetimecolumn import DateTimeColumn
from asynch.proto.columns.decimalcolumn import Decimal32Column
from asynch.proto.columns.floatcolumn import Float32, Float64
from asynch.proto.columns.intcolumn import (
    Int8Column,
    Int16Column,
    Int32Column,
    UInt16Column,
    UInt64Column,
)
from asynch.proto.columns.ipcolumn import IPv4Column
from asynch.proto.columns.stringcolumn import FixedString, String
from asynch.proto.columns.timecolumn import Time64Column, TimeColumn
from tests.test_proto.protocol_helpers import make_buffered_reader


def _complete_context(column_options):
    column_options["context"].settings = {}
    column_options["context"].server_info = SimpleNamespace(get_timezone=lambda: "UTC")


@pytest.mark.parametrize(
    "spec, expected_type",
    [
        ("TINYINT SIGNED", Int8Column),
        ("SMALLINT", Int16Column),
        ("INT", Int32Column),
        ("YEAR", UInt16Column),
        ("UNSIGNED", UInt64Column),
        ("FLOAT", Float32),
        ("DOUBLE PRECISION", Float64),
        ("BOOLEAN", BoolColumn),
        ("VARCHAR", String),
        ("VARCHAR(255)", String),
        ("BINARY(16)", FixedString),
        ("Decimal32(2)", Decimal32Column),
        ("DEC(9, 2)", Decimal32Column),
        ("TIMESTAMP", DateTimeColumn),
        ("INET4", IPv4Column),
    ],
)
def test_clickhouse_type_aliases(column_options, spec, expected_type):
    _complete_context(column_options)
    column = get_column_by_spec(spec, column_options)

    assert isinstance(column, expected_type)


@pytest.mark.parametrize(
    "spec",
    [
        "IntervalNanosecond",
        "IntervalMicrosecond",
        "IntervalMillisecond",
        "IntervalQuarter",
    ],
)
def test_newer_interval_types_parse(column_options, spec):
    column = get_column_by_spec(spec, column_options)

    assert column.ch_type == spec


@pytest.mark.parametrize(
    "spec",
    ["LineString", "MultiLineString"],
)
def test_remaining_geo_aliases_parse(column_options, spec):
    column = get_column_by_spec(spec, column_options)

    assert isinstance(column, ArrayColumn)


@pytest.mark.asyncio
async def test_bfloat16_read_write(column_options):
    column = get_column_by_spec("BFloat16", column_options)

    assert isinstance(column, BFloat16Column)

    await column.write_items([42.7, -1.25])
    assert bytes(column.writer.buffer) == b"*B\xa0\xbf"

    column.reader = make_buffered_reader(bytes(column.writer.buffer))
    assert await column.read_items(2) == (42.5, -1.25)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "spec, items, expected_buffer, expected",
    [
        (
            "Time",
            [time(1, 2, 3), "999:59:59", -5],
            b"\x8b\x0e\x00\x00\x7f\xee6\x00\xfb\xff\xff\xff",
            (
                timedelta(hours=1, minutes=2, seconds=3),
                timedelta(hours=999, minutes=59, seconds=59),
                -timedelta(seconds=5),
            ),
        ),
        (
            "Time64(3)",
            ["01:02:03.456", timedelta(seconds=-5, microseconds=-250000)],
            b"\xc0\xd08\x00\x00\x00\x00\x00~\xeb\xff\xff\xff\xff\xff\xff",
            (
                timedelta(hours=1, minutes=2, seconds=3, milliseconds=456),
                -timedelta(seconds=5, milliseconds=250),
            ),
        ),
    ],
)
async def test_time_columns_read_write(column_options, spec, items, expected_buffer, expected):
    column = get_column_by_spec(spec, column_options)

    assert isinstance(column, (TimeColumn, Time64Column))

    await column.write_data(list(items))
    assert bytes(column.writer.buffer) == expected_buffer

    column.reader = make_buffered_reader(bytes(column.writer.buffer))
    assert await column.read_data(len(items)) == expected
