import json
from types import SimpleNamespace

import pytest

from asynch.proto.columns import get_column_by_spec
from asynch.proto.columns.aggregatefunctioncolumn import AggregateFunctionColumn
from asynch.proto.columns.dynamiccolumn import DynamicColumn
from asynch.proto.columns.jsoncolumn import JsonColumn
from asynch.proto.columns.qbitcolumn import QBitColumn
from asynch.proto.columns.variantcolumn import VariantColumn
from asynch.proto.streams.buffered import BufferedWriter
from tests.test_proto.protocol_helpers import make_buffered_reader


def _context():
    return SimpleNamespace(
        client_settings={"strings_encoding": "utf-8", "strings_as_bytes": False},
        settings={},
        server_info=SimpleNamespace(get_timezone=lambda: "UTC"),
    )


def _column_options(payload=b""):
    return {
        "context": _context(),
        "reader": make_buffered_reader(payload),
        "writer": BufferedWriter(),
    }


@pytest.mark.parametrize(
    "spec, expected_type",
    [
        ("Variant(UInt8, String)", VariantColumn),
        ("Dynamic", DynamicColumn),
        ("QBit(Float32, 2)", QBitColumn),
        ("Geometry", VariantColumn),
        ("GEOMETRY", VariantColumn),
        ("JSON", JsonColumn),
        ("AggregateFunction(count)", AggregateFunctionColumn),
    ],
)
def test_complex_type_families_parse(spec, expected_type):
    column = get_column_by_spec(spec, _column_options())

    assert isinstance(column, expected_type)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "spec, items, expected",
    [
        ("Variant(UInt8, String)", [("UInt8", 1)], bytes.fromhex("00 00 00 00 00 00 00 00 01 01")),
        (
            "Dynamic",
            [1],
            bytes.fromhex(
                "01 00 00 00 00 00 00 00 01 01 05 55 49 6e 74 38 "
                "00 00 00 00 00 00 00 00 01 01"
            ),
        ),
        (
            "QBit(Float32, 2)",
            [[1.0, 2.0]],
            bytes.fromhex(
                "00 02 01 01 01 01 01 01 01 00 00 00 00 00 00 00 "
                "00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00"
            ),
        ),
        (
            "Geometry",
            [("Point", (1.0, 2.0))],
            bytes.fromhex(
                "00 00 00 00 00 00 00 00 03 00 00 00 00 00 00 f0 "
                "3f 00 00 00 00 00 00 00 40"
            ),
        ),
        ("AggregateFunction(count)", [300], bytes.fromhex("ac 02")),
        ("AggregateFunction(sum, UInt64)", [5], bytes.fromhex("05 00 00 00 00 00 00 00")),
        ("AggregateFunction(avg, UInt8)", [(6, 3)], bytes.fromhex("06 00 00 00 00 00 00 00 03")),
    ],
)
async def test_complex_type_native_bytes(spec, items, expected):
    column = get_column_by_spec(spec, _column_options())

    await column.write_state_prefix()
    await column.write_data(items)

    assert bytes(column.writer.buffer) == expected


@pytest.mark.asyncio
async def test_json_writes_native_string_serialization():
    column = get_column_by_spec("JSON", _column_options())

    await column.write_state_prefix()
    await column.write_data([{"a": 1}])

    assert bytes(column.writer.buffer) == (
        bytes.fromhex("01 00 00 00 00 00 00 00")
        + len(json.dumps({"a": 1})).to_bytes(1, "little")
        + json.dumps({"a": 1}).encode()
    )
