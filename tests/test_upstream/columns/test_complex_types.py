import pytest

from asynch.proto import constants
from tests.test_upstream.columns._helpers import create_table, execute

pytestmark = pytest.mark.asyncio


async def _has_type_family(conn, family):
    rows = await execute(
        conn,
        "SELECT count() FROM system.data_type_families WHERE name = %(family)s",
        {"family": family},
    )
    return bool(rows[0][0])


EXPERIMENTAL_SETTINGS = {
    "allow_experimental_dynamic_type": 1,
    "allow_experimental_geo_types": 1,
    "allow_experimental_json_type": 1,
    "allow_experimental_object_type": 1,
    "allow_experimental_qbit_type": 1,
    "allow_experimental_variant_type": 1,
}


def _requires_v2_dynamic_serialization(conn, family):
    if (
        conn._connection.server_info.used_revision
        < constants.DBMS_MIN_REVISION_WITH_V2_DYNAMIC_AND_JSON_SERIALIZATION
    ):
        pytest.skip(f"{family} nested serialization requires protocol revision 54473")


@pytest.mark.parametrize(
    "column_type, value, expected",
    [
        ("Variant(UInt8, String)", ("UInt8", 7), 7),
        ("Variant(UInt8, String)", ("String", "seven"), "seven"),
        ("Dynamic", 7, 7),
    ],
)
async def test_variant_and_dynamic_roundtrip(conn, column_type, value, expected):
    family = column_type.split("(", 1)[0]
    if not await _has_type_family(conn, family):
        pytest.skip(f"ClickHouse server does not expose {family}")
    if family == "Variant":
        _requires_v2_dynamic_serialization(conn, family)

    async with create_table(conn, f"a {column_type}", settings=EXPERIMENTAL_SETTINGS) as table:
        await execute(
            conn, f"INSERT INTO {table} VALUES", [(value,)], settings=EXPERIMENTAL_SETTINGS
        )

        inserted = await execute(conn, f"SELECT * FROM {table}", settings=EXPERIMENTAL_SETTINGS)

    assert inserted == [(expected,)]


async def test_json_roundtrip(conn):
    if not await _has_type_family(conn, "JSON"):
        pytest.skip("ClickHouse server does not expose JSON")

    value = {"a": 1, "b": "x", "c": 1.5, "d": True, "empty": {}}
    async with create_table(conn, "a JSON", settings=EXPERIMENTAL_SETTINGS) as table:
        await execute(
            conn, f"INSERT INTO {table} VALUES", [(value,)], settings=EXPERIMENTAL_SETTINGS
        )

        inserted = await execute(conn, f"SELECT * FROM {table}", settings=EXPERIMENTAL_SETTINGS)

    assert inserted == [(value,)]


@pytest.mark.parametrize("column_type", ["QBit(Float32, 2)", "QBit(Float64, 2)"])
async def test_qbit_roundtrip(conn, column_type):
    if not await _has_type_family(conn, "QBit"):
        pytest.skip("ClickHouse server does not expose QBit")

    async with create_table(conn, f"a {column_type}", settings=EXPERIMENTAL_SETTINGS) as table:
        await execute(
            conn, f"INSERT INTO {table} VALUES", [([1.0, 2.0],)], settings=EXPERIMENTAL_SETTINGS
        )

        inserted = await execute(conn, f"SELECT * FROM {table}", settings=EXPERIMENTAL_SETTINGS)

    assert inserted == [([1.0, 2.0],)]


async def test_dynamic_mixed_block_roundtrip(conn):
    if not await _has_type_family(conn, "Dynamic"):
        pytest.skip("ClickHouse server does not expose Dynamic")

    values = [(7,), ("x",), (1.5,), (True,)]
    async with create_table(conn, "a Dynamic", settings=EXPERIMENTAL_SETTINGS) as table:
        await execute(conn, f"INSERT INTO {table} VALUES", values, settings=EXPERIMENTAL_SETTINGS)

        inserted = await execute(conn, f"SELECT * FROM {table}", settings=EXPERIMENTAL_SETTINGS)

    assert inserted == values


@pytest.mark.parametrize(
    ("column_type", "values", "expected"),
    [
        ("Array(Dynamic)", [([1, "x", True],)], [([1, "x", True],)]),
        ("Tuple(Dynamic, Dynamic)", [((1, "x"),)], [((1, "x"),)]),
        ("Map(String, Dynamic)", [({"a": 1, "b": "x"},)], [({"a": 1, "b": "x"},)]),
    ],
)
async def test_dynamic_nested_roundtrip(conn, column_type, values, expected):
    if not await _has_type_family(conn, "Dynamic"):
        pytest.skip("ClickHouse server does not expose Dynamic")

    async with create_table(conn, f"a {column_type}", settings=EXPERIMENTAL_SETTINGS) as table:
        await execute(
            conn,
            f"INSERT INTO {table} VALUES",
            values,
            settings=EXPERIMENTAL_SETTINGS,
        )
        inserted = await execute(conn, f"SELECT * FROM {table}", settings=EXPERIMENTAL_SETTINGS)

    assert inserted == expected


async def test_array_variant_roundtrip(conn):
    if not await _has_type_family(conn, "Variant"):
        pytest.skip("ClickHouse server does not expose Variant")
    _requires_v2_dynamic_serialization(conn, "Variant")

    values = [([("UInt8", 7), ("String", "seven")],)]
    async with create_table(
        conn,
        "a Array(Variant(UInt8, String))",
        settings=EXPERIMENTAL_SETTINGS,
    ) as table:
        await execute(
            conn,
            f"INSERT INTO {table} VALUES",
            values,
            settings=EXPERIMENTAL_SETTINGS,
        )
        inserted = await execute(conn, f"SELECT * FROM {table}", settings=EXPERIMENTAL_SETTINGS)

    assert inserted == [([7, "seven"],)]


async def test_array_json_roundtrip(conn):
    if not await _has_type_family(conn, "JSON"):
        pytest.skip("ClickHouse server does not expose JSON")
    _requires_v2_dynamic_serialization(conn, "JSON")

    values = [([{"a": 1}, {"b": "x"}],)]
    async with create_table(conn, "a Array(JSON)", settings=EXPERIMENTAL_SETTINGS) as table:
        await execute(
            conn,
            f"INSERT INTO {table} VALUES",
            values,
            settings=EXPERIMENTAL_SETTINGS,
        )
        inserted = await execute(conn, f"SELECT * FROM {table}", settings=EXPERIMENTAL_SETTINGS)

    assert inserted == values


async def test_array_qbit_roundtrip(conn):
    if not await _has_type_family(conn, "QBit"):
        pytest.skip("ClickHouse server does not expose QBit")

    values = [([[1.0, 2.0], [3.0, 4.0]],)]
    async with create_table(
        conn,
        "a Array(QBit(Float32, 2))",
        settings=EXPERIMENTAL_SETTINGS,
    ) as table:
        await execute(
            conn,
            f"INSERT INTO {table} VALUES",
            values,
            settings=EXPERIMENTAL_SETTINGS,
        )
        inserted = await execute(conn, f"SELECT * FROM {table}", settings=EXPERIMENTAL_SETTINGS)

    assert inserted == values


async def test_geometry_roundtrip(conn):
    if not await _has_type_family(conn, "Geometry"):
        pytest.skip("ClickHouse server does not expose Geometry")

    async with create_table(conn, "a Geometry", settings=EXPERIMENTAL_SETTINGS) as table:
        await execute(
            conn,
            f"INSERT INTO {table} VALUES",
            [(("Point", (1.0, 2.0)),)],
            settings=EXPERIMENTAL_SETTINGS,
        )

        inserted = await execute(conn, f"SELECT * FROM {table}", settings=EXPERIMENTAL_SETTINGS)

    assert inserted == [((1.0, 2.0),)]


@pytest.mark.parametrize(
    "column_type, state, expected",
    [
        ("AggregateFunction(count)", 300, 300),
        ("AggregateFunction(sum, UInt64)", 5, 5),
        ("AggregateFunction(sum, Float32)", 1.5, 1.5),
        ("AggregateFunction(avg, UInt8)", (6, 3), 2.0),
    ],
)
async def test_aggregate_function_roundtrip(conn, column_type, state, expected):
    if not await _has_type_family(conn, "AggregateFunction"):
        pytest.skip("ClickHouse server does not expose AggregateFunction")

    async with create_table(conn, f"a {column_type}") as table:
        await execute(conn, f"INSERT INTO {table} VALUES", [(state,)])

        inserted = await execute(conn, f"SELECT finalizeAggregation(a) FROM {table}")

    assert inserted == [(expected,)]
