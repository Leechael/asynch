from datetime import date, datetime

import pytest

pytestmark = pytest.mark.asyncio

JSON_SETTINGS = {
    "allow_experimental_json_type": 1,
    "allow_experimental_object_type": 1,
}


async def execute(conn, query, params=None, settings=None):
    return await conn._connection.execute(query, args=params, settings=settings)


async def has_type_family(conn, family):
    rows = await execute(
        conn,
        "SELECT count() FROM system.data_type_families WHERE name = %(family)s",
        {"family": family},
    )
    return bool(rows[0][0])


async def test_array_operator_expressions_accept_python_arrays(conn):
    params = {"values": [1, 2, 3], "extra": [4, 5]}

    result = await execute(
        conn,
        """
        SELECT
            has(%(values)s, 2),
            arraySum(arrayMap(x -> x * 2, %(values)s)),
            arrayFilter(x -> x > 1, %(values)s),
            arrayConcat(%(values)s, %(extra)s)
        """,
        params,
    )

    assert result == [(True, 12, [2, 3], [1, 2, 3, 4, 5])]


async def test_vector_distance_expressions_accept_python_tuples(conn):
    params = {
        "x": (1.0, 2.0),
        "y": (4.0, 6.0),
        "orthogonal_x": (1.0, 0.0),
        "orthogonal_y": (0.0, 1.0),
    }

    result = await execute(
        conn,
        """
        SELECT
            round(L2Distance(%(x)s, %(y)s), 4),
            round(cosineDistance(%(orthogonal_x)s, %(orthogonal_y)s), 4)
        """,
        params,
    )

    assert result == [(5.0, 1.0)]


async def test_json_operator_expressions_accept_python_dicts(conn):
    if not await has_type_family(conn, "JSON"):
        pytest.skip("ClickHouse server does not expose JSON")

    params = {"doc": {"a": {"b": "x"}, "n": 42, "flag": True}}

    result = await execute(
        conn,
        """
        SELECT
            JSONExtractString(%(doc)s, 'a', 'b'),
            JSONExtractInt(%(doc)s, 'n'),
            JSON_VALUE(%(doc)s, '$.a.b')
        """,
        params,
        settings=JSON_SETTINGS,
    )

    assert result == [("x", 42, "x")]


async def test_date_operator_expressions_accept_python_dates(conn):
    params = {
        "start": date(2024, 1, 1),
        "end": date(2024, 1, 31),
        "dt": datetime(2024, 1, 15, 12, 34, 56),
    }

    result = await execute(
        conn,
        """
        SELECT
            dateDiff('day', CAST(%(start)s AS Date), CAST(%(end)s AS Date)),
            addDays(CAST(%(start)s AS Date), 7),
            toStartOfMonth(CAST(%(dt)s AS DateTime))
        """,
        params,
    )

    assert result == [
        (
            30,
            date(2024, 1, 8),
            date(2024, 1, 1),
        )
    ]
