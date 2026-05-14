from datetime import date, datetime
from uuid import UUID

import pytest

from tests.test_upstream.columns._helpers import create_table, execute

pytestmark = pytest.mark.asyncio

JSON_SETTINGS = {"allow_experimental_json_type": 1}


async def test_where_scalar_comparison_operators(conn):
    rows = [
        (1, 10, 1.5, "alpha", True),
        (2, 20, 2.5, "beta", False),
        (3, 30, 3.5, "gamma", True),
    ]

    async with create_table(conn, "id UInt8, i Int32, f Float64, s String, b Bool") as table:
        await execute(conn, f"INSERT INTO {table} VALUES", rows)

        result = await execute(
            conn,
            f"""
            SELECT id FROM {table}
            WHERE i >= %(min_i)s
              AND i < %(max_i)s
              AND f != %(excluded_f)s
              AND s IN %(names)s
              AND b = %(enabled)s
            ORDER BY id
            """,
            {
                "min_i": 10,
                "max_i": 35,
                "excluded_f": 2.5,
                "names": ("alpha", "gamma"),
                "enabled": True,
            },
        )

        between_result = await execute(
            conn,
            f"SELECT id FROM {table} WHERE i BETWEEN %(lo)s AND %(hi)s ORDER BY id",
            {"lo": 10, "hi": 20},
        )

        not_in_result = await execute(
            conn,
            f"SELECT id FROM {table} WHERE s NOT IN %(names)s ORDER BY id",
            {"names": ("beta",)},
        )

    assert result == [(1,), (3,)]
    assert between_result == [(1,), (2,)]
    assert not_in_result == [(1,), (3,)]


async def test_where_string_pattern_operators(conn):
    rows = [(1, "alpha"), (2, "beta"), (3, "alphabet")]

    async with create_table(conn, "id UInt8, s String") as table:
        await execute(conn, f"INSERT INTO {table} VALUES", rows)

        like_result = await execute(
            conn,
            f"SELECT id FROM {table} WHERE s LIKE %(pattern)s ORDER BY id",
            {"pattern": "alph%"},
        )
        regexp_result = await execute(
            conn,
            f"SELECT id FROM {table} WHERE match(s, %(pattern)s) ORDER BY id",
            {"pattern": "^a.*t$"},
        )

    assert like_result == [(1,), (3,)]
    assert regexp_result == [(3,)]


async def test_where_uuid_and_temporal_operators(conn):
    uuid_a = UUID("2efcead4-ff55-4db5-bdb4-6b36a308d8e0")
    uuid_b = UUID("c0fcbba9-0752-44ed-a5d6-4dfb4342b89d")
    rows = [
        (1, uuid_a, date(2024, 1, 1), datetime(2024, 1, 1, 10, 0, 0)),
        (2, uuid_b, date(2024, 2, 1), datetime(2024, 2, 1, 10, 0, 0)),
        (3, uuid_a, date(2024, 3, 1), datetime(2024, 3, 1, 10, 0, 0)),
    ]

    async with create_table(conn, "id UInt8, u UUID, d Date, dt DateTime('UTC')") as table:
        await execute(conn, f"INSERT INTO {table} VALUES", rows)

        result = await execute(
            conn,
            f"""
            SELECT id FROM {table}
            WHERE u = %(uuid)s
              AND d >= CAST(%(from_date)s AS Date)
              AND dt < CAST(%(before_dt)s AS DateTime)
            ORDER BY id
            """,
            {
                "uuid": uuid_a,
                "from_date": date(2024, 1, 15),
                "before_dt": datetime(2024, 4, 1, 0, 0, 0),
            },
        )

    assert result == [(3,)]


async def test_where_nullable_and_tuple_operators(conn):
    rows = [(1, None), (2, 10), (3, 20)]

    async with create_table(conn, "id UInt8, value Nullable(Int32)") as table:
        await execute(conn, f"INSERT INTO {table} VALUES", rows)

        null_result = await execute(
            conn,
            f"SELECT id FROM {table} WHERE value IS NULL ORDER BY id",
        )
        tuple_result = await execute(
            conn,
            f"SELECT id FROM {table} WHERE (id, value) IN %(pairs)s ORDER BY id",
            {"pairs": ((2, 10), (3, 99))},
        )

    assert null_result == [(1,)]
    assert tuple_result == [(2,)]


async def test_where_array_and_map_operators(conn):
    rows = [
        (1, [1, 2, 3], ["red", "blue"], {"role": "admin"}),
        (2, [4, 5], ["green"], {"role": "reader"}),
        (3, [6, 7, 8], ["red", "green"], {"role": "writer"}),
    ]

    async with create_table(
        conn,
        "id UInt8, nums Array(Int32), tags Array(String), attrs Map(String, String)",
    ) as table:
        await execute(conn, f"INSERT INTO {table} VALUES", rows)

        result = await execute(
            conn,
            f"""
            SELECT id FROM {table}
            WHERE has(tags, %(tag)s)
              AND hasAll(nums, %(required)s)
              AND arrayExists(x -> x > %(threshold)s, nums)
              AND attrs['role'] != %(role)s
            ORDER BY id
            """,
            {"tag": "red", "required": [1, 2], "threshold": 2, "role": "reader"},
        )

    assert result == [(1,)]


async def test_where_vector_distance_operators(conn):
    rows = [
        (1, [1.0, 2.0]),
        (2, [4.0, 6.0]),
        (3, [10.0, 10.0]),
    ]

    async with create_table(conn, "id UInt8, vec Array(Float64)") as table:
        await execute(conn, f"INSERT INTO {table} VALUES", rows)

        result = await execute(
            conn,
            f"""
            SELECT id FROM {table}
            WHERE L2Distance(vec, %(needle)s) <= %(max_distance)s
            ORDER BY id
            """,
            {"needle": [1.0, 2.0], "max_distance": 5.0},
        )

    assert result == [(1,), (2,)]


async def test_where_json_operators(conn):
    rows = [
        (1, {"user": {"name": "Ada"}, "score": 10, "active": True}),
        (2, {"user": {"name": "Grace"}, "score": 20, "active": False}),
        (3, {"user": {"name": "Ada"}, "score": 30, "active": True}),
    ]

    async with create_table(conn, "id UInt8, doc JSON", settings=JSON_SETTINGS) as table:
        await execute(conn, f"INSERT INTO {table} VALUES", rows, settings=JSON_SETTINGS)

        result = await execute(
            conn,
            f"""
            SELECT id FROM {table}
            WHERE doc.user.name = %(name)s
              AND doc.score >= %(min_score)s
              AND JSONExtractBool(%(filter)s, 'active') = true
            ORDER BY id
            """,
            {
                "name": "Ada",
                "min_score": 20,
                "filter": {"active": True},
            },
            settings=JSON_SETTINGS,
        )

    assert result == [(3,)]
