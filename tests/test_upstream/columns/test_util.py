from asynch.proto.columns.util import get_inner_spec


def test_get_inner_spec():
    inner = "a Tuple(Array(Int8), Array(Int64)), b Nullable(String)"

    assert get_inner_spec("Nested", f"Nested({inner}) dummy ") == inner
