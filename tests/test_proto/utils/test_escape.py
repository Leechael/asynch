from datetime import datetime, time

import pytest

from asynch.proto.utils.escape import escape_param

pytestmark = pytest.mark.no_clickhouse


def test_escape_datetime_omits_fraction_when_microseconds_are_zero():
    assert escape_param(datetime(2026, 1, 1, 0, 0, 0)) == "'2026-01-01 00:00:00'"


def test_escape_datetime_preserves_fraction_when_microseconds_are_present():
    assert escape_param(datetime(2026, 1, 1, 0, 0, 0, 123000)) == "'2026-01-01 00:00:00.123000'"


def test_escape_time_omits_fraction_when_microseconds_are_zero():
    assert escape_param(time(12, 34, 56)) == "'12:34:56'"


def test_escape_time_preserves_fraction_when_microseconds_are_present():
    assert escape_param(time(12, 34, 56, 789000)) == "'12:34:56.789000'"


def test_typed_datetime_spells_out_the_type_when_a_fraction_is_present():
    assert (
        escape_param(datetime(2026, 1, 1, 0, 0, 0, 123456), typed_datetime=True)
        == "toDateTime64('2026-01-01 00:00:00.123456', 6)"
    )


def test_typed_datetime_leaves_whole_seconds_alone():
    """Without a fraction the bare string is already exact, so nothing changes."""

    assert escape_param(datetime(2026, 1, 1, 0, 0, 0), typed_datetime=True) == (
        "'2026-01-01 00:00:00'"
    )


def test_typed_datetime_is_ignored_for_server_side_parameters():
    """Server-side parameters travel in a typed slot, not as statement text."""

    assert escape_param(
        datetime(2026, 1, 1, 0, 0, 0, 123456), for_server=True, typed_datetime=True
    ) == ("'2026-01-01 00:00:00.123456'")


def test_typed_datetime_reaches_values_nested_in_containers():
    assert escape_param([datetime(2026, 1, 1, 0, 0, 0, 123456)], typed_datetime=True) == (
        "[toDateTime64('2026-01-01 00:00:00.123456', 6)]"
    )
