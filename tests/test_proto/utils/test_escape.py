from datetime import datetime, time

import pytest

from asynch.proto.utils.escape import escape_param

pytestmark = pytest.mark.no_clickhouse


def test_escape_datetime_omits_fraction_when_microseconds_are_zero():
    assert escape_param(datetime(2026, 1, 1, 0, 0, 0)) == "'2026-01-01 00:00:00'"


def test_escape_datetime_preserves_fraction_when_microseconds_are_present():
    assert (
        escape_param(datetime(2026, 1, 1, 0, 0, 0, 123000))
        == "'2026-01-01 00:00:00.123000'"
    )


def test_escape_time_omits_fraction_when_microseconds_are_zero():
    assert escape_param(time(12, 34, 56)) == "'12:34:56'"


def test_escape_time_preserves_fraction_when_microseconds_are_present():
    assert escape_param(time(12, 34, 56, 789000)) == "'12:34:56.789000'"
