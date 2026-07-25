from datetime import datetime, time, timedelta
from datetime import timezone as dt_timezone

import pytest

from asynch.proto.context import Context
from asynch.proto.cs import ServerInfo
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


def _utc_context() -> Context:
    context = Context()
    context.server_info = ServerInfo(
        name="ClickHouse",
        version_major=25,
        version_minor=4,
        version_patch=0,
        revision=54483,
        timezone="UTC",
        display_name="test",
        used_revision=54483,
    )
    return context


AWARE = datetime(2026, 1, 1, 0, 0, 0, 123456, tzinfo=dt_timezone(timedelta(hours=9)))


def test_typed_datetime_sends_an_aware_value_as_an_instant():
    """Microseconds since the epoch, so no zone or transition can blur it."""

    assert (
        escape_param(AWARE, context=_utc_context(), typed_datetime=True)
        == "fromUnixTimestamp64Micro(1767193200123456)"
    )


def test_two_instants_sharing_a_wall_clock_reading_stay_distinct():
    """A fall-back hour reads the same twice; the epoch form still separates them."""

    first = datetime(2025, 11, 2, 5, 30, 0, 123456, tzinfo=dt_timezone.utc)
    second = datetime(2025, 11, 2, 6, 30, 0, 123456, tzinfo=dt_timezone.utc)
    context = _utc_context()

    assert escape_param(first, context=context, typed_datetime=True) != escape_param(
        second, context=context, typed_datetime=True
    )


def test_naive_datetime_stays_a_bare_string():
    """A naive value means a wall time in the target column's zone.

    A typed literal is read in the server's zone instead, which would move the
    instant whenever the column carries a zone of its own. Only the server can
    resolve a naive value against the column, so the driver leaves that to it.
    """

    assert escape_param(
        datetime(2026, 1, 1, 0, 0, 0, 123456), context=_utc_context(), typed_datetime=True
    ) == ("'2026-01-01 00:00:00.123456'")


def test_aware_datetime_needs_no_negotiated_timezone():
    """An instant is absolute, so the spelling does not depend on server info."""

    assert escape_param(AWARE, typed_datetime=True) == "fromUnixTimestamp64Micro(1767193200123456)"


def test_typed_datetime_leaves_whole_seconds_alone():
    """Without a fraction the bare string is already exact, so nothing changes."""

    whole = datetime(2026, 1, 1, 0, 0, 0, tzinfo=dt_timezone.utc)

    assert escape_param(whole, context=_utc_context(), typed_datetime=True) == (
        "'2026-01-01 00:00:00'"
    )


def test_typed_datetime_is_ignored_for_server_side_parameters():
    """Server-side parameters travel in a typed slot, not as statement text."""

    assert escape_param(AWARE, context=_utc_context(), for_server=True, typed_datetime=True) == (
        "'2025-12-31 15:00:00.123456'"
    )


def test_typed_datetime_reaches_values_nested_in_containers():
    assert escape_param([AWARE], context=_utc_context(), typed_datetime=True) == (
        "[fromUnixTimestamp64Micro(1767193200123456)]"
    )
