from datetime import datetime, time, timedelta, tzinfo
from datetime import timezone as dt_timezone
from enum import Enum, IntEnum

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


def test_aware_whole_seconds_are_sent_as_an_instant_too():
    """A wall time carries no zone, so a zoned column would read it as its own.

    Nothing about that depends on a sub-second part being present, so an aware
    value takes the instant route whether or not it has one.
    """

    whole = datetime(2026, 1, 1, 0, 0, 0, tzinfo=dt_timezone.utc)

    assert escape_param(whole, context=_utc_context(), typed_datetime=True) == (
        "fromUnixTimestamp64Micro(1767225600000000)"
    )


def test_naive_whole_seconds_stay_a_bare_string():
    """Still the server's to resolve against the column."""

    whole = datetime(2026, 1, 1, 0, 0, 0)

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


class _NoOffset(tzinfo):
    """A tzinfo that declines to say what the offset is."""

    def utcoffset(self, dt):
        return None

    def tzname(self, dt):
        return "X"

    def dst(self, dt):
        return None


def test_tzinfo_without_an_offset_is_not_treated_as_aware():
    """Python calls such a value naive, and epoch arithmetic on it would raise."""

    value = datetime(2026, 1, 1, 0, 0, 0, 123456, tzinfo=_NoOffset())

    assert escape_param(value, typed_datetime=True) == "'2026-01-01 00:00:00.123456'"


def test_escaping_survives_a_context_without_server_info():
    """The utility is usable before a connection has negotiated anything."""

    whole = datetime(2026, 1, 1, 0, 0, 0, tzinfo=dt_timezone.utc)

    # With the spelling turned off an aware value takes the wall-time branch,
    # which is the one that reaches for a negotiated timezone.
    assert escape_param(whole, context=Context()) == "'2026-01-01 00:00:00'"


class HashedId(int):
    """An integer row id whose text form is a prefixed hash, not the number.

    Drivers hand values like this to ``escape_param`` as ordinary SQL
    parameters. What matters is the number the value carries, never how the
    class chose to spell itself.
    """

    def __str__(self):
        return "wks_%d" % int(self)

    def __repr__(self):
        return "HashedId(%d)" % int(self)

    def __format__(self, spec):
        return "wks_%d" % int(self)


class _StrOnlyId(int):
    """An int subclass that only overrides ``__str__``."""

    def __str__(self):
        return "str_only_%d" % int(self)


class _ReprOnlyId(int):
    """An int subclass that only overrides ``__repr__``."""

    def __repr__(self):
        return "repr_only_%d" % int(self)


class _FormatOnlyId(int):
    """An int subclass that only overrides ``__format__``."""

    def __format__(self, spec):
        return "format_only_%d" % int(self)


def test_integer_subclasses_really_do_spell_themselves_as_text():
    """The premise of the regression: their own text form is not a number."""

    assert str(HashedId(1)) == "wks_1"
    assert format(HashedId(1)) == "wks_1"
    assert str(_StrOnlyId(1)) == "str_only_1"
    assert str(_ReprOnlyId(1)) == "repr_only_1"
    assert format(_FormatOnlyId(1)) == "format_only_1"


INT_SUBCLASSES = [HashedId, _StrOnlyId, _ReprOnlyId, _FormatOnlyId]

INT_SUBCLASS_VALUES = [
    0,
    -1,
    -(2**63),
    1,
    2**63 - 1,
    2**64 - 1,  # UInt64 max
    2**80 + 7,  # beyond any 64-bit column, still a number
]


@pytest.mark.parametrize("subclass", INT_SUBCLASSES, ids=lambda c: c.__name__)
@pytest.mark.parametrize("value", INT_SUBCLASS_VALUES, ids=str)
def test_integer_subclass_is_inlined_as_the_number_it_carries(subclass, value):
    """Statement text carries the number, so the server sees valid SQL."""

    assert escape_param(subclass(value)) == str(int(value))


@pytest.mark.parametrize("subclass", INT_SUBCLASSES, ids=lambda c: c.__name__)
@pytest.mark.parametrize("value", INT_SUBCLASS_VALUES, ids=str)
def test_integer_subclass_reaches_the_server_as_the_number_it_carries(subclass, value):
    """Server-side parameters are quoted, and still say only the number."""

    assert escape_param(subclass(value), for_server=True) == "'%d'" % int(value)


def test_integer_subclass_inside_lists_keeps_the_number():
    assert escape_param([HashedId(1), HashedId(2)]) == "[1, 2]"


def test_integer_subclass_inside_tuples_keeps_the_number():
    assert escape_param((HashedId(3), HashedId(4))) == "(3, 4)"


def test_integer_subclass_survives_nested_containers():
    assert escape_param([[HashedId(5)], (HashedId(6),)]) == "[[5], (6)]"


def test_integer_subclass_mixes_with_plain_values_in_containers():
    assert escape_param([HashedId(7), 8, None]) == "[7, 8, NULL]"


def test_plain_ints_are_unchanged():
    assert escape_param(5) == "5"
    assert escape_param(-5) == "-5"
    assert escape_param(5, for_server=True) == "'5'"


def test_bool_keeps_its_spelling():
    """A bool is an int subclass, but ``True``/``False`` is what the server reads."""

    assert escape_param(True) == "True"
    assert escape_param(False) == "False"
    assert escape_param(True, for_server=True) == "'True'"
    assert escape_param(False, for_server=True) == "'False'"
    assert escape_param([True, False]) == "[True, False]"


def test_bool_next_to_an_integer_subclass_in_a_container():
    assert escape_param([True, HashedId(1)]) == "[True, 1]"


class Color(Enum):
    RED = "red"


class Level(IntEnum):
    LOW = 1


def test_enum_members_still_resolve_to_their_value():
    assert escape_param(Color.RED) == "'red'"
    assert escape_param(Color.RED, for_server=True) == "'red'"


def test_int_enum_members_still_resolve_to_their_value():
    """An IntEnum is an int subclass too; its value wins over its spelling."""

    assert escape_param(Level.LOW) == "1"
    assert escape_param(Level.LOW, for_server=True) == "'1'"


def test_enum_with_an_integer_subclass_value_resolves_to_the_number():
    class IdEnum(Enum):
        FIRST = HashedId(9)

    assert escape_param(IdEnum.FIRST) == "9"
    assert escape_param(IdEnum.FIRST, for_server=True) == "'9'"
