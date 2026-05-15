import re
from datetime import time, timedelta
from decimal import Decimal

from ..utils import compat
from .base import FormatColumn

TIME_RE = re.compile(
    r"^(?P<sign>-)?(?P<hours>\d+):(?P<minutes>\d{2}):(?P<seconds>\d{2})(?P<fraction>\.\d+)?$"
)


def _parse_time_string(value):
    match = TIME_RE.match(value)
    if not match:
        raise ValueError(f"Invalid ClickHouse time literal: {value!r}")

    sign = -1 if match.group("sign") else 1
    hours = int(match.group("hours"))
    minutes = int(match.group("minutes"))
    seconds = int(match.group("seconds"))
    fraction = Decimal(match.group("fraction") or "0")

    total = Decimal(hours * 3600 + minutes * 60 + seconds) + fraction
    return total * sign


def _time_to_decimal_seconds(value):
    if isinstance(value, time):
        return Decimal(value.hour * 3600 + value.minute * 60 + value.second) + (
            Decimal(value.microsecond) / Decimal(1_000_000)
        )
    if isinstance(value, timedelta):
        return Decimal(value.days * 86400 + value.seconds) + (
            Decimal(value.microseconds) / Decimal(1_000_000)
        )
    if isinstance(value, compat.string_types):
        return _parse_time_string(value)
    return Decimal(str(value))


def _timedelta_from_units(value, scale=1):
    sign = -1 if value < 0 else 1
    value = abs(value)
    seconds, fraction = divmod(value, scale)
    delta = timedelta(
        seconds=seconds,
        microseconds=(fraction * 1_000_000) // scale,
    )
    return -delta if sign < 0 else delta


class TimeColumn(FormatColumn):
    ch_type = "Time"
    py_types = (time, timedelta) + compat.integer_types + compat.string_types
    format = "i"

    def before_write_items(self, items, nulls_map=None):
        null_value = self.null_value
        for i, item in enumerate(items):
            if nulls_map and nulls_map[i]:
                items[i] = null_value
            elif not isinstance(item, compat.integer_types):
                items[i] = int(_time_to_decimal_seconds(item))

    def after_read_items(self, items, nulls_map=None):
        if nulls_map is None:
            return tuple(_timedelta_from_units(item) for item in items)
        return tuple(
            None if is_null else _timedelta_from_units(items[i])
            for i, is_null in enumerate(nulls_map)
        )


class Time64Column(TimeColumn):
    ch_type = "Time64"
    format = "q"

    def __init__(self, scale=3, **kwargs):
        self.scale = scale
        super().__init__(**kwargs)

    def before_write_items(self, items, nulls_map=None):
        scale = Decimal(10) ** self.scale
        null_value = self.null_value
        for i, item in enumerate(items):
            if nulls_map and nulls_map[i]:
                items[i] = null_value
            elif not isinstance(item, compat.integer_types):
                items[i] = int(_time_to_decimal_seconds(item) * scale)

    def after_read_items(self, items, nulls_map=None):
        scale = 10**self.scale
        if nulls_map is None:
            return tuple(_timedelta_from_units(item, scale) for item in items)
        return tuple(
            None if is_null else _timedelta_from_units(items[i], scale)
            for i, is_null in enumerate(nulls_map)
        )


def create_time_column(spec, column_options):
    if spec == "Time":
        return TimeColumn(**column_options)

    if spec == "Time64":
        return Time64Column(**column_options)

    return Time64Column(scale=int(spec[7:-1]), **column_options)
