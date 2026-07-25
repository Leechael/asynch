import json
from datetime import date, datetime, time
from datetime import timezone as datetime_timezone
from enum import Enum
from typing import Any, Mapping
from uuid import UUID

from pytz import timezone

from .compat import string_types, text_type

_EPOCH = datetime(1970, 1, 1, tzinfo=datetime_timezone.utc)


def is_aware(item: datetime) -> bool:
    """Whether the value carries a usable offset.

    A tzinfo alone is not enough: one whose ``utcoffset`` returns None leaves
    the value naive in every way that matters, and arithmetic against an aware
    epoch would raise rather than yield an instant.
    """

    return item.tzinfo is not None and item.utcoffset() is not None


def epoch_microseconds(item: datetime) -> int:
    """Microseconds since the epoch, by integer arithmetic on an aware value."""

    delta = item - _EPOCH
    return (delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds


escape_chars_map = {
    "\b": "\\b",
    "\f": "\\f",
    "\r": "\\r",
    "\n": "\\n",
    "\t": "\\t",
    "\0": "\\0",
    "\a": "\\a",
    "\v": "\\v",
    "\\": "\\\\",
    "'": "\\'",
}


def escape_param(
    item: Any,
    context=None,
    for_server: bool = False,
    typed_datetime: bool = False,
) -> str:
    """Render a Python value as ClickHouse SQL text.

    ``typed_datetime`` sends an aware datetime as the instant it denotes,
    microseconds since the epoch, rather than as text the server has to
    reinterpret. This holds whatever the value's precision: a whole second
    needs no fraction preserved but still has an instant to preserve, which a
    wall time carrying no zone of its own would lose against a column that has
    one. A naive value is left as a bare string, since only the server can
    resolve it against the target column, and so are server-side parameters,
    which travel in a typed slot rather than in the statement text.
    """

    typed_datetime = typed_datetime and not for_server

    if item is None:
        escaped = "NULL"

    elif isinstance(item, datetime):
        # An aware value denotes an instant, so it is sent as one: microseconds
        # since the epoch, which no zone or daylight saving transition can blur.
        # A wall time would not manage that. It cannot survive a fall-back hour,
        # where two instants share the same local reading, and it carries no
        # zone of its own, so a column with one reads it as a local time and
        # lands on a different instant. Neither depends on a sub-second part
        # being present, so every aware value takes this route.
        #
        # A naive value denotes a wall time whose zone belongs to the target
        # column, which only the server can resolve, so it stays a bare string
        # for the server to parse against that column.
        if typed_datetime and is_aware(item):
            escaped = "fromUnixTimestamp64Micro(%d)" % epoch_microseconds(item)
        else:
            server_info = getattr(context, "server_info", None)
            if item.tzinfo is not None and server_info is not None:
                server_tz = timezone(server_info.get_timezone())
                item = item.astimezone(server_tz)
            fmt = "%Y-%m-%d %H:%M:%S"
            if item.microsecond:
                fmt += ".%f"
            escaped = "'%s'" % item.strftime(fmt)

    elif isinstance(item, date):
        escaped = "'%s'" % item.strftime("%Y-%m-%d")

    elif isinstance(item, time):
        fmt = "%H:%M:%S"
        if item.microsecond:
            fmt += ".%f"
        escaped = "'%s'" % item.strftime(fmt)

    elif isinstance(item, string_types):
        if for_server:
            item = "".join(escape_chars_map.get(c, c) for c in item)
        escaped = "'%s'" % "".join(escape_chars_map.get(c, c) for c in item)

    elif isinstance(item, list):
        escaped = "[%s]" % ", ".join(
            text_type(
                escape_param(
                    x, context=context, for_server=for_server, typed_datetime=typed_datetime
                )
            )
            for x in item
        )

    elif isinstance(item, dict):
        escaped = escape_param(
            json.dumps(item, ensure_ascii=False, separators=(",", ":")),
            context=context,
            for_server=for_server,
        )

    elif isinstance(item, tuple):
        escaped = "(%s)" % ", ".join(
            text_type(
                escape_param(
                    x, context=context, for_server=for_server, typed_datetime=typed_datetime
                )
            )
            for x in item
        )

    elif isinstance(item, Enum):
        escaped = escape_param(
            item.value, context=context, for_server=for_server, typed_datetime=typed_datetime
        )

    elif isinstance(item, UUID):
        escaped = "'%s'" % str(item)

    else:
        escaped = str(item)

    if for_server and not escaped.startswith("'"):
        escaped = "'%s'" % escaped
    return escaped


def escape_params(
    params: Mapping[str, Any],
    context=None,
    for_server: bool = False,
    typed_datetime: bool = False,
) -> dict[str, str]:
    return {
        key: escape_param(
            value, context=context, for_server=for_server, typed_datetime=typed_datetime
        )
        for key, value in params.items()
    }
