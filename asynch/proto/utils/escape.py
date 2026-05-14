import json
from datetime import date, datetime, time
from enum import Enum
from typing import Any, Mapping
from uuid import UUID

from pytz import timezone

from .compat import string_types, text_type

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


def escape_param(item: Any, context=None, for_server: bool = False) -> str:
    if item is None:
        escaped = "NULL"

    elif isinstance(item, datetime):
        if item.tzinfo is not None and context is not None:
            server_tz = timezone(context.server_info.get_timezone())
            item = item.astimezone(server_tz)
        escaped = "'%s'" % item.strftime("%Y-%m-%d %H:%M:%S")

    elif isinstance(item, date):
        escaped = "'%s'" % item.strftime("%Y-%m-%d")

    elif isinstance(item, time):
        escaped = "'%s'" % item.strftime("%H:%M:%S")

    elif isinstance(item, string_types):
        if for_server:
            item = "".join(escape_chars_map.get(c, c) for c in item)
        escaped = "'%s'" % "".join(escape_chars_map.get(c, c) for c in item)

    elif isinstance(item, list):
        escaped = "[%s]" % ", ".join(
            text_type(escape_param(x, context=context, for_server=for_server)) for x in item
        )

    elif isinstance(item, dict):
        escaped = escape_param(
            json.dumps(item, ensure_ascii=False, separators=(",", ":")),
            context=context,
            for_server=for_server,
        )

    elif isinstance(item, tuple):
        escaped = "(%s)" % ", ".join(
            text_type(escape_param(x, context=context, for_server=for_server)) for x in item
        )

    elif isinstance(item, Enum):
        escaped = escape_param(item.value, context=context, for_server=for_server)

    elif isinstance(item, UUID):
        escaped = "'%s'" % str(item)

    else:
        escaped = str(item)

    if for_server and not escaped.startswith("'"):
        escaped = "'%s'" % escaped
    return escaped


def escape_params(
    params: Mapping[str, Any], context=None, for_server: bool = False
) -> dict[str, str]:
    return {
        key: escape_param(value, context=context, for_server=for_server)
        for key, value in params.items()
    }
