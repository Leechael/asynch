import datetime
import time


class DBAPITypeObject:
    def __init__(self, *values: str):
        self.values = frozenset(values)

    def __eq__(self, other):
        if not isinstance(other, str):
            return False
        return _base_type(other) in self.values

    def __hash__(self):
        return hash(self.values)


def _base_type(type_code: str) -> str:
    type_code = type_code.strip()
    for wrapper in ("Nullable", "LowCardinality"):
        prefix = f"{wrapper}("
        if type_code.startswith(prefix) and type_code.endswith(")"):
            return _base_type(type_code[len(prefix) : -1])
    return type_code.split("(", 1)[0]


Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime


def DateFromTicks(ticks):
    return Date(*time.localtime(ticks)[:3])


def TimeFromTicks(ticks):
    return Time(*time.localtime(ticks)[3:6])


def TimestampFromTicks(ticks):
    return Timestamp(*time.localtime(ticks)[:6])


def Binary(value):
    if isinstance(value, str):
        return value.encode()
    return bytes(value)


STRING = DBAPITypeObject("String", "FixedString", "UUID", "IPv4", "IPv6", "Enum8", "Enum16")
BINARY = DBAPITypeObject("String", "FixedString")
NUMBER = DBAPITypeObject(
    "UInt8",
    "UInt16",
    "UInt32",
    "UInt64",
    "UInt128",
    "UInt256",
    "Int8",
    "Int16",
    "Int32",
    "Int64",
    "Int128",
    "Int256",
    "Float32",
    "Float64",
    "BFloat16",
    "Decimal",
    "Decimal32",
    "Decimal64",
    "Decimal128",
    "Decimal256",
    "Bool",
)
DATETIME = DBAPITypeObject("Date", "Date32", "DateTime", "DateTime64", "Time", "Time64")
ROWID = DBAPITypeObject()
