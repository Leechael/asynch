from struct import Struct

from ..utils import compat
from .base import Column
from .util import get_inner_columns, get_inner_spec


class AggregateFunctionColumn(Column):
    py_types = (object,)
    null_value = None

    def __init__(self, function_name, argument_specs, **kwargs):
        self.function_name = function_name
        self.argument_specs = argument_specs
        super().__init__(**kwargs)

        if function_name not in {"count", "sum", "avg"}:
            raise NotImplementedError(
                "AggregateFunction states are function-specific; only count, sum, "
                "and avg have built-in Native decoding"
            )

    async def write_items(self, items):
        if self.function_name == "count":
            for item in items:
                await self.writer.write_varint(item)
            return

        if self.function_name == "sum":
            await self.writer.write_bytes(
                b"".join(_pack_sum_state(self.argument_specs[0], item) for item in items)
            )
            return

        data = bytearray()
        for item in items:
            value, count = item
            data.extend(_pack_sum_state(self.argument_specs[0], value))
            data.extend(_encode_varint(count))
        await self.writer.write_bytes(bytes(data))

    async def read_items(self, n_items):
        if self.function_name == "count":
            return tuple([await self.reader.read_varint() for _ in range(n_items)])

        if self.function_name == "sum":
            return tuple(
                [
                    _unpack_sum_state(
                        self.argument_specs[0],
                        await self.reader.read_bytes(_sum_state_size(self.argument_specs[0])),
                    )
                    for _ in range(n_items)
                ]
            )

        result = []
        for _ in range(n_items):
            value = _unpack_sum_state(
                self.argument_specs[0],
                await self.reader.read_bytes(_sum_state_size(self.argument_specs[0])),
            )
            count = await self.reader.read_varint()
            result.append((value, count))
        return tuple(result)


def _encode_varint(value):
    result = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            result.append(byte | 0x80)
        else:
            result.append(byte)
            return bytes(result)


def _sum_state_size(spec):
    if spec.startswith(("Float", "BFloat")):
        return 8
    if spec.startswith("Decimal256"):
        return 32
    if spec.startswith("Decimal"):
        return 16
    return 8


def _pack_sum_state(spec, value):
    if spec.startswith(("Float", "BFloat")):
        return Struct("<d").pack(float(value))
    if spec.startswith("UInt"):
        return int(value).to_bytes(8, "little", signed=False)
    if spec.startswith("Int"):
        return int(value).to_bytes(8, "little", signed=True)
    if spec.startswith("Decimal"):
        signed_value = int(value)
        return signed_value.to_bytes(_sum_state_size(spec), "little", signed=True)
    if isinstance(value, compat.integer_types):
        return int(value).to_bytes(8, "little", signed=True)
    return Struct("<d").pack(float(value))


def _unpack_sum_state(spec, data):
    if spec.startswith(("Float", "BFloat")):
        return Struct("<d").unpack(data)[0]
    if spec.startswith("UInt"):
        return int.from_bytes(data, "little", signed=False)
    return int.from_bytes(data, "little", signed=True)


def create_aggregate_function_column(spec, column_options):
    inner = get_inner_spec("AggregateFunction", spec)
    parts = [part.strip() for part in get_inner_columns(inner)]
    function_name = parts[0]
    argument_specs = parts[1:]
    return AggregateFunctionColumn(function_name, argument_specs, **column_options)
