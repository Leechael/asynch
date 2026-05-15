from math import ceil
from struct import pack, unpack

from ..utils import compat
from .base import Column
from .util import get_inner_columns, get_inner_spec


class QBitColumn(Column):
    py_types = (list, tuple)
    null_value = []

    def __init__(self, element_spec, dimension, **kwargs):
        self.element_spec = element_spec
        self.dimension = dimension
        self.element_bits = _element_bits(element_spec)
        self.bytes_per_plane = ceil(dimension / 8)
        self.total_bits = self.bytes_per_plane * 8
        super().__init__(**kwargs)

    async def write_items(self, items):
        planes_by_bit = [[] for _ in range(self.element_bits)]
        for vector in items:
            planes = self._vector_to_planes(vector)
            for bit, plane in enumerate(planes):
                planes_by_bit[bit].append(plane)

        for planes in planes_by_bit:
            await self.writer.write_fixed_strings(planes, self.bytes_per_plane)

    async def read_items(self, n_items):
        planes_by_bit = []
        for _ in range(self.element_bits):
            planes = []
            for _ in range(n_items):
                plane = await self.reader.read_fixed_str(
                    self.bytes_per_plane,
                    as_bytes=True,
                )
                planes.append(bytes(plane))
            planes_by_bit.append(planes)

        result = []
        for row in range(n_items):
            row_planes = [planes_by_bit[bit][row] for bit in range(self.element_bits)]
            result.append(self._planes_to_vector(row_planes))
        return tuple(result)

    def _vector_to_planes(self, vector):
        if len(vector) != self.dimension:
            raise ValueError(
                f"QBit({self.element_spec}, {self.dimension}) expects vectors of "
                f"length {self.dimension}, got {len(vector)}"
            )

        planes = [bytearray(self.bytes_per_plane) for _ in range(self.element_bits)]
        for element_index, value in enumerate(vector):
            word = _pack_float(self.element_spec, value)
            bit_index = (self.total_bits - 1) - (element_index ^ 7)
            byte_position = bit_index // 8
            bit_mask = 1 << (bit_index % 8)

            for bit in range(self.element_bits):
                source_bit = self.element_bits - 1 - bit
                if word & (1 << source_bit):
                    planes[bit][byte_position] |= bit_mask

        return [bytes(plane) for plane in planes]

    def _planes_to_vector(self, planes):
        values = []
        for element_index in range(self.dimension):
            word = 0
            bit_index = (self.total_bits - 1) - (element_index ^ 7)
            byte_position = bit_index // 8
            bit_mask = 1 << (bit_index % 8)

            for bit, plane in enumerate(planes):
                if plane[byte_position] & bit_mask:
                    source_bit = self.element_bits - 1 - bit
                    word |= 1 << source_bit

            values.append(_unpack_float(self.element_spec, word))

        return values


def _element_bits(element_spec):
    if element_spec == "BFloat16":
        return 16
    if element_spec == "Float32":
        return 32
    if element_spec == "Float64":
        return 64
    raise ValueError(f"Unsupported QBit element type {element_spec}")


def _pack_float(element_spec, value):
    if not isinstance(value, (float,) + compat.integer_types):
        raise TypeError(f"QBit value must be numeric, got {type(value)}")

    if element_spec == "BFloat16":
        return unpack("<I", pack("<f", float(value)))[0] >> 16
    if element_spec == "Float32":
        return unpack("<I", pack("<f", float(value)))[0]
    return unpack("<Q", pack("<d", float(value)))[0]


def _unpack_float(element_spec, word):
    if element_spec == "BFloat16":
        return unpack("<f", pack("<I", word << 16))[0]
    if element_spec == "Float32":
        return unpack("<f", pack("<I", word))[0]
    return unpack("<d", pack("<Q", word))[0]


def create_qbit_column(spec, column_options):
    inner = get_inner_spec("QBit", spec)
    element_spec, dimension = [item.strip() for item in get_inner_columns(inner)]
    return QBitColumn(element_spec, int(dimension), **column_options)
