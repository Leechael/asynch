from struct import pack, unpack

from ..utils import compat
from .base import Column


class BFloat16Column(Column):
    ch_type = "BFloat16"
    py_types = (float,) + compat.integer_types

    async def write_items(self, items):
        data = bytearray()
        for item in items:
            float32_bits = unpack("<I", pack("<f", float(item)))[0]
            data.extend(pack("<H", float32_bits >> 16))
        await self.writer.write_bytes(data)

    async def read_items(self, n_items):
        data = await self.reader.read_bytes(2 * n_items)
        return tuple(
            unpack("<f", pack("<I", unpack("<H", data[i : i + 2])[0] << 16))[0]
            for i in range(0, len(data), 2)
        )
