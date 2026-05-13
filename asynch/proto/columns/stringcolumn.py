from asynch.proto.columns.base import Column
from asynch.proto.streams.buffered import BufferedReader, BufferedWriter
from asynch.proto.utils import compat


class String(Column):
    ch_type = "String"
    py_types = compat.string_types
    null_value = ""
    read_as_bytes = False

    def __init__(self, *args, **kwargs):
        context = kwargs.get("context")
        self.encoding = "utf-8"
        if context is not None:
            self.encoding = context.client_settings["strings_encoding"]
        super().__init__(*args, **kwargs)

    def check_item_for_write(self, item):
        if not isinstance(item, (str, bytes)):
            item.encode()

    async def write_items(self, items):
        prepared = []
        for item in items:
            self.check_item_for_write(item)
            if isinstance(item, str):
                item = item.encode(self.encoding)
            prepared.append(item)
        await self.writer.write_strings(prepared)

    async def read_items(self, n_items):
        ret = []
        for _ in range(n_items):
            packet = await self.reader.read_str(as_bytes=True)
            if self.read_as_bytes:
                ret.append(packet)
                continue
            try:
                ret.append(packet.decode(self.encoding))
            except UnicodeDecodeError:
                ret.append(packet)
        return tuple(ret)


class ByteString(String):
    py_types = (bytes,)
    null_value = b""
    read_as_bytes = True

    def check_item_for_write(self, item):
        if not isinstance(item, bytes):
            raise ValueError("bytes object expected")


class FixedString(String):
    ch_type = "FixedString"
    read_as_bytes = False

    def __init__(self, reader: BufferedReader, writer: BufferedWriter, length: int, **kwargs):
        self.length = length
        super().__init__(reader, writer, **kwargs)

    async def write_items(self, items):
        prepared = []
        for item in items:
            self.check_item_for_write(item)
            if isinstance(item, str):
                item = item.encode(self.encoding)
            prepared.append(item)
        await self.writer.write_fixed_strings(prepared, self.length)

    async def read_items(self, n_items):
        ret = []
        for _ in range(n_items):
            packet = await self.reader.read_fixed_str(self.length, as_bytes=True)
            if self.read_as_bytes:
                ret.append(packet)
                continue
            packet = packet.rstrip(b"\x00")
            try:
                ret.append(packet.decode(self.encoding))
            except UnicodeDecodeError:
                ret.append(packet)
        return tuple(ret)


class ByteFixedString(FixedString):
    py_types = (bytearray, bytes)
    null_value = b""
    read_as_bytes = True

    def check_item_for_write(self, item):
        if not isinstance(item, bytes):
            raise ValueError("bytes object expected")


def create_string_column(spec, column_options):
    client_settings = column_options["context"].client_settings
    strings_as_bytes = client_settings["strings_as_bytes"]
    if spec == "String":
        cls = ByteString if strings_as_bytes else String
        return cls(**column_options)
    else:
        length = int(spec[12:-1])
        cls = ByteFixedString if strings_as_bytes else FixedString
        return cls(length=length, **column_options)
