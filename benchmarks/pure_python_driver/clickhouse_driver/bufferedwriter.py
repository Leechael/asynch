# Translated from clickhouse_driver/bufferedwriter.pyx at 49afa09.
"""Pure-Python mechanical translation of clickhouse-driver buffered writers."""

from . import errors
from .varint import make_varint


class BufferedWriter:
    def __init__(self, bufsize):
        # pyx:14-25 allocates/frees a char pointer; bytearray owns equivalent
        # mutable storage and is released by normal Python object lifetime.
        self.buffer = bytearray(bufsize)
        self.position = 0
        self.buffer_size = bufsize
        super().__init__()

    def write_into_stream(self):
        raise NotImplementedError

    def write(self, data):
        written = 0
        data_len = len(data)

        while written < data_len:
            size = min(data_len - written, self.buffer_size - self.position)
            # pyx:37 copies between C pointers; slice assignment is the direct
            # Python equivalent without changing the buffer algorithm.
            self.buffer[self.position : self.position + size] = data[written : written + size]

            if self.position == self.buffer_size:
                self.write_into_stream()

            self.position += size
            written += size

    def flush(self):
        self.write_into_stream()

    def write_strings(self, items, encoding=None):
        do_encode = encoding is not None

        for value in items:
            if not isinstance(value, bytes):
                if do_encode:
                    value = value.encode(encoding)
                else:
                    raise ValueError("bytes object expected")

            self.write(make_varint(len(value)))
            self.write(value)

    def write_fixed_strings_as_bytes(self, items, length):
        # pyx:63-84 uses zeroed PyMem storage; bytearray has the same initial
        # zero contents and is converted to bytes at the existing write edge.
        items_buffer = bytearray(length * len(items))
        buffer_position = 0

        for value in items:
            value_len = len(value)
            if length < value_len:
                raise errors.TooLargeStringSize()

            items_buffer[buffer_position : buffer_position + value_len] = value
            buffer_position += length

        self.write(bytes(items_buffer))

    def write_fixed_strings(self, items, length, encoding=None):
        if encoding is None:
            self.write_fixed_strings_as_bytes(items, length)
            return

        # pyx:91-116 has the same zeroed fixed-width staging buffer.
        items_buffer = bytearray(length * len(items))
        buffer_position = 0

        for value in items:
            if not isinstance(value, bytes):
                value = value.encode(encoding)

            value_len = len(value)
            if length < value_len:
                raise errors.TooLargeStringSize()

            items_buffer[buffer_position : buffer_position + value_len] = value
            buffer_position += length

        self.write(bytes(items_buffer))


class BufferedSocketWriter(BufferedWriter):
    def __init__(self, sock, bufsize):
        self.sock = sock
        super().__init__(bufsize)

    def write_into_stream(self):
        self.sock.sendall(bytes(self.buffer[: self.position]))
        self.position = 0


class CompressedBufferedWriter(BufferedWriter):
    def __init__(self, compressor, bufsize):
        self.compressor = compressor
        super().__init__(bufsize)

    def write_into_stream(self):
        self.compressor.write(bytes(self.buffer[: self.position]))
        self.position = 0

    def flush(self):
        self.write_into_stream()
