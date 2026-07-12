# Translated from clickhouse_driver/bufferedreader.pyx at 49afa09.
"""Pure-Python mechanical translation of clickhouse-driver buffered readers."""


class BufferedReader:
    def __init__(self, bufsize):
        self.buffer = bytearray(bufsize)
        # pyx:34/71 obtains a fresh char pointer over this bytearray. A
        # memoryview is Python's non-copying equivalent of that pointer.
        self.buffer_view = memoryview(self.buffer)
        self.position = 0
        self.current_buffer_size = 0
        super().__init__()

    def read_into_buffer(self):
        raise NotImplementedError

    def read(self, unread):
        # When the buffer is large enough bytes read are almost always hit it.
        # pyx:28-47 uses a char pointer and PyBytes_FromStringAndSize; slicing
        # the bytearray is its direct Python object equivalent.
        next_position = unread + self.position
        if next_position < self.current_buffer_size:
            position = self.position
            self.position = next_position
            return self.buffer_view[position : self.position].tobytes()

        result = bytes()
        while unread > 0:
            if self.position == self.current_buffer_size:
                self.read_into_buffer()
                self.position = 0

            read_bytes = min(unread, self.current_buffer_size - self.position)
            result += self.buffer_view[self.position : self.position + read_bytes].tobytes()
            self.position += read_bytes
            unread -= read_bytes

        return result

    def read_one(self):
        if self.position == self.current_buffer_size:
            self.read_into_buffer()
            self.position = 0

        result = self.buffer[self.position]
        self.position += 1
        return result

    def read_strings(self, n_items, encoding=None):
        """Read strings inline, preserving the upstream batch-oriented algorithm."""
        items = []

        for _ in range(n_items):
            shift = size = 0

            while True:
                if self.position == self.current_buffer_size:
                    self.read_into_buffer()
                    self.position = 0

                item = self.buffer[self.position]
                self.position += 1

                size |= (item & 0x7F) << shift
                if item < 0x80:
                    break

                shift += 7

            right = self.position + size
            if right > self.current_buffer_size:
                # pyx:121-169 maintains a manually allocated C string for an
                # optional decode. Python bytes is the equivalent owned buffer.
                result = self.buffer_view[self.position : self.current_buffer_size].tobytes()
                bytes_read = self.current_buffer_size - self.position
                while bytes_read != size:
                    self.position = size - bytes_read
                    self.read_into_buffer()
                    self.position = min(self.position, self.current_buffer_size)
                    result += self.buffer_view[: self.position].tobytes()
                    bytes_read += self.position
            else:
                result = self.buffer_view[self.position : right].tobytes()
                self.position = right

            if encoding:
                try:
                    result = result.decode(encoding)
                except UnicodeDecodeError:
                    pass
            items.append(result)

        return tuple(items)

    def read_fixed_strings_as_bytes(self, n_items, length):
        data = self.read(length * n_items)
        return tuple(data[index * length : (index + 1) * length] for index in range(n_items))

    def read_fixed_strings(self, n_items, length, encoding=None):
        if encoding is None:
            return self.read_fixed_strings_as_bytes(n_items, length)

        data = self.read(length * n_items)
        items = []
        for index in range(n_items):
            item = data[index * length : (index + 1) * length]
            # pyx:209-220 scans trailing NULs in a C buffer; rstrip with this
            # exact byte is the direct Python equivalent.
            item = item.rstrip(b"\x00")
            try:
                item = item.decode(encoding)
            except UnicodeDecodeError:
                item = data[index * length : (index + 1) * length]
            items.append(item)

        return tuple(items)


class BufferedSocketReader(BufferedReader):
    def __init__(self, sock, bufsize):
        self.sock = sock
        super().__init__(bufsize)

    def read_into_buffer(self):
        self.current_buffer_size = self.sock.recv_into(self.buffer)
        if self.current_buffer_size == 0:
            raise EOFError("Unexpected EOF while reading bytes")


class CompressedBufferedReader(BufferedReader):
    def __init__(self, read_block, bufsize):
        self.read_block = read_block
        super().__init__(bufsize)

    def read_into_buffer(self):
        self.buffer = bytearray(self.read_block())
        self.buffer_view = memoryview(self.buffer)
        self.current_buffer_size = len(self.buffer)
        if self.current_buffer_size == 0:
            raise EOFError("Unexpected EOF while reading bytes")
