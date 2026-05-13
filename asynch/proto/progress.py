from asynch.proto import constants
from asynch.proto.streams.buffered import BufferedReader


class Progress:
    def __init__(self, reader: BufferedReader):
        self.rows = 0
        self.bytes = 0
        self.total_rows = 0
        self.total_bytes = 0
        self.written_rows = 0
        self.written_bytes = 0
        self.elapsed_ns = 0
        self.reader = reader

    async def read(
        self,
        server_revision,
    ):
        self.rows = await self.reader.read_varint()
        self.bytes = await self.reader.read_varint()

        revision = server_revision
        if revision >= constants.DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS:
            self.total_rows = await self.reader.read_varint()

        if revision >= constants.DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS:
            self.total_bytes = await self.reader.read_varint()

        if revision >= constants.DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO:
            self.written_rows = await self.reader.read_varint()
            self.written_bytes = await self.reader.read_varint()

        if revision >= constants.DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS:
            self.elapsed_ns = await self.reader.read_varint()

    def increment(self, another_progress):
        self.rows += another_progress.rows
        self.bytes += another_progress.bytes
        self.total_rows += another_progress.total_rows
        self.total_bytes += another_progress.total_bytes
        self.written_rows += another_progress.written_rows
        self.written_bytes += another_progress.written_bytes
        self.elapsed_ns += another_progress.elapsed_ns
