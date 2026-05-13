from enum import Enum


class StrEnum(str, Enum):
    def __str__(self) -> str:
        return self.value


class CompressionAlgorithm(StrEnum):
    lz4 = "lz4"
    lz4hc = "lz4hc"
    zstd = "zstd"


class ConnectionStatus(StrEnum):
    created = "created"
    opened = "opened"
    closed = "closed"


class CursorStatus(StrEnum):
    ready = "ready"
    running = "running"
    finished = "finished"
    closed = "closed"


class PoolStatus(StrEnum):
    created = "created"
    opened = "opened"
    closed = "closed"


class ClickhouseScheme(StrEnum):
    clickhouse = "clickhouse"
    clickhouses = "clickhouses"
