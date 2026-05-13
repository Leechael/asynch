class ClientPacket:
    """
    Packet types that client transmits
    """

    # Name, version, revision, default DB
    HELLO = 0

    # Query id, query settings, stage up to which the query must be executed,
    # whether the compression must be used, query text
    # (without data for INSERTs).
    QUERY = 1

    # A block of data (compressed or not).
    DATA = 2

    # Cancel the query execution.
    CANCEL = 3

    # Check that connection to the server is alive.
    PING = 4

    # Check status of tables on the server.
    TABLES_STATUS_REQUEST = 5

    # Keep the connection alive.
    KEEP_ALIVE = 6

    # A block of data (compressed or not).
    SCALAR = 7

    # List of unique parts ids to exclude from query processing.
    IGNORED_PART_UUIDS = 8

    # A filename to read from s3 (used in s3Cluster).
    READ_TASK_RESPONSE = 9

    # Coordinator's decision with a modified set of mark ranges allowed to read.
    MERGE_TREE_READ_TASK_RESPONSE = 10

    # Request SSH signature challenge.
    SSH_CHALLENGE_REQUEST = 11

    # Reply to SSH signature challenge.
    SSH_CHALLENGE_RESPONSE = 12

    # Query plan.
    QUERY_PLAN = 13

    _types_str = [
        "Hello",
        "Query",
        "Data",
        "Cancel",
        "Ping",
        "TablesStatusRequest",
        "KeepAlive",
        "Scalar",
        "IgnoredPartUUIDs",
        "ReadTaskResponse",
        "MergeTreeReadTaskResponse",
        "SSHChallengeRequest",
        "SSHChallengeResponse",
        "QueryPlan",
    ]

    @classmethod
    def to_str(cls, packet):
        return "Unknown packet" if packet > 13 else cls._types_str[packet]


class ServerPacket:
    """
    Packet types that server transmits.
    """

    # Name, version, revision.
    HELLO = 0

    # A block of data (compressed or not).
    DATA = 1

    # The exception during query execution.
    EXCEPTION = 2

    # Query execution progress: rows read, bytes read.
    PROGRESS = 3

    # Ping response
    PONG = 4

    # All packets were transmitted
    END_OF_STREAM = 5

    # Packet with profiling info.
    PROFILE_INFO = 6

    # A block with totals (compressed or not).
    TOTALS = 7

    # A block with minimums and maximums (compressed or not).
    EXTREMES = 8

    # A response to TablesStatus request.
    TABLES_STATUS_RESPONSE = 9

    # System logs of the query execution
    LOG = 10

    # Columns' description for default values calculation
    TABLE_COLUMNS = 11
    # List of unique parts ids.
    PART_UUIDS = 12

    # String (UUID) describes a request for which next task is needed
    READ_TASK_REQUEST = 13

    # Packet with profile events from server.
    PROFILE_EVENTS = 14

    MERGE_TREE_ALL_RANGES_ANNOUNCEMENT = 15

    # Request from a MergeTree replica to a coordinator
    MERGE_TREE_READ_TASK_REQUEST = 16

    # Receive server's (session-wide) default timezone
    TIMEZONE_UPDATE = 17

    # Return challenge for SSH signature signing
    SSH_CHALLENGE = 18

    _types_str = [
        "Hello",
        "Data",
        "Exception",
        "Progress",
        "Pong",
        "EndOfStream",
        "ProfileInfo",
        "Totals",
        "Extremes",
        "TablesStatusResponse",
        "Log",
        "TableColumns",
        "PartUUIDs",
        "ReadTaskRequest",
        "ProfileEvents",
        "MergeTreeAllRangesAnnouncement",
        "MergeTreeReadTaskRequest",
        "TimezoneUpdate",
        "SSHChallenge",
    ]

    @classmethod
    def to_str(cls, packet):
        if packet is None:
            return "Connection closed by remote"

        return "Unknown packet" if packet > 18 else cls._types_str[packet]

    @classmethod
    def strings_in_message(cls, packet):
        if packet == cls.TABLE_COLUMNS:
            return 2
        return 0


class Compression:
    DISABLED = 0
    ENABLED = 1


class CompressionMethod:
    LZ4 = 1
    LZ4HC = 2
    ZSTD = 3


class CompressionMethodByte:
    LZ4 = 0x82
    ZSTD = 0x90
