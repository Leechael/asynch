# ClickHouse Native Protocol Revision Ledger

This ledger tracks the native protocol revisions after the current `asynch`
baseline, `54468`, through the local ClickHouse reference checkout's
`DBMS_TCP_PROTOCOL_VERSION = 54483`.

Primary sources:

- `repos/ClickHouse/src/Core/ProtocolDefines.h`
- `repos/ClickHouse/src/Core/Protocol.h`
- `repos/ClickHouse/src/Client/Connection.cpp`
- `repos/ClickHouse/src/Server/TCPHandler.cpp`
- `repos/ClickHouse/src/Interpreters/ClientInfo.cpp`
- `repos/ClickHouse/src/Core/BlockInfo.h`
- `repos/ClickHouse/src/Core/BlockInfo.cpp`

| Revision | Upstream symbol | Wire impact | Direction | `asynch` status | Required coverage |
| --- | --- | --- | --- | --- | --- |
| 54469 | `DBMS_MIN_REVISION_WITH_ROWS_BEFORE_AGGREGATION` | No native TCP read/write branch found in the current reference checkout. The symbol gates server-side statistics support only. | none | Accounted, no client-visible change | Constant parity test and ledger entry |
| 54470 | `DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS` | Addendum writes client send/receive chunked capability strings. Server hello includes server send/receive chunked capability strings. Default negotiation can remain `notchunked`. | both | Implemented | Boundary tests for addendum write and hello read alignment |
| 54471 | `DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL` | Server hello includes parallel replicas protocol version. Client addendum includes supported parallel replicas protocol version. | both | Implemented | Boundary tests for hello/addendum below and at the gate |
| 54472 | `DBMS_MIN_PROTOCOL_VERSION_WITH_INTERSERVER_EXTERNALLY_GRANTED_ROLES` | Query packet includes serialized externally granted roles before the interserver secret hash. Normal client queries send an empty roles string. | client to server | Missing | Boundary tests for query packet layout below and at the gate |
| 54473 | `DBMS_MIN_REVISION_WITH_V2_DYNAMIC_AND_JSON_SERIALIZATION` | No native TCP read/write branch found in the current reference checkout outside the protocol constant. | none | Accounted, no client-visible change | Constant parity test and ledger entry |
| 54474 | `DBMS_MIN_REVISION_WITH_SERVER_SETTINGS` | Server hello includes settings serialized as strings with flags. Client must consume them to keep the stream aligned. | server to client | Missing | Boundary tests for hello read alignment with empty and non-empty settings |
| 54475 | `DBMS_MIN_REVISION_WITH_QUERY_AND_LINE_NUMBERS` | `ClientInfo` appends `script_query_number` and `script_line_number`. | client to server | Missing | Boundary tests for `ClientInfo.write` below and at the gate |
| 54476 | `DBMS_MIN_REVISON_WITH_JWT_IN_INTERSERVER` | `ClientInfo` appends an optional JWT marker/string for interserver queries. Normal client queries send marker `0`. | client to server | Missing | Boundary tests for `ClientInfo.write` below and at the gate |
| 54477 | `DBMS_MIN_REVISION_WITH_QUERY_PLAN_SERIALIZATION` | Server hello appends query plan serialization version. ClickHouse also adds client packet `QueryPlan = 13`. | server to client, client enum | Missing | Boundary tests for hello read alignment and packet name coverage |
| 54478 | `DBMS_MIN_REVISON_WITH_PARALLEL_BLOCK_MARSHALLING` | Server-side parallel block marshalling choice; no direct client packet field found in the current reference checkout. | none | Accounted, no client-visible change | Constant parity test and ledger entry |
| 54479 | `DBMS_MIN_REVISION_WITH_VERSIONED_CLUSTER_FUNCTION_PROTOCOL` | Server hello appends cluster function protocol version. | server to client | Missing | Boundary tests for hello read alignment below and at the gate |
| 54480 | `DBMS_MIN_REVISION_WITH_OUT_OF_ORDER_BUCKETS_IN_AGGREGATION` | `BlockInfo` field `3` serializes a vector of signed bucket ids. | both | Missing | Boundary tests for block info read/write below and at the gate |
| 54481 | `DBMS_MIN_REVISION_WITH_COMPRESSED_LOGS_PROFILE_EVENTS_COLUMNS` | Log/profile-events/table-columns blocks may be read through the compressed block stream. No payload shape change for ordinary data blocks. | server to client | Needs focused support or explicit compatibility test | Unit coverage for packet routing when compression is disabled; integration smoke with logs/profile events |
| 54482 | `DBMS_MIN_REVISION_WITH_REPLICATED_SERIALIZATION` | No native TCP read/write branch found in the current reference checkout outside the protocol constant. | none | Accounted, no client-visible change | Constant parity test and ledger entry |
| 54483 | `DBMS_MIN_REVISION_WITH_NULLABLE_SPARSE_SERIALIZATION` | No native TCP read/write branch found in the current reference checkout outside the protocol constant. Latest target revision. | none | Accounted, no client-visible change after preceding gates | Constant parity test, raised `CLIENT_REVISION`, latest-server smoke |
