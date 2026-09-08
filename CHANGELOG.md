Changelog
=========

All notable changes to the `0.9.x` line of `lisachenko/kafka-client` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this line
follows the Apache Kafka release it speaks rather than semantic versioning of its own: every
`0.9.x` release implements the **Kafka 0.9.0.1 wire protocol** and nothing above it. The line
below it is `0.8.x` (Kafka 0.8.2.2), the ones above are `0.10.x` and `main`, and every line is
merged upwards into the next one.

Unreleased — the 0.9.x line
---------------------------

Everything a Kafka 0.9.0.1 broker speaks, built on top of the `0.8.x` line it was merged from.
The entries below land ticket by ticket; the protocol surface is the first of them.

### Added

- **Api keys 11-16** — `JOIN_GROUP`, `HEARTBEAT`, `LEAVE_GROUP`, `SYNC_GROUP`, `DESCRIBE_GROUPS`
  and `LIST_GROUPS`, the group membership and group introspection apis that Kafka 0.9 added
  (`kafka/api/RequestKeys.scala` @ 0.9.0.1). `Protocol\ApiKeys` now ends at 16: SaslHandshake (17)
  and ApiVersions (18) are Kafka 0.10.
- **Error codes 21-31** — `InvalidRequiredAcksException` (21), `IllegalGenerationException` (22),
  `InconsistentGroupProtocolException` (23), `InvalidGroupIdException` (24),
  `UnknownMemberIdException` (25), `InvalidSessionTimeoutException` (26),
  `RebalanceInProgressException` (27), `InvalidCommitOffsetSizeException` (28),
  `TopicAuthorizationFailedException` (29), `GroupAuthorizationFailedException` (30) and
  `ClusterAuthorizationFailedException` (31), with their constants on `KafkaException` and their
  entries in the code map. None of them is retriable in the Java client of 0.9.0.1.
- **An api-version probe against a real broker** (`tests/Integration/ApiVersionProbeTest.php`,
  `tests/Fixture/RawApiProbe.php`). Kafka 0.9 has no ApiVersions request, so the api surface of a
  broker can only be established by sending a minimal request of every key and version; the result
  is the api-key table of the protocol document.

### Changed

- **Error code 13 is `NetworkException`** (`ServerExceptionInterface`, retriable), as on `main`.
  It was `StaleLeaderEpochCode` in `kafka/common/ErrorMapping.scala` @ 0.8.2.2 and no broker ever
  sent it; `StaleLeaderEpochException` is therefore gone and the class the socket layer raises for
  a dropped connection now carries the wire code 13.
- **OffsetFetch v1 for a partition the cluster does not host** answers offset `-1` with the error
  code `0` on a 0.9.0.1 broker, where 0.8.2.2 answered 3 (`UnknownTopicOrPartition`): version 1
  stopped filtering the requested partitions against the metadata cache. The integration suite and
  the protocol document record it.
- **`docs/protocol/0.9.0.md`** now describes the Kafka 0.9.0.1 grammar: the api-key table with the
  probed versions, the error table -1 … 31, what a 0.9 broker does with an api it does not serve
  (it drops the request silently and keeps the connection open), which apis do not validate the
  version they are sent with, and what is *not* in 0.9.
- README and CHANGELOG follow the `0.9.x` line; the compatibility matrix lists the api keys 0-16.

### Removed

- `StaleLeaderEpochException` and the `STALE_LEADER_EPOCH` constant, whose code 13 belongs to
  `NetworkException` from Kafka 0.9 onwards.

Unreleased — the 0.8.x line
---------------------------

The history below is the one of the `0.8.x` branch, which this line was merged from: its first
release, a rewrite of the client onto the declarative binary schema engine of `main`, with every
api of a Kafka 0.8.2.2 broker implemented and verified against a real one.

### Added

- **Protocol engine.** `Protocol\BinarySchema` and `BinarySchemaInterface::getScheme()`: every
  message and DTO declares its binary layout and the engine reads, writes and sizes it. No
  request or response class hand-rolls `pack()`/`unpack()` any more. Two defects of the engine
  on `main` are fixed here: `int8` is read as a signed value, and a null byte array round-trips
  as the int32 `-1` the specification prescribes.
- **Framing.** A response is read into memory by its announced `Size` and parsed from that
  buffer, so a body parser can never read past its own message and desynchronize the connection.
  A frame larger than `socket.request.max.bytes` is rejected.
- **All client-facing apis of Kafka 0.8.2.2**: Metadata v0, Produce v0, Fetch v0,
  Offsets (ListOffset) v0, GroupCoordinator v0, OffsetCommit v0 and v1, OffsetFetch v0 and v1,
  plus ControlledShutdown v0.
- **Network client.** One connection per broker, opened on demand and kept open for the requests that follow
  (`Network\ConnectionFactory`, `connections.max.idle.ms`); a correlation id on every request that is checked against
  the answer (`Network\ResponseValidator`, `Errors\CorrelationIdMismatchException`), and a metadata refresh with
  `retries`/`retry.backoff.ms` for the errors that one can cure - 3, 5, 6 and a dropped connection
  (`Network\RetryPolicy`). A request that fans out over several partition leaders and only partly succeeds is
  reported as a `Errors\TopicPartitionRequestException` carrying the partial result and the exception of each failed
  topic-partition.
- **Message set v0** (`Common\Record\{Record,Message,MessageSet,CompressionCodec,Snappy}`) with
  CRC-32 validation, gzip and snappy compression, unwrapping of compressed sets and silent
  dropping of the partial trailing message a broker is allowed to send.
- **`Producer\KafkaProducer`**, the 0.8 port of the producer of `main`: `send()` buffers a record
  and hands back a promise that is resolved with the `RecordMetadata` of the batch its
  topic-partition was appended in, or rejected with the error of that partition. Batching by
  `batch.size` and `linger.ms`, `compression.type` applied to a whole batch, `max.request.size`
  enforced before a record is buffered, and `Producer\DefaultPartitioner`, which places a keyed
  record with the same murmur2 hash as `org.apache.kafka.common.utils.Utils.murmur2` and spreads
  the records without a key over the partitions that have a leader.
- **`Admin\AdminClient`**, the 0.8 port of the admin client of `main`: `findAllBrokers()`,
  `findCoordinator()`, `listGroupOffsets()`, and the 0.8-specific `listTopics()`,
  `describeTopics()`, `listOffsets()` and `controlledShutdown()`.
- **`Common\CoordinatorLookup`**, which retries the GroupCoordinator request while the broker
  answers 15 (the `__consumer_offsets` topic is being created) or 14 (the coordinator is loading
  the offsets of the group).
- **`offsets.storage`** configuration option, selecting the Kafka-backed version 1 or the
  ZooKeeper-backed version 0 of the OffsetCommit and OffsetFetch apis.
- **Documentation**: [docs/protocol/0.9.0.md](docs/protocol/0.9.0.md) describes the whole 0.8.2.2
  grammar, the broker quirks and an annotated hex dump of every wire vector; the same vectors are
  stored machine-readable in [docs/protocol/vectors](docs/protocol/vectors).
- **Tests**: a unit suite, an integration suite that runs against the Kafka 0.8.2.2 container of
  `docker-compose.yml`, and a compliance suite that replays every documented wire vector through
  the request and response classes and keeps the document and the code from drifting apart. All
  three run in CI.
- Runnable examples: [`examples/producer.php`](examples/producer.php),
  [`examples/consumer.php`](examples/consumer.php) and [`examples/admin.php`](examples/admin.php).

### Changed

- Identifiers follow the `main` branch wherever the concept exists on both lines, so that the
  cascade merges upwards stay mechanical — only the wire format is 0.8. Api key 10 is therefore
  `GroupCoordinator` (`ConsumerMetadata` in the 0.8.2 sources), and the exception classes are
  `MessageTooLargeException` (10), `GroupLoadInProgressException` (14),
  `GroupCoordinatorNotAvailableException` (15), `NotCoordinatorForGroupException` (16) and
  `RecordListTooLargeException` (18).
- The version-independent value objects (`TopicPartition`, `RecordMetadata`, the serializers,
  `Cluster`, `Node`, the configuration classes) are the ones of `main`.
- PHP 8.4 throughout: typed properties, `declare(strict_types=1)`, PER-CS 2.0, PHPStan clean.

### Removed

- The `packPayload()`/`unpackPayload()` bridge of the base classes, which let a protocol class
  parse its payload by hand while the branch was being migrated. Every class is described by a
  scheme now, so the hook and its reflection lookups are gone.
- Everything the Kafka 0.8.2.2 protocol does not have: group membership (JoinGroup … LeaveGroup),
  `describeGroup()`/`listGroups()`/`getApiVersions()`, SSL and SASL, `ThrottleTime`, nullable
  topic arrays, message format v1 with timestamps, LZ4, record batches v2 and the error codes
  above 20.
