Changelog
==
All notable changes to `lisachenko/kafka-client` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and every line of
this repository follows the Apache Kafka release it speaks rather than semantic versioning of its
own: `main` implements the **Kafka 1.1.1 wire protocol** — the last release of the 1.x line — and
nothing above it. The lines below it are `0.11.x` (Kafka 0.11.0.3), `0.10.x` (Kafka 0.10.2.2),
`0.9.x` (Kafka 0.9.0.1) and `0.8.x` (Kafka 0.8.2.2), and every line is merged upwards into the next
one, so the sections below accumulate: what a line added stays true of every line above it.

Unreleased — the 1.x line (Kafka 1.1.1)
---------------------------------------

The 1.x line, built on top of the `0.11.x` line it was cascade-merged from. Everything below is
verified against a real Apache **1.1.1** broker (`docker/kafka-1.1.1/`, four listeners) and
documented in [docs/protocol/1.1.md](docs/protocol/1.1.md). The line is in development; this
section grows with every ticket that lands.

### Added

- **The Kafka 1.1.1 broker of the line** — `docker/kafka-1.1.1/` with the four listeners of the
  0.11 image (PLAINTEXT 9092, SSL 9093, SASL_PLAINTEXT 9094, SASL_SSL 9095) and its
  `transaction.state.log.*=1` settings, plus two new ones: **two log directories**
  (`log.dirs=/tmp/kafka-logs,/tmp/kafka-logs-2`, so that `AlterReplicaLogDirs` can move a replica
  instead of only answering 57) and a **`delegation.token.master.key`** (so that the token apis
  answer 64 instead of 61). `inter.broker.protocol.version` and `log.message.format.version` are
  `1.1-IV0`; `docker-compose.yml` builds it as the container `kafka-1-1-1`.
- **Api keys 34-42** — `ALTER_REPLICA_LOG_DIRS` (34), `DESCRIBE_LOG_DIRS` (35),
  `SASL_AUTHENTICATE` (36), `CREATE_PARTITIONS` (37), `CREATE_DELEGATION_TOKEN` (38),
  `RENEW_DELEGATION_TOKEN` (39), `EXPIRE_DELEGATION_TOKEN` (40), `DESCRIBE_DELEGATION_TOKEN` (41)
  and `DELETE_GROUPS` (42). `Protocol\ApiKeys` now ends at 42; everything above it is Kafka 2.x.
- **Error codes 56-71** — `KafkaStorageException` (56), `LogDirNotFoundException` (57),
  `SaslAuthenticationFailedException` (58), `UnknownProducerIdException` (59),
  `ReassignmentInProgressException` (60) with Kafka 1.0, and `DelegationTokenDisabledException`
  (61), `DelegationTokenNotFoundException` (62), `DelegationTokenOwnerMismatchException` (63),
  `UnsupportedByAuthenticationException` (64), `DelegationTokenAuthorizationException` (65),
  `DelegationTokenExpiredException` (66), `InvalidPrincipalTypeException` (67),
  `GroupNotEmptyException` (68), `GroupIdNotFoundException` (69), `FetchSessionIdNotFoundException`
  (70) and `InvalidFetchSessionEpochException` (71) with Kafka 1.1, each with its constant on
  `KafkaException` and its entry in the code map. **56, 70 and 71 are the only retriable ones**, as
  in `Errors.java` @ 1.1.1, and 59 extends `OutOfOrderSequenceException` because it is the special
  case of an out-of-order sequence the broker can explain. Two identifiers deviate from the Java
  client on purpose: 58 is `SaslAuthenticationFailedException`, because `SaslAuthenticationException`
  is already the client-side exception of this package, and 64 keeps the Java *class* name
  `UnsupportedByAuthenticationException` rather than its constant.
- **`DescribeGroupResponseMetadata::STATE_COMPLETING_REBALANCE`** — Kafka 1.0 renamed the group
  state between the last JoinGroup and the leader's SyncGroup from `AwaitingSync` to
  `CompletingRebalance`, and that is the string a 1.x coordinator answers. `STATE_AWAITING_SYNC`
  stays for the lines below, documented as the 0.9-to-0.11 name of the very same state.
- **SaslHandshake v1 and the SaslAuthenticate api (key 36, v0)** — KIP-152, Kafka 1.0. The client
  now opens a SASL connection with a **v1** handshake (`SaslHandshakeRequest::VERSION = 1`,
  `SaslHandshakeRequestV0` for the frame of the lines below) and carries the PLAIN token inside a
  `SaslAuthenticateRequest`, whose `SaslAuthenticateResponse` finally has an error code: wrong
  credentials are the code **58** (`SaslAuthenticationFailedException`) with the message of the
  broker — `Authentication failed: Invalid username or password` — where a 0.11 broker closed the
  connection without a word. `SaslAuthenticationException`, the client-side exception the socket
  layer raises, carries that code, the broker's message and the wire exception as its cause, and
  still leaves every retry loop of the client. The raw, unframed exchange of a v0 handshake stays
  implemented and is still served by a 1.1.1 broker; `IO\SocketStream` selects it through a single
  protected method, and both paths are covered by the unit and the integration suite.
- **Wire vectors of the two apis** — `docs/protocol/vectors/sasl-authenticate.json` (new) and six
  more entries in `sasl-handshake.json`: the v1 handshake and its answer, the 33 of a mechanism the
  broker has not enabled, the 34 of a second handshake (with the **empty** mechanism list that
  Kafka 1.1 answers there, where 1.0.2 still filled it), the accepted `SaslAuthenticate` exchange,
  the 58 of a wrong password and the 34 of a second `SaslAuthenticate`. The v0 vectors of the
  handshake are replayed through `SaslHandshakeRequestV0` from now on.
- **Produce v4 and v5 (Kafka 1.0)** — the body of the versions 3, 4 and 5 is one and the same
  (`PRODUCE_REQUEST_V5` is `PRODUCE_REQUEST_V4` is `PRODUCE_REQUEST_V3` @ 1.1.1). Version 4 states
  that the client understands the error code **56** `KAFKA_STORAGE_ERROR`, which a broker translates
  to 6 `NOT_LEADER_FOR_PARTITION` for a version 3 or lower; version 5 appends **`log_start_offset`**
  to every partition entry of the answer. `ProduceRequest::VERSION` is 5 with `ProduceRequestV4` and
  `ProduceRequestV3` below it, `ProduceResponse` is v5 with `ProduceResponseV4`/`V3` and the new
  `ProduceResponsePartitionV2`/`ProduceResponseTopicV2` for the frame the versions 2 to 4 share, and
  `ProduceResponsePartition::$logStartOffset` is `-1` (`INVALID_OFFSET`) below version 5.
  `Client::produce()` sends **v5** for the message format v2 and keeps v2 for the legacy message
  sets; the `ProduceRequest::__construct()` signature is unchanged.
- **Fetch v6 and v7 with the incremental fetch sessions of KIP-227 (Kafka 1.0 and 1.1)** — v6 is the
  v5 frame in both directions and states that the client understands the error code 56; **v7** adds
  `session_id` and `epoch` (int32) between the isolation level and the topics array and a trailing
  `forgotten_topics_data`, and the answer gains a top-level `error_code` and `session_id` behind the
  throttle time. New: `Protocol\Request\FetchMetadata` — the Java `FetchMetadata`, field for field,
  with `INVALID_SESSION_ID`, `INITIAL_EPOCH`, `FINAL_EPOCH`, `legacy()`, `initial()`,
  `newIncremental()`, `nextIncremental()`, `nextCloseExisting()` and `isFull()` (the Java constants
  `LEGACY` and `INITIAL` are factory methods here, because a PHP class constant cannot hold an
  object) — and `Protocol\Data\FetchRequestForgottenTopic`. `FetchRequest::VERSION` is 7 with
  `FetchRequestV6`/`V5` below it and two new optional constructor arguments (`?FetchMetadata
  $metadata = null`, `array $forgottenTopicPartitions = []`); `FetchResponse` is v7 with
  `FetchResponseV6`/`V5` and the new `$errorCode`/`$sessionId`, both 0 below version 7.
  `Client::fetchPartitions()` sends v7 with the **session-less** metadata (`session_id 0`,
  `epoch -1`), which a 1.1.1 broker serves exactly as it serves a Fetch v6; the sessions in the
  consumer are a ticket of their own.
- **Metadata v5 (Kafka 1.0, KIP-112/113)** — every partition entry of the answer gains
  **`offline_replicas`**, the replicas whose broker is down or whose log directory has failed; the
  request is the version 4 frame. `MetadataRequest`/`MetadataResponse` are v5 with
  `MetadataRequestV4`/`MetadataResponseV4` below them, `Common\PartitionMetadata` gains
  `$offlineReplicas` (`[]` below version 5) with `PartitionMetadataV0` for the entry of the versions
  0 to 4, and `Common\TopicMetadataV1` carries the topic entry of the versions 1 to 4.
  `Cluster`, `TopicMetadata` and `AdminClient::describeTopics()` pass the new field through; on a
  one-broker cluster it is always empty.
- **Wire vectors of the three apis** — sixteen frames captured on the 1.1.1 container with the
  client id and topic `t3-vectors`: the v4 and v5 pairs of Produce (the v5 answer after a
  `DeleteRecords`, so its `log_start_offset` is 2), the v6 pair of Fetch, the six frames of one
  fetch session (the full fetch that opens it, the incremental fetch that is answered with the one
  partition that changed, the fetch that forgets a partition, and the two session errors **70** and
  **71**) and the v5 pair of Metadata.
- **The two JBOD apis of KIP-113, Kafka 1.0** — `DescribeLogDirs` (key 35, v0) and
  `AlterReplicaLogDirs` (key 34, v0), with `Admin\AdminClient::describeLogDirs()` and
  `Admin\AdminClient::alterReplicaLogDirs()`. Both are **broker-local**, so the first takes a list
  of broker ids and answers `array<int, array<string, Admin\LogDirInfo>>` — broker id, then the
  absolute path of each `log.dirs` entry — and the second is keyed by an
  `Admin\TopicPartitionReplica` (`topic-partition-brokerId`, the `toString()` of the Java class)
  and reports `null` or the exception of each replica, like `alterConfigs()`. A replica is an
  `Admin\ReplicaInfo` with `size`, `offsetLag` and `isFuture`. The request array of DescribeLogDirs
  is **nullable**: `null` asks for every replica of the broker, an empty array only for the
  directories themselves. `alterReplicaLogDirs()` answers as soon as the move is **accepted** — the
  copy runs in a `ReplicaAlterLogDirsThread`, and the replica is reported in both directories, the
  destination with `isFuture = true` and an `offsetLag` that counts down, until the mover swaps the
  logs in.
- **Wire vectors of the two apis** — `docs/protocol/vectors/describe-log-dirs.json` and
  `alter-replica-log-dirs.json`, six frames each, captured on the two log directories of the
  container: both shapes of the nullable topic array (the empty one answers 64 bytes, the null one
  answered 235 207 bytes for the several thousand replicas of the shared container and is therefore
  not stored), the answer for one named partition, the answer taken **while a real move was
  running**, the accepted move, the **57** of a path that is not in `log.dirs` and the **9** of a
  replica the broker does not host.
- **`examples/admin-log-dirs.php`** — the disks of every broker, a replica moved between two of
  them and watched while the mover copies it, and the two error codes the api has of its own.
- **The four delegation-token apis of KIP-48 (keys 38 to 41, all v0, Kafka 1.1)** —
  `CreateDelegationTokenRequest`/`Response`, `RenewDelegationToken…`, `ExpireDelegationToken…` and
  `DescribeDelegationToken…` with `Data\DescribeDelegationTokenResponseToken`, the principal struct
  `Common\Security\KafkaPrincipal` that all four embed, and the value objects
  `Admin\DelegationToken` and `Admin\TokenInformation` (the names of the Java client). The
  `AdminClient` gained `createDelegationToken(array $renewers = [], int $maxLifeTimeMs = -1)`,
  `renewDelegationToken(string $hmac, int $renewTimePeriodMs = -1)`,
  `expireDelegationToken(string $hmac, int $expiryTimePeriodMs = -1)` and
  `describeDelegationToken(?array $owners = null)`, each of them served by any broker of the
  cluster. **`throttle_time_ms` is the last field of all four answers**, not the first one — an api
  added after KIP-124 appends it. A token is named by the raw bytes of its **HMAC**, never by its
  id; `DelegationToken::hmacAsBase64String()` is the form `kafka-delegation-tokens.sh` prints.
- **Limitation: a delegation token can be issued but not used.** Authenticating *with* a token is a
  SASL/SCRAM login whose user name is the token id and whose password is the base64 HMAC, and this
  client speaks SASL/**PLAIN** only. The four apis are implemented and verified against a real
  1.1.1 broker; the login with their result is not implemented. See "What is not in Kafka 1.1.1".
- **Wire vectors of the token apis** — the new `docs/protocol/vectors/delegation-tokens.json`, the
  one vector file that holds several apis (its `apiKey` is null and every request vector carries the
  key of its own api). Its fourteen frames are one life of one token on the SASL_PLAINTEXT listener
  with the client id `t7-vectors` and the principal `User:kafkatest`, plus the error answers 67, 63,
  62, 66 and the two 64s of the PLAINTEXT listener.

### Changed

- **The protocol document is `docs/protocol/1.1.md`** (renamed from `docs/protocol/0.11.0.md`, as
  every line renames it), and its front matter, api-key table, "What is not in Kafka 1.1.1", error
  codes, group-state tables and "Broker quirks and observations" now describe Kafka 1.1.1. The
  section "The last Scala api does not check its version" became **"Every api of the table
  validates its version"**: Kafka 1.0 moved ControlledShutdown to the schemas of the Java client,
  so **ApiVersions is the only api left whose unknown version is answered** instead of costing the
  connection.
- **The api-key table is the 43-key answer of a 1.1.1 broker** (keys 0 to 42), re-captured into
  `docs/protocol/vectors/api-versions.json` as `apiversions.response.v0` and `.v1`. Against
  0.11.0.3 it raises Produce to v5, Fetch to v7, Metadata to v5, SaslHandshake to v1 and
  DescribeConfigs to v1, adds the nine keys 34-42, and reports **ControlledShutdown as v0-v1**
  where a 0.9-to-0.11 broker reported v1 alone.
- **Behaviour of the broker that the inherited suite pinned differently** — all of it measured on
  the container and recorded in the document: a duplicate of any of the **last five** batches of a
  producer id and partition is answered as the original append (0.11 kept one batch and answered 45
  for anything older); a Fetch below v4 of a partition whose records carry headers is **served with
  the headers dropped** and the error code 0, where 0.11 refused it with -1 — measured with a batch
  of three records of which only the middle one has a header, so no record is skipped and the
  timestamps survive down to the message format v1; `is_default` of a
  DescribeConfigs v0 answer is derived from the KIP-226 config **source**, so a topic option whose
  broker synonym stands in the `server.properties` is not a default any more; `is_read_only` of a
  broker entry means "not dynamically updatable"; AlterConfigs **accepts** a broker resource and
  refuses it per option (`Cannot update these configs dynamically: Set(log.retention.hours)`); and
  CreateTopics rewrote three of its error messages (`Number of partitions must be larger than 0.`,
  `Replication factor: 2 larger than available brokers: 1.`, `Topic name "x" is illegal, it
  contains a character other than …`).
- **README** now announces the 1.x line: the badges point at `main`, the protocol-version matrix
  lists all 43 api keys of a 1.1.1 broker with the ticket that raises each remaining one, and the
  feature matrix gained a `main` column.

Unreleased — the 0.11.x line (Kafka 0.11.0.3)
-------------------------------------------

The 0.11 line, built on top of the `0.10.x` line it was cascade-merged from. Everything below was
verified against a real Apache **0.11.0.3** broker (`docker/kafka-0.11.0.3/`, four listeners) and
is documented byte for byte in [docs/protocol/1.1.md](docs/protocol/1.1.md), whose 229 wire
vectors [`tests/Compliance`](tests/Compliance) replays through the protocol classes — the 109 frames
this line captured and the 120 of the three lines below, which a 0.11.0.3 broker still speaks.

### Added

- **The Kafka 0.11.0.3 broker of the line** — `docker/kafka-0.11.0.3/` with the four listeners of
  the 0.10 image (PLAINTEXT 9092, SSL 9093, SASL_PLAINTEXT 9094, SASL_SSL 9095) plus
  `transaction.state.log.replication.factor=1` and `transaction.state.log.min.isr=1`, without
  which the `__transaction_state` topic cannot be created on a one-broker cluster;
  `docker-compose.yml` builds it as the container `kafka-0-11-0-3`.
- **Api keys 21-33** — `DELETE_RECORDS` (21), `INIT_PRODUCER_ID` (22), `OFFSET_FOR_LEADER_EPOCH`
  (23), `ADD_PARTITIONS_TO_TXN` (24), `ADD_OFFSETS_TO_TXN` (25), `END_TXN` (26),
  `WRITE_TXN_MARKERS` (27), `TXN_OFFSET_COMMIT` (28), `DESCRIBE_ACLS` (29), `CREATE_ACLS` (30),
  `DELETE_ACLS` (31), `DESCRIBE_CONFIGS` (32) and `ALTER_CONFIGS` (33). `Protocol\ApiKeys` now ends
  at 33; everything above it is Kafka 1.0.
- **Error codes 45-55** — `OutOfOrderSequenceException` (45), `DuplicateSequenceNumberException`
  (46), `ProducerFencedException` (47), `InvalidTxnStateException` (48), `InvalidPidMappingException`
  (49), `InvalidTxnTimeoutException` (50), `ConcurrentTransactionsException` (51),
  `TransactionCoordinatorFencedException` (52), `TransactionalIdAuthorizationException` (53),
  `SecurityDisabledException` (54) and `OperationNotAttemptedException` (55), with their constants
  on `KafkaException` and their entries in the code map. **46 is the only retriable one of the
  eleven**, as in `Errors.java` @ 0.11.0.3 — a duplicate sequence number means the batch is already
  in the log — and a 0.11.0.3 broker never sends it to a client (see below).
- **The zigzag varint family of the record batch v2 in the schema engine** —
  `BinarySchema::TYPE_VARINT_ZIGZAG` (11), `TYPE_VARLONG_ZIGZAG` (12), `TYPE_VARCHAR_ZIGZAG` (13)
  and `FLAG_VARARRAY` (14), with `Common\Utils\ByteUtils` for the zigzag and CRC-32C helpers of
  `org.apache.kafka.common.utils.ByteUtils` and `Stream::readVarint()`/`writeVarint()` for the byte
  loop. No request or response body of Kafka 0.11 uses them — only the records inside a batch do.
- **The record batch v2, the message format of Kafka 0.11** (KIP-98 for the format, KIP-82 for the
  headers) — `Common\Record\RecordBatch` is the 61-byte batch header as a `getScheme()` declaration
  plus the reader and writer of its delta-encoded records, next to the `MessageSet`/`Message` of the
  formats v0 and v1, which a 0.11.0.3 broker still reads and writes. `Common\Record\RecordV2` is the
  wire form of a record — varint lengths, offset and timestamp deltas, varint-counted headers — and
  `Common\Record\Header`, `ControlRecordKey`, `ControlRecordType` and `EndTransactionMarker` are the
  headers of a record and the markers of the transaction protocol. `Common\Record\MemoryRecords`
  reads a byte region of **any** of the three formats, dispatching on the magic byte at the offset 16
  of an entry, and hides the records of a control batch from an application. The checksum is the
  **CRC-32C** of `ByteUtils`, covering the batch from its attributes on, so that the base offset and
  the partition leader epoch the broker assigns stay outside it; compression compresses the records
  and not a wrapper message. `Record` gained the trailing `array $headers` and `withHeaders()`, and
  `Message::classOfMagic()` refuses a magic 2 with the reader to use instead. Twelve wire vectors in
  `docs/protocol/vectors/message-format.json`, among them a captured control batch and the two
  down-conversions a 0.11 broker performs.
- **ApiVersions v1 (key 18)** — `ApiVersionsRequest` now sends **version 1** and
  `ApiVersionsResponse` reads its trailing `throttleTimeMs`; `ApiVersionsRequestV0` and
  `ApiVersionsResponseV0` keep the version 0 frames, which is also the layout the broker answers an
  unknown version in. `Client::apiVersions()` and `Admin\AdminClient::getApiVersions()` send v1 and
  report the 34 api keys a 0.11.0.3 broker serves. Five wire vectors in
  `docs/protocol/vectors/api-versions.json`, captured on the container.
- **`FetchRequest::READ_UNCOMMITTED` / `READ_COMMITTED`** — the isolation levels of Fetch v4
  (KIP-98), under the identifiers of the pre-schema `main`.
- **The throttle time of KIP-124 on fourteen apis** — the client now sends Metadata **v4**, Offsets
  **v2**, OffsetCommit **v3**, OffsetFetch **v3**, GroupCoordinator **v1**, JoinGroup **v2**,
  Heartbeat **v1**, LeaveGroup **v1**, SyncGroup **v1**, DescribeGroups **v1**, ListGroups **v1**,
  CreateTopics **v2** and DeleteTopics **v1**, and every one of those answers is read with a leading
  `throttleTimeMs`. Each lower version keeps a class of its own (`…RequestV<n>` / `…ResponseV<n>`),
  so the class of an answer always names the version of the request that asked for it, and the
  vectors of the lines below replay unchanged.
- **GroupCoordinator v1 (key 10, FindCoordinator in the 0.11 sources)** — the request gained
  `coordinator_type` (`GroupCoordinatorRequest::COORDINATOR_TYPE_GROUP` /
  `COORDINATOR_TYPE_TRANSACTION`) and the answer a `throttleTimeMs` and a nullable `errorMessage`.
  `Common\CoordinatorLookup::findCoordinator(string $key, int $coordinatorType = …)` takes the type,
  and `Client::getTransactionCoordinator(string $transactionalId)` looks a transactional id up with
  it — the request a transactional producer has to send before anything else.
- **`allow_auto_topic_creation` of Metadata v4 (KIP-4)** — `Common\Cluster` sends `true`, which is
  what every version below 4 does implicitly, while `Admin\AdminClient::describeTopics()`,
  `listTopics()` and `findAllBrokers()` send `false`: an administrator no longer creates a topic by
  asking about it, and an absent topic is answered with the error code 3.
- **`isolation_level` of Offsets v2 (KIP-98)** — `OffsetsRequest` takes it as its third argument and
  defaults to `FetchRequest::READ_UNCOMMITTED`; with `READ_COMMITTED` the broker answers the last
  stable offset instead of the log end offset.
- **OffsetForLeaderEpoch (key 23, v0, KIP-101)** — `OffsetForLeaderEpochRequest`/`Response` and
  their four DTOs, with wire vectors in `docs/protocol/vectors/offset-for-leader-epoch.json`. It is
  a broker-to-broker api and this client sends it nowhere; a broker without an authorizer answers an
  ordinary client all the same, which is how the vectors were captured.
- **Produce v3** (KIP-98) — `ProduceRequest` sends version 3 with the nullable `TransactionalId` in
  front of `RequiredAcks` and a **record batch of the message format v2** per topic-partition;
  `ProduceRequestV2` keeps the highest version that may carry a legacy message set, because a
  0.11.0.3 broker *closes the connection* on a Produce v3 whose magic is below 2. The answer of
  version 3 is the answer of version 2 byte for byte (`PRODUCE_RESPONSE_V3` **is**
  `PRODUCE_RESPONSE_V2`), so `ProduceResponse` and the new `ProduceResponseV2` read the same frame;
  the `log_start_offset` of a produce answer is Kafka 1.0 and is not on this line. Three wire
  vectors in `docs/protocol/vectors/produce.json`.
- **Fetch v4 and v5** (KIP-98, KIP-107) — `FetchRequest` sends version 5 with the `IsolationLevel`
  of the consumer and the per-partition `LogStartOffset`, and its answer carries
  `LastStableOffset`, `LogStartOffset` and the nullable `AbortedTransactions` array
  (`Protocol\Data\FetchResponseAbortedTransaction`) of every partition. `FetchRequestV4`/`V3` and
  `FetchResponseV4`/`V3` keep the lower versions; `FetchResponsePartition::getRecords()` returns a
  `MemoryRecords` region that reads whichever of the three message formats the broker answered
  with. Seven wire vectors in `docs/protocol/vectors/fetch.json`, one of them a `read_committed`
  answer with an aborted transaction in it.
- **`Common\FetchedPartition` reports the transactional state of a partition** — `$lastStableOffset`,
  `$logStartOffset` and `$abortedTransactions` next to the high water mark, plus
  `getMemoryRecords()` and `hasPartialTrailingRecord()`; `getNextOffset()` moves past a control
  batch, which holds no record an application may see.
- **Record headers travel end to end** (KIP-82) — `KafkaProducer::send()` writes the
  `Record::$headers` into the record batch of a Produce v3, and `KafkaConsumer::poll()` reads them
  back out of a Fetch v5 answer; `Consumer\ConsumerRecord` carries them next to the deserialized
  key and value. They exist in the message format v2 alone: a Fetch below version 4 of a partition
  whose records carry headers is answered with the error code -1, because the broker cannot
  convert them down.
- **`Client::produceRecords()`** — the one place that turns records into the record set of a
  Produce request, with the producer id, the producer epoch, the per-topic-partition base sequence
  and the transactional id of KIP-98 as its parameters (-1, -1, none and `null` for a plain
  producer). `Client::produce()` is that method without producer state.
- **DeleteRecords v0 (key 21, KIP-107)** — `DeleteRecordsRequest`/`DeleteRecordsResponse` with
  their topic and partition DTOs, `Client::deleteRecords()` (split per partition leader, like a
  produce) and `Admin\AdminClient::deleteRecords()`, which answers the new **low watermark** of
  every partition as an `Admin\DeletedRecords`. The offset to delete before is a plain integer or
  an `Admin\RecordsToDelete`; `RecordsToDelete::allRecords()` is the `-1` of the wire, i.e.
  everything up to the high watermark. Six wire vectors in
  `docs/protocol/vectors/delete-records.json`.
- **DescribeConfigs v0 (key 32, KIP-133)** — `DescribeConfigsRequest`/`DescribeConfigsResponse`
  with their resource and entry DTOs, and `Admin\AdminClient::describeConfigs()`, which reads the
  configuration of a topic or of a broker into an `Admin\Config` of `Admin\ConfigEntry` objects
  (`value`, `isDefault`, `isSensitive`, `isReadOnly`). A resource is named with an
  `Admin\ConfigResource` (`topic()` / `broker()`, the type ids of
  `org.apache.kafka.common.requests.ResourceType` @ 0.11.0.3) and addressed in the result by its
  `key()`; a **broker** resource is sent to the broker it names, a topic resource to any broker.
  Six wire vectors in `docs/protocol/vectors/describe-configs.json`.
- **AlterConfigs v0 (key 33, KIP-133)** — `AlterConfigsRequest`/`AlterConfigsResponse` with their
  DTOs and `Admin\AdminClient::alterConfigs()`, which **replaces** the whole configuration of a
  topic (an option left out is reset to its default, which is what `Config::nonDefaultValues()`
  exists for) and reports the error of every resource instead of throwing. A 0.11 broker alters
  topics only and refuses a broker resource with 42; `validateOnly` validates without writing. Six
  wire vectors in `docs/protocol/vectors/alter-configs.json`.
- **InitProducerId v0 (key 22, KIP-98)** — `InitProducerIdRequest`/`InitProducerIdResponse` and
  `Client::initProducerId(?string $transactionalId = null, int $transactionTimeoutMs = 60000)`,
  which answers a `Producer\Internals\ProducerIdAndEpoch`. A `null` transactional id is asked of
  any broker of the cluster and answers a fresh producer id with the epoch 0; a real one is sent to
  `Client::getTransactionCoordinator()` and answers the producer id of that id with its epoch
  bumped by one. Six wire vectors in `docs/protocol/vectors/init-producer-id.json`.
- **The idempotent producer (KIP-98)** — `ProducerConfig::ENABLE_IDEMPOTENCE`
  (`enable.idempotence`, off by default) and `ProducerConfig::TRANSACTION_TIMEOUT_MS`
  (`transaction.timeout.ms`, 60000). With the option on, `KafkaProducer` keeps a
  `Producer\Internals\TransactionManager`: it asks for a producer id with the first flush, stamps
  every batch with the producer id, the epoch and the next sequence number of its topic-partition,
  and moves that sequence on by the records the broker acknowledged, so a batch that is sent again
  after a lost acknowledgement is recognised by the broker and answered with the offset of the
  original append instead of being written twice. `Client::produce()` takes the manager as its
  second argument, `ProducerConfig::resolveIdempotence()` applies what the guarantee implies —
  `acks = all` and a non-zero `retries`, both overridden when the caller left them alone and
  refused when the caller set them to something else; `Client::produce()` refuses producer state
  next to anything but `acks = all` as well. Four Produce v3 vectors of an idempotent
  batch, its duplicate and an out-of-order sequence in `docs/protocol/vectors/produce.json`.
- **The error codes 45, 46 and 47 have a meaning for the producer now.** 47
  (`ProducerFencedException`) is fatal: the producer refuses everything after it. 45
  (`OutOfOrderSequenceException`) makes an idempotent producer throw its producer id away and start
  over with a new one, as the Java `Sender` @ 0.11.0.3 does; a transactional producer keeps it and
  reports the error. 46 (`DuplicateSequenceNumberException`) counts as an append — its sequence
  numbers are consumed and the partition is reported as accepted — although a 0.11.0.3 broker never
  sends it to a client: it answers a duplicate of the last batch with the code **0** and the offset
  of the original append, and a duplicate of an older one with 45.
- **The transactional producer of KIP-98** — the five apis of the transaction protocol
  (`AddPartitionsToTxnRequest`/`Response` 24, `AddOffsetsToTxnRequest`/`Response` 25,
  `EndTxnRequest`/`Response` 26, `WriteTxnMarkersRequest`/`Response` 27 — broker to broker, classes
  and vectors only — and `TxnOffsetCommitRequest`/`Response` 28, with their `Protocol\Data` DTOs),
  the client methods `Client::addPartitionsToTxn()`, `addOffsetsToTxn()`, `endTxn()` and
  `txnOffsetCommit()`, and the state machine of `Producer\Internals\TransactionManager` on top of
  the new `Producer\Internals\TransactionState` enum — `UNINITIALIZED`, `INITIALIZING`, `READY`,
  `IN_TRANSACTION`, `COMMITTING_TRANSACTION`, `ABORTING_TRANSACTION`, `ABORTABLE_ERROR`,
  `FATAL_ERROR`, with the transitions of `TransactionManager.State` @ 0.11.0.3. The coordinator
  codes 14, 15 and 16 make the coordinator be looked up again and 51 (`ConcurrentTransactions`) is
  retried, both bounded by `metadata.fetch.timeout.ms`; 47, 48, 49 and 53 are fatal and everything
  else that fails inside a transaction makes it abortable.
- **`ProducerConfig::TRANSACTIONAL_ID`** (`transactional.id`, default `null`) and the five methods
  of the Java producer on `KafkaProducer`: `initTransactions()`, `beginTransaction()`,
  `sendOffsetsToTransaction()`, `commitTransaction()` and `abortTransaction()`. A transactional id
  implies `enable.idempotence` — and with it `acks = all` and a non-zero `retries` — the empty
  string is refused as a configuration error, a `send()` outside a transaction is refused,
  `commitTransaction()` flushes the buffered batch first and `abortTransaction()` discards it.
- **`ConsumerConfig::ISOLATION_LEVEL`** (`isolation.level`, `read_uncommitted` by default) with the
  constants `ISOLATION_LEVEL_READ_UNCOMMITTED` and `ISOLATION_LEVEL_READ_COMMITTED`. The level is
  sent in the Fetch **and** in the Offsets request, so `KafkaConsumer::endOffsets()`, `position()`
  and `seekToEnd()` of a `read_committed` consumer answer the *last stable offset* instead of the
  log end offset; `Admin\AdminClient::listOffsets()` deliberately stays at `read_uncommitted`.
- **`Consumer\Internals\AbortedTransactionFilter`**, the `Fetcher.PartitionRecords` algorithm of
  the Java consumer: a 0.11.0.3 broker answers a `read_committed` fetch with the records of aborted
  transactions and their ABORT control batches in it and only *names* the transactions in
  `aborted_transactions`, so the consumer walks the batches of an answer and drops the ones whose
  producer id is aborted at that offset. Control batches never reach an application in either
  isolation level.
- **`examples/transactional-producer.php`** — the consume-transform-produce loop end to end,
  verified against the `read_committed` console consumer of the 0.11.0.3 container.

### Changed

- **`docs/protocol/1.1.md` is the grammar of Kafka 0.11.0.3.** The api-key table is the literal
  ApiVersions answer of the container (34 keys), the error-code table runs to 55, the sources are
  the ones at `0.11.0.3-rc0` — the Apache repository has no `0.11.0.3` tag — and the "Broker quirks
  and observations" section records what 0.11 changed against 0.10.2.2.
- **The raw varint type numbers 5, 6 and 7 are reserved.** They were `TYPE_VARINT`, `TYPE_VARLONG`
  and `TYPE_VARCHAR` of the pre-schema `main`, which wrote non-zigzag varints — an encoding that is
  on the wire of no api of Kafka 0.11 — and they stay free so that an old scheme cannot be mistaken
  for a new one.
- **The version that costs the connection moved with the release.** A 0.11.0.3 broker serves the
  versions a 0.10.2.2 broker closed the socket for (OffsetCommit v3, Metadata v3, JoinGroup v2 …)
  and closes it one version higher, and for the api key 34. The integration tests that pin that
  behaviour derive their frames from the served api table instead of hard-coding versions.
- **The `ErrorMessage` of an unknown topic-level option changed** from
  `Unknown Log configuration <name>.` to `Unknown topic config name: <name>`
  (`LogConfig.validateNames()` @ 0.11.0.3); the error code 40 and the shape of the answer are
  unchanged.
- **`ProducerConfig::MESSAGE_FORMAT_VERSION` defaults to `0.11.0`**, the record batch of the
  message format v2 (`ProducerConfig::MESSAGE_FORMAT_VERSION_0_11_0`), which is what the producer
  writes and what a Produce v3 request carries. `0.10.x` and `0.9.0` keep writing the message sets
  of the formats v1 and v0, and a client that configures one of them sends **Produce v2**, because
  a version 3 request accepts the message format v2 alone. Only the message format v2 has a place
  for record headers, for the producer id and the sequence numbers of an idempotent producer and
  for a transaction.
- **`Client::fetchPartitions()` sends Fetch v5** with the `isolation.level` of the configuration
  (`read_uncommitted` unless it says otherwise), so the records of a poll come back in the format
  the log holds them in instead of the message format v1 a version 3 answer was converted down to.
- **The container name of the test fixtures** is `kafka-0-11-0-3`, so the quota tests and the
  message-format tests that create a topic with `kafka-topics.sh` run again instead of skipping.
- **`main` carries only the broker of its own line.** `docker/kafka-0.10.2.2/` — a byte-identical copy
  of the 0.11 image but for its Kafka version — and `docker/kafka-0.9.0.1/`, which nothing on this branch
  used at all, were removed, so `docker/` holds `kafka-0.11.0.3/` alone. Every reference to a
  `ssl/broker.crt` or a `jaas.conf` in the tests and the examples points at `docker/kafka-0.11.0.3/` now.
  The images of the lower lines live on the `0.10.x` and `0.9.x` branches, where their tests need them.
- **The examples were brought to the protocol of the line.** `consumer.php` and `consumer-group.php`
  write a `RecordBatch` instead of a `MessageSet`, because the `ProduceRequest` they use is version 3
  now and a 0.11 broker closes the connection on a lower magic; `admin.php`, `ssl.php` and `sasl.php`
  create their topic with `createTopics()`, because `describeTopics()` sends
  `allow_auto_topic_creation = false` and no longer brings a topic into existence; `producer.php`
  writes the record batch v2 of the default `message.format.version`. Three examples are new:
  `record-headers.php` (KIP-82 end to end), `idempotent-producer.php` (`enable.idempotence`, the
  producer id and the sequence numbers) and `admin-configs.php` (`describeConfigs()`,
  `alterConfigs()` and `deleteRecords()`).

### Notes

- The ACL apis 29, 30 and 31 are **not** implemented: a broker without an `authorizer.class.name`
  answers all three with the error code 54 (`SecurityDisabled`), and every wire vector of this
  repository comes from a real broker.
- `SaslAuthenticate` (key 36) and the error code 56 are Kafka 1.0 and stay out of this line.
- **OffsetForLeaderEpoch (23) and WriteTxnMarkers (27) are broker-to-broker apis**: the classes and
  the wire vectors are here because they are part of the protocol of 0.11, but this client sends
  neither, and no method of `Client` or `AdminClient` produces one.
- **`retries` defaults to 3 with `enable.idempotence`**, not to the `Integer.MAX_VALUE` of the Java
  producer: this client has no background sender, so the budget is a loop that `flush()` blocks on
  and an unbounded one would be an unbounded flush.
- **A 0.11.0.3 broker remembers one batch per producer id and partition.** The five-batch window of
  `ProducerStateEntry.NumBatchesToRetain` is Kafka 1.0, so a duplicate of a batch that is no longer
  the last one is answered 45 and not with the offset of the original append.
- **Five error codes of the transaction protocol could not be produced on a one-broker container
  without an authorizer** — 49 (`InvalidProducerIdMapping`), 51 as a stable wire vector (it is
  transient), 52 (`TransactionCoordinatorFenced`, which needs two coordinators), 53 and 30. They are
  implemented from the sources of 0.11.0.3 and marked as such in the protocol document, next to the
  ones the container really answered.
- **The epoch of an `InitProducerId` may move by more than one.** A coordinator that has to roll an
  open transaction back bumps the epoch once for the fencing and once for the new producer, and
  answers 51 in between: a client reads the epoch it is given and never computes `epoch + 1`.

Previous line — 0.10.x (Kafka 0.10.2.2)
----------------------------------------

Everything a Kafka 0.10.2.2 broker speaks, built on top of the `0.9.x` line it was merged from.
Every wire format below was verified against a real 0.10.2.2 broker and is documented byte for
byte in [docs/protocol/1.1.md](docs/protocol/1.1.md), with 120 wire vectors in
[docs/protocol/vectors](docs/protocol/vectors) that `tests/Compliance` replays through the
protocol classes.

### Added

- **The Kafka 0.10.2.2 broker of the line** — `docker/kafka-0.10.2.2/` with a PLAINTEXT listener
  on 9092, an SSL listener on 9093 and the **SASL_PLAINTEXT** (9094) and **SASL_SSL** (9095)
  listeners that Kafka 0.10 makes possible, with SASL/PLAIN users in a JAAS file;
  `docker-compose.yml` builds it.
- **Api keys 17-20** — `SASL_HANDSHAKE` (17) and `API_VERSIONS` (18), which arrived with Kafka
  0.10.0, and `CREATE_TOPICS` (19) and `DELETE_TOPICS` (20), which arrived with 0.10.1.
  `Protocol\ApiKeys` now ends at 20; everything above it is Kafka 0.11.
- **Error codes 32-44** — `InvalidTimestampException` (32), `UnsupportedSaslMechanismException`
  (33), `IllegalSaslStateException` (34), `UnsupportedVersionException` (35),
  `TopicExistsException` (36), `InvalidPartitionsException` (37),
  `InvalidReplicationFactorException` (38), `InvalidReplicaAssignmentException` (39),
  `InvalidConfigException` (40), `NotControllerException` (41), `InvalidRequestException` (42),
  `UnsupportedForMessageFormatException` (43) and `PolicyViolationException` (44), with their
  constants on `KafkaException` and their entries in the code map. Only 41 is retriable.
- **`BinarySchema::TYPE_BOOLEAN`** — the one-byte primitive that Kafka 0.10 introduces
  (`is_internal` of Metadata v1, `validate_only` of CreateTopics v1). The engine also writes a
  `null` nullable array as `ff ff ff ff` and an empty one as `00 00 00 00`, which are two
  different requests from Metadata v1 on.
- **ApiVersions api (key 18, v0)** — `ApiVersionsRequest`, `ApiVersionsResponse` and
  `Protocol\Data\ApiVersionsResponseMetadata`. The response indexes the version range of every
  api by its api key and answers `supports(int $apiKey, int $version)` and
  `maxVersionOf(int $apiKey)`. `Client::apiVersions(Node $node)` returns the whole response,
  `Admin\AdminClient::getApiVersions(Node $node)` the indexed array — the name it has on `main`.
  This is the first line of the client that can ask a broker what it speaks instead of probing
  it frame by frame; the client itself still sends the fixed versions of its Kafka release.
- **Message format v1** (KIP-31/KIP-32) — `Common\Record\Message` is the format v1 message
  (`crc, magic, attributes, timestamp, key, value`) and `MessageV0` the format of 0.8/0.9, both
  driven by one version-aware scheme on `static::MAGIC`, with the magic byte as the
  discriminator. `Common\Record\TimestampType` (`NO_TIMESTAMP_TYPE`, `CREATE_TIME`,
  `LOG_APPEND_TIME`) is bit 3 of the attributes; `Record`, `ConsumerRecord` and
  `Producer\RecordMetadata` carry the timestamp and its type. A compressed v1 set stores
  **relative** inner offsets and the reader restores the absolute ones, a `LogAppendTime` wrapper
  replaces the timestamps of its inner messages, and `MessageSet::shallowFromBuffer()` is the
  shallow iteration a broker does.
- **LZ4 compression** — `Common\Record\Lz4`, a pure-PHP LZ4 block codec plus the Kafka LZ4 frame
  with the **KAFKA-3160** quirk (a magic 0 frame carries the broken descriptor checksum, a magic 1
  frame the correct one, a reader accepts both); `CompressionCodec::LZ4` and
  `compression.type = lz4`.
- **Produce v2** — `ProduceResponse`/`ProduceResponseV1`/`ProduceResponseV0` with the
  per-partition `LogAppendTime` (`NO_LOG_APPEND_TIME = -1`) between `Offset` and the throttle
  time; the request body of the three versions is identical. `Client::produce()` sends v2.
- **Fetch v2 and v3** — `FetchRequest`/`FetchRequestV2`/`FetchRequestV1`/`FetchRequestV0` and the
  matching responses. Version 2 is the statement "I understand message format v1", which stops the
  broker from converting its answer down to format v0; version 3 (KIP-74) adds the request-level
  `MaxBytes` after `MinBytes` and makes the partition order of the request significant.
  `ConsumerConfig::FETCH_MAX_BYTES` (`fetch.max.bytes`, default 52428800), and
  `KafkaConsumer::poll()` rotates the partitions that returned records to the end of its fetch
  order, as `SubscriptionState.movePartitionToEnd` does in the Java consumer.
- **Metadata v1 and v2** — the nullable topic array (`null` = every topic, `[]` = no topic), the
  broker `Rack`, the topic `IsInternal`, the `ControllerId` of v1 and the `ClusterId` of v2, with
  `MetadataRequest`/`MetadataResponse` (v2), `…V1` and `…V0` and `Node`/`NodeV0`,
  `TopicMetadata`/`TopicMetadataV0`. `Cluster::clusterId()`, `Cluster::controller()` and
  `Cluster::topics(?bool $excludeInternalTopics = null)` expose them, and both new fields survive
  the metadata cache file.
- **Offsets (ListOffset) v1** (KIP-79) — offsets by message timestamp with **one** offset per
  partition and the timestamp of the message that was found; `OffsetsRequest`/`OffsetsResponse`
  are version 1 and the `…V0` classes keep the version 0 frame with its offset array.
  `Consumer\OffsetAndTimestamp`, `Client::fetchTopicPartitionOffsetsForTimes()` and
  `KafkaConsumer::{offsetsForTimes,beginningOffsets,endOffsets}()`.
- **OffsetFetch v2** (KIP-88) — `OffsetFetchRequest::forAllTopics()` and a `null` topic array ask
  the coordinator for every topic the group committed, and the response gains the group-level
  `ErrorCode` after the topics; `OffsetFetchRequestV1`/`V0` refuse a null array as their Java
  counterpart does. `AdminClient::listGroupOffsets()` takes the shape of `main` again.
- **JoinGroup v1** (KIP-62) — the `RebalanceTimeout` after the `SessionTimeout`, sent as
  `ConsumerConfig::MAX_POLL_INTERVAL_MS` (`max.poll.interval.ms`, default 300000);
  `Client::joinGroup()` takes it as its last, optional argument.
- **CreateTopics (key 19, v0 and v1) and DeleteTopics (key 20, v0)** — `Admin\NewTopic` (partitions
  and replication factor, or an explicit replica assignment, plus topic configs),
  `AdminClient::createTopics()` with `validateOnly` of version 1, `AdminClient::deleteTopics()` and
  `AdminClient::findController()`, which reads the `controller_id` of a Metadata answer and repeats
  a request once against a freshly looked up controller when a topic answers 41 (`NotController`).
  Both methods return one entry per requested topic: `null` or the exception of its error code,
  with the `error_message` of CreateTopics v1 in the context.
- **SASL/PLAIN** (KIP-43) — `SaslHandshakeRequest`/`SaslHandshakeResponse` (key 17, v0), the raw
  token exchange that follows it on the same socket, `Common\Security\{SaslMechanism,SaslToken}`,
  the client-side `SaslAuthenticationException` for a connection the broker closes on wrong
  credentials, and the `security.protocol = SASL_PLAINTEXT` / `SASL_SSL` transports with
  `sasl.mechanism`, `sasl.username` and `sasl.password`.
- **The group state `Empty`** — `DescribeGroupResponseMetadata::STATE_EMPTY`, the fifth state of a
  0.10.1 coordinator: a group whose last member left keeps its committed offsets instead of being
  dropped.
- **Wire vectors of everything the line adds** — `api-versions.json`, `message-format.json`,
  `create-topics.json`, `delete-topics.json`, `sasl-handshake.json` and the new frames in
  `metadata.json`, `produce.json`, `fetch.json`, `offsets.json`, `offset-fetch.json` and
  `join-group.json`, all captured from the container and replayed by
  `tests/Compliance/ProtocolVectorTest`; `DocumentationSyncTest` additionally checks that every
  `@see docs/protocol/1.1.md, section "…"` of the sources names a heading that exists.
- **Examples** — [`examples/create-topic.php`](examples/create-topic.php),
  [`examples/offsets-for-times.php`](examples/offsets-for-times.php) and
  [`examples/sasl.php`](examples/sasl.php).

### Changed

- **Breaking: `AdminClient::listOffsets()` lost its `$maxNumberOfOffsets` parameter** and answers
  `topic => partition => offset` instead of `topic => partition => [offsets]`. Version 1 of the
  api returns exactly one offset per partition; a caller that wants the old segment-timestamp
  behaviour builds an `OffsetsRequestV0` itself. `Client::fetchTopicPartitionOffsets()` changed the
  same way.
- **Breaking: `AdminClient::listGroupOffsets(string $groupId, ?iterable $topicPartitions = null)`** —
  the partitions are optional now, because OffsetFetch v2 can ask for every topic of the group.
- **Breaking: the consumer defaults are those of the Java consumer of 0.10.1** —
  `session.timeout.ms` **10000** (was 30000), `request.timeout.ms` **305000** (was 40000) and the
  new `max.poll.interval.ms` 300000. `subscribe()` refuses a `request.timeout.ms` that does not
  exceed *both* `session.timeout.ms` and `max.poll.interval.ms`, because a JoinGroup blocks the
  connection for the whole rebalance.
- **Breaking: `Common\Record\Record` and `Consumer\ConsumerRecord` gained two constructor
  parameters**, `?int $timestamp = null` and `int $timestampType = TimestampType::NO_TIMESTAMP_TYPE`,
  at the end of their signatures; `Producer\RecordMetadata::$timestamp` is filled now (the create
  time of the batch, or the `LogAppendTime` the broker answered) where the 0.9 line left it `null`.
- **Breaking: `Client::joinGroup()` takes a `?int $rebalanceTimeoutMs = null`** as its last
  argument (`null` = the configured `max.poll.interval.ms`), and `Client` sends Produce v2, Fetch
  v3, Offsets v1, Metadata v2, OffsetFetch v2 and JoinGroup v1 instead of the 0.9 versions.
- **The protocol document is `docs/protocol/1.1.md`** and describes Kafka 0.10.2.2: the api-key
  table is the literal ApiVersions answer of the broker, one section per api of the line, the error
  table runs to 44, and "Broker quirks and observations" collects every behaviour the integration
  suite established. `docs/protocol/0.9.0.md` stays on the `0.9.x` branch.
- **An api the broker does not serve now closes the connection.** A 0.9.0.1 broker dropped a frame
  it could not parse and kept the connection open; a 0.10.2.2 broker closes the socket, for an
  unknown api key, for a version it does not serve, and for a body that does not match the schema
  of a version it does serve. A client sees the end of the stream, i.e. a `NetworkException`, not
  a request timeout. `tests/Integration/{ApiVersionProbeTest,ProtocolFramingTest}` and
  `OffsetsCoordinatorTest` were rewritten for it, and `RawApiProbe::CLOSED` replaces
  `RawApiProbe::SILENT` in every expectation.
- **ControlledShutdown v0 is retired but still parsed.** A 0.10.2.2 broker reports
  `minVersion = 1` for key 7, because version 0 uses a request header without a client id; the
  frame is nevertheless still accepted, because key 7 is the last api the broker parses with a
  Scala class that never looks at the version. `ControlledShutdownRequestV0` is kept for the
  `0.8.x`/`0.9.x` vectors, and `AdminClient::controlledShutdown()` keeps sending v1.
- **A group whose last member leaves survives as `Empty`**, keeps its committed offsets until
  `offsets.retention.minutes` expires them, is still listed by ListGroups and answers JoinGroup so
  that a new member can take it over. A 0.9.0.1 coordinator dropped such a group at once and
  answered `Dead`, which made "everybody left" and "never existed" indistinguishable.
- **A broker without topics answers Metadata with its brokers.** The empty broker array of a fresh
  0.8/0.9 cluster is gone; an empty broker array still means "not ready, retry" and the readiness
  probe of the test suite is unchanged, but it is no longer the normal state of a new cluster.
- **`AdminClient::findController()` reads the `controller_id` of a Metadata v1/v2 answer** instead
  of probing every broker with a request only the controller answers.
- **`Consumer\RecordTooLargeException` cannot be raised by a Fetch v3 answer any more**: the broker
  returns the first message of a partition whole even when it exceeds the limits (KIP-74), so
  `FetchResponsePartition::isSingleMessageTooLarge()` is only asked for the versions 0 to 2.
- **README, CHANGELOG and the examples describe the 0.10.x line**, with a compatibility matrix per
  api and version over the three protocol branches, a configuration reference for every option, and
  the four listeners of the integration suite.

Previous line — 0.9.x (Kafka 0.9.0.1)
-------------------------------------

Everything a Kafka 0.9.0.1 broker speaks, built on top of the `0.8.x` line it was merged from: the six group
apis the release added, the higher versions of the four apis it raised, the consumer group protocol with its
assignors, transport security, and the throttle time that client quotas report. Every wire format below was
verified against a real 0.9.0.1 broker and is documented byte for byte in
[docs/protocol/0.9.0.md](docs/protocol/0.9.0.md).

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
- **The versions that 0.9 raised**, each of them a `getScheme()` that follows the `VERSION` constant of its class,
  with a class per layout as in the `OffsetCommitRequest`/`OffsetCommitRequestV0` pattern:
  * **Produce v1** — the response gains `ThrottleTime` int32 *after* the topics (`ProduceRequestV0`/`ProduceResponseV0`
    keep version 0);
  * **Fetch v1** — the response gains `ThrottleTimeMs` int32 *before* the topics (`FetchRequestV0`/`FetchResponseV0`);
  * **OffsetCommit v2** — a global `RetentionTime` int64 after the member id replaces the per-partition `TimeStamp`,
    so a v2 partition entry has the layout of a v0 one again (`OffsetCommitRequestV1`, `OffsetCommitRequestPartitionV1`,
    `OffsetCommitRequestTopicV1`); `OffsetCommitRequest::DEFAULT_RETENTION_TIME` (-1) asks for the retention of the
    broker;
  * **ControlledShutdown v1** — the request finally carries the client id in the common header
    (`ControlledShutdownRequestV0` drops it from the scheme again).

  `Client` sends the highest version a 0.9.0.1 broker serves — Produce v1, Fetch v1, OffsetCommit v2 for
  `offsets.storage = kafka` — and `AdminClient::controlledShutdown()` sends v1.
- **Group membership protocol** — `JoinGroupRequest`/`Response`, `SyncGroupRequest`/`Response`,
  `HeartbeatRequest`/`Response`, `LeaveGroupRequest`/`Response` (all v0) with the DTOs
  `JoinGroupRequestProtocol`, `JoinGroupResponseMember` and `SyncGroupRequestMember`, and
  `Client::{joinGroup,syncGroup,heartbeat,leaveGroup}` with the signatures of `main`. The three coordinator errors
  14, 15 and 16 are retried by `Network\RetryPolicy`; 22, 23, 25, 26, 27 and 30 describe *this member* and reach
  the caller as their exception class. The `rebalance_timeout` of JoinGroup v1 and the throttle time of SyncGroup
  v1 are Kafka 0.10.1 and are deliberately absent.
- **Consumer group protocol (`protocol_type = "consumer"`)** — `Consumer\Subscription` and
  `Consumer\MemberAssignment`, the two structures that JoinGroup and SyncGroup carry as opaque
  byte arrays (`ConsumerProtocol` @ 0.9.0.1), declared on the schema engine with `pack()` and
  `unpack()` helpers, and the assignors that fill them: `Consumer\RangeAssignor` (the default of
  the Java client) and `Consumer\RoundRobinAssignor`, both with the ordering rules of Java, so a
  PHP member may lead a group of Java members. `Consumer\AbstractPartitionAssignor::fromStrategy()`
  resolves the `partition.assignment.strategy` option - a wire name or the class of a custom
  `Consumer\PartitionAssignorInterface`. Byte-exact vectors captured from the members of Java
  0.9.0.1 consumer groups (`docs/protocol/vectors/consumer-protocol.json`).
- **`KafkaConsumer` joins consumer groups** — `subscribe()` (optionally with a
  `Consumer\ConsumerRebalanceListener`), `unsubscribe()` and `subscription()`, driven by
  `Consumer\Internals\ConsumerCoordinator` and `Consumer\Internals\SubscriptionState`: the first `poll()` joins the
  group (JoinGroup → the assignor of the leader → SyncGroup → the positions of the committed offsets), every
  further `poll()` sends the heartbeat when `heartbeat.interval.ms` has elapsed — PHP has no background thread —
  and rejoins on 27, 25 or 22, `commitSync()` and the auto-commit carry the member id and the generation, and
  `close()` commits once more and leaves the group with a LeaveGroup request. `assign()` still picks partitions by
  hand and joins no group; the two are mutually exclusive, as in the Java client.
- **Admin group apis** — `ListGroupsRequest`/`Response` and `DescribeGroupsRequest`/`Response` (v0, with
  `ListGroupResponseProtocol`, `DescribeGroupResponseMetadata` and `DescribeGroupResponseMember`) and
  `Admin\AdminClient::{listGroups,listAllGroups,describeGroup,describeGroups}`. A broker only knows the groups it
  coordinates, so `listAllGroups()` merges the answers of every broker and `describeGroups()` asks the coordinator
  of each group. An unknown group is not an error: it is answered with the state `Dead` and the error code 0.
- **SSL transport** — `Common\Security\SecurityProtocol` and `Common\Security\SslProtocol` plus the
  `ClientConfig` options `security.protocol`, `ssl.protocol`, `ssl.enabled.protocols`, `ssl.ca.cert.location`,
  `ssl.client.cert.location`, `ssl.key.location` and `ssl.key.password`. `IO\SocketStream` builds the stream
  context and enables crypto right after `connect()`, because a broker speaks TLS from the first byte of a
  connection to its SSL listener; the certificate of the broker is always verified. Kafka 0.9 has one listener per
  security protocol and Metadata v0 answers with the endpoint of the listener the request arrived on, so a client
  that bootstraps over TLS discovers the TLS endpoints of the whole cluster. `SASL_PLAINTEXT` and `SASL_SSL` are
  refused with an explanation: SASL in 0.9 is GSSAPI-only and negotiated outside the protocol, and `SaslHandshake`
  is api key 17 of Kafka 0.10.
- **Throttle time of the client quotas that Kafka 0.9 introduced** — `Producer\RecordMetadata::$throttleTimeMs`
  (every promise of `KafkaProducer::send()` resolves with it) and `Common\FetchedPartition::$throttleTimeMs` of
  `Client::fetchPartitions()`. A quota never rejects anything: the broker appends or reads and only delays the
  answer, by the number of milliseconds the response then reports.
- **Configuration**: `ConsumerConfig::{SESSION_TIMEOUT_MS, HEARTBEAT_INTERVAL_MS, PARTITION_ASSIGNMENT_STRATEGY,
  OFFSET_RETENTION_MS}` and the `ssl.*`/`security.protocol` options above.
- **Documentation and examples**: [docs/protocol/0.9.0.md](docs/protocol/0.9.0.md) is the 0.9.0.1 grammar with a
  section per api, the api-key and error tables, the broker quirks of the release and 62 annotated wire vectors;
  the same vectors are machine-readable in [docs/protocol/vectors](docs/protocol/vectors) and replayed by
  `tests/Compliance`. New runnable examples: [`examples/consumer-group.php`](examples/consumer-group.php) and
  [`examples/ssl.php`](examples/ssl.php).

### Changed

- **Error code 13 is `NetworkException`** (`ServerExceptionInterface`, retriable), as on `main`.
  It was `StaleLeaderEpochCode` in `kafka/common/ErrorMapping.scala` @ 0.8.2.2 and no broker ever
  sent it; `StaleLeaderEpochException` is therefore gone and the class the socket layer raises for
  a dropped connection now carries the wire code 13.
- **`Client::commitGroupOffsets()` takes the signature of `main`**:
  `(Node $coordinatorNode, string $groupId, string $memberId, int $generationId, array $topicPartitionOffsets,
  int $retentionTimeMs): void`, so a commit carries the membership of the consumer and the retention of
  OffsetCommit v2.
- **`ControlledShutdownRequest::__construct()`** takes the client id as its second argument, as on `main`:
  version 1 of the api puts it into the header.
- **`request.timeout.ms` defaults to 40000 for a consumer** (the general default stays 30000), as in the Java
  consumer of 0.9.0.1, and `subscribe()` refuses a value that does not exceed `session.timeout.ms`: the
  coordinator holds a JoinGroup until the whole rebalance is over, so a shorter socket timeout could never
  survive a rebalance.
- **`Network\ConnectionFactory` keys its connection cache by the security protocol as well**, so an encrypted
  connection is never handed out to a caller that asked for a plaintext one. The key of a plaintext connection is
  unchanged (`tcp://host:port`).
- **The properties of `ProduceRequest` and `FetchRequest` are `protected`** instead of `private`, so that the
  version 0 subclasses of both can reuse them.
- **OffsetFetch v1 for a partition the cluster does not host** answers offset `-1` with the error
  code `0` on a 0.9.0.1 broker, where 0.8.2.2 answered 3 (`UnknownTopicOrPartition`): version 1
  stopped filtering the requested partitions against the metadata cache. The integration suite and
  the protocol document record it.
- **ControlledShutdown for a broker id the controller does not know** answers 8 (`BrokerNotAvailable`), where
  0.8.2.2 leaked -1 (`Unknown`): `ControlledShutdownRequest.handleError()` @ 0.9.0.1 maps `e.getClass` instead of
  `e.getCause`. The `controlledshutdown.response.v0` vector was re-captured on a 0.9.0.1 broker for it.
- **`docs/protocol/0.9.0.md`** now describes the Kafka 0.9.0.1 grammar: the api-key table with the
  probed versions, the error table -1 … 31, what a 0.9 broker does with an api it does not serve
  (it drops the request silently and keeps the connection open), which apis do not validate the
  version they are sent with, and what is *not* in 0.9.
- README and CHANGELOG follow the `0.9.x` line; the compatibility matrix lists the api keys 0-16.

### Removed

- `StaleLeaderEpochException` and the `STALE_LEADER_EPOCH` constant, whose code 13 belongs to
  `NetworkException` from Kafka 0.9 onwards.

Previous line — 0.8.x (Kafka 0.8.2.2)
-------------------------------------

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
