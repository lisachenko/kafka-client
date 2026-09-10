# Handoff: the `main` line (Kafka 1.x)

State at handoff: the `0.11.x` line is **complete**. Everything a Kafka 0.11.0.3 broker speaks is implemented on the
`BinarySchema` engine and verified against a real broker — 1487 unit tests, 236 compliance tests replaying the 229
wire vectors of the four lines (120 inherited, 109 captured on the 0.11.0.3 container), and 426 integration tests
against the 0.11.0.3 container over its four listeners, without a single skip. The grammar is written down in
[`docs/protocol/0.11.0.md`](../protocol/0.11.0.md), the machine-readable frames in
[`docs/protocol/vectors`](../protocol/vectors), the behaviour a client cannot read out of the grammar in that
document's "Broker quirks and observations" section, and the record of how the line was built — what was decided,
what the broker forced, what was left out — in [`docs/handoff/0.11.x.md`](0.11.x.md).

`main` is the **1.x line**. It was identical to `0.11.x` at the branch point (`64d767c`, the merge of PR #87) and
receives everything that lands on `0.11.x` through the cascade (`.github/workflows/cascade.yml` opens the PR on
every push to `0.11.x`; rules in [`docs/CASCADE.md`](../CASCADE.md)). Unlike every line before it, this one starts
from a finished **schema-based** implementation: there is nothing to drop and nothing to re-implement, every class
of 0.11 is the class the 1.x version of the same api extends, and rule 4 of `CLAUDE.md` simply means "start from
the 0.11 class and add what the new release adds".

Read `CLAUDE.md` first (hard rules, toolchain, the Composer/phpstan sandbox workaround, Docker, the JIT caveat).

## First decision for the owner: which Kafka release the line speaks

Every line below speaks the **last** release of its Kafka line (0.8.2.2, 0.9.0.1, 0.10.2.2, 0.11.0.3). On that
principle the 1.x line speaks **Kafka 1.1.1**, and everything below is written for it. The 1.x line raised the
protocol twice, and a 1.1.1 broker serves both raises:

* **1.0.0** (1.0.1 and 1.0.2 changed no wire format) — Produce v4/v5, Fetch v6, Metadata v5, SaslHandshake v1 and
  **SaslAuthenticate** (36), **AlterReplicaLogDirs** (34), **DescribeLogDirs** (35), **CreatePartitions** (37), the
  error codes 56–60, the five-batch deduplication window of the idempotent producer, and `log.message.format.version`
  1.0 (still magic 2).
* **1.1.0** (1.1.1 changed no wire format) — Fetch v7 with the **incremental fetch sessions** of KIP-227,
  DescribeConfigs v1 and the **dynamic broker configuration** of KIP-226, the **delegation tokens** (38–41) of
  KIP-48, **DeleteGroups** (42) of KIP-229, the error codes 61–71.

If the owner prefers "1.0.2 only", strike the 1.1.0 row everywhere below: Fetch stops at v6, DescribeConfigs at v0,
`ApiKeys` at 37, `KafkaException` at 60 — nothing else in this plan changes. The Kafka repository has the tags
`1.0.2` and `1.1.1` (no `-rc` detour this time), and archive.apache.org has `kafka_2.11-1.1.1.tgz`, so the Scala
2.11 image recipe of the 0.11 line works unchanged.

## What Kafka 1.1.1 adds over 0.11.0.3

Verified field by field against the Kafka sources at the tags **1.0.2** and **1.1.1** (`git clone --depth 1
--branch 1.1.1 https://github.com/apache/kafka`): `clients/src/main/java/org/apache/kafka/common/protocol/{ApiKeys,Errors}.java`
and — because `Protocol.java` stopped being the schema authority in 1.0 — the `schemaVersions()` of every class in
`clients/src/main/java/org/apache/kafka/common/requests/`. As always the broker of the line has the last word, and
this line can ask it: the served api table of a 1.1.1 container is in the section "The ApiVersions answer of a
1.1.1 broker" below.

| Api | 0.11.0.3 | 1.1.1 | Since | Start from |
|---|---|---|---|---|
| Produce (0) | v0–v3 | **v4**: the same frame as v3 (`PRODUCE_REQUEST_V4 = PRODUCE_REQUEST_V3`, `PRODUCE_RESPONSE_V4 = PRODUCE_RESPONSE_V3`); the version says that the client understands the error code 56 `KAFKA_STORAGE_ERROR`. **v5**: the response partition gains `log_start_offset` int64 after `log_append_time` — the field the 0.11 plan wrongly expected on v3; a producer uses it to tell a spurious `OutOfOrderSequence` (its records were deleted below the log start offset) from a real one | 1.0 | `ProduceRequest`/`ProduceResponse` (+ `…V<n>` subclasses), `ProduceResponsePartition` |
| Fetch (1) | v0–v5 | **v6**: the same frame as v5 (`FETCH_REQUEST_V6 = FETCH_REQUEST_V5`, response likewise); says that the client understands 56. **v7** (KIP-227, incremental fetch sessions): the request gains `session_id` int32 and `epoch` int32 after `isolation_level` and before `topics`, and a trailing `forgotten_topics_data [topic [partition]]`; the response gains a top-level `error_code` int16 and `session_id` int32 after `throttle_time_ms`. `session_id = 0, epoch = -1` is the session-less full fetch of every older version, which the broker still serves — a client may implement the frame and keep sending full fetches, or implement sessions in the consumer (below) | v6 1.0, v7 1.1 | `FetchRequest`/`FetchResponse`, `FetchRequestTopicPartition`, `FetchedPartition`, `KafkaConsumer` |
| Metadata (3) | v0–v4 | **v5**: every partition gains `offline_replicas [int32]` after `isr` (KIP-112/113, JBOD); request unchanged | 1.0 | `MetadataRequest`/`MetadataResponse`, `Common\PartitionMetadata` |
| Offsets (2), OffsetCommit (8), OffsetFetch (9), GroupCoordinator (10), JoinGroup (11), Heartbeat (12), LeaveGroup (13), SyncGroup (14), DescribeGroups (15), ListGroups (16), ApiVersions (18), CreateTopics (19), DeleteTopics (20), DeleteRecords (21), InitProducerId (22), OffsetForLeaderEpoch (23), the transaction apis (24–28), AlterConfigs (33) | – | **unchanged** — same versions, same frames. AlterConfigs is unchanged on the wire but a 1.1 broker accepts a **broker** resource for the dynamic configuration options of KIP-226 (0.11 answered 42 for every broker resource) | – | – |
| SaslHandshake (17) | v0 | **v1**: the same frame; a client that sends v1 promises to continue with **SaslAuthenticate** requests instead of the raw, unframed token exchange, and in return receives an error code for a failed authentication instead of a closed connection | 1.0 | `SaslHandshakeRequest`, the SASL path of `IO\SocketStream` |
| ControlledShutdown (7) | v1 | unchanged (v0 still parsed, version still ignored — re-verify) | – | – |
| DescribeConfigs (32) | v0 | **v1** (KIP-226): the request gains `include_synonyms` boolean after the resources; in the response entry `is_default` boolean is **replaced** by `config_source` int8 (0 unknown, 1 topic, 2 dynamic broker, 3 dynamic default broker, 4 static broker, 5 default) and the entry gains `config_synonyms [config_name config_value config_source]` | 1.1 | `DescribeConfigsRequest`/`Response`, `Admin\ConfigEntry`, `AdminClient::describeConfigs()` |
| **AlterReplicaLogDirs (34)** | – | new (KIP-113): `[log_dir [topic [partition]]]` → `throttle_time_ms [topic [partition error_code]]`; sent to the broker that hosts the replicas; needs a broker with more than one `log.dirs` entry to do anything, otherwise 57 `LOG_DIR_NOT_FOUND` | 1.0 | nothing; `ApiKeys::ALTER_REPLICA_LOG_DIRS` |
| **DescribeLogDirs (35)** | – | new (KIP-113): `[topic [partition]]` (nullable = every partition) → `throttle_time_ms [error_code log_dir [topic [partition size offset_lag is_future]]]`; broker-local | 1.0 | nothing |
| **SaslAuthenticate (36)** | – | new (KIP-152): `sasl_auth_bytes` bytes → `error_code error_message sasl_auth_bytes`; carries the SASL token exchange as ordinary framed requests after a SaslHandshake **v1**; a failed PLAIN authentication answers 58 `SASL_AUTHENTICATION_FAILED` with a message where 0.11 closed the socket | 1.0 | `SaslHandshakeRequest`, `Common\Security\SaslToken`, the SASL code of `SocketStream`, `tests/Integration/SaslTransportTest.php` |
| **CreatePartitions (37)** | – | new (KIP-195): `[topic count assignment(nullable [[int32]])] timeout validate_only` → `throttle_time_ms [topic error_code error_message]`; controller-only, like CreateTopics (41 `NOT_CONTROLLER` otherwise); cannot shrink a topic (37 `INVALID_PARTITIONS`) | 1.0 | `CreateTopicsRequest` for the shape, `AdminClient::createTopics()` for the controller lookup |
| **CreateDelegationToken (38), RenewDelegationToken (39), ExpireDelegationToken (40), DescribeDelegationToken (41)** | – | new (KIP-48): tokens issued to a SASL principal; refused on PLAINTEXT and one-way-SSL channels with 64 `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED`, and disabled with 61 unless the broker has a `delegation.token.master.key`; *using* a token means authenticating with SASL/**SCRAM**, which this client does not have (PLAIN only) | 1.1 | nothing; see the open question |
| **DeleteGroups (42)** | – | new (KIP-229): `[group]` → `throttle_time_ms [group error_code]`; to the group coordinator; 68 `NON_EMPTY_GROUP` for a group with members, 69 `GROUP_ID_NOT_FOUND` for an unknown one | 1.1 | `DescribeGroupsRequest` for the shape, `AdminClient::describeGroups()` for the coordinator lookup |
| LeaderAndIsr (4) v1, UpdateMetadata (6) v4 | v0 / v0–v3 | broker→broker, not implemented (the probe still sends them with a stale epoch and expects 11) | 1.0 | – |
| Message format | v2 | **unchanged** — magic 2, no new attribute bit; `log.message.format.version` defaults to 1.1 and a 1.1.1 broker still down-converts for Fetch v0–v3 exactly as 0.11 did | – | `Common\Record\*` untouched |
| Error codes | −1 … 55 | **56** `KAFKA_STORAGE_ERROR` (retriable, `InvalidMetadataException`), **57** `LOG_DIR_NOT_FOUND`, **58** `SASL_AUTHENTICATION_FAILED` (`AuthenticationException`), **59** `UNKNOWN_PRODUCER_ID` (extends `OutOfOrderSequenceException`), **60** `REASSIGNMENT_IN_PROGRESS`; **61** `DELEGATION_TOKEN_AUTH_DISABLED`, **62** `DELEGATION_TOKEN_NOT_FOUND`, **63** `DELEGATION_TOKEN_OWNER_MISMATCH`, **64** `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED`, **65** `DELEGATION_TOKEN_AUTHORIZATION_FAILED`, **66** `DELEGATION_TOKEN_EXPIRED`, **67** `INVALID_PRINCIPAL_TYPE`, **68** `NON_EMPTY_GROUP`, **69** `GROUP_ID_NOT_FOUND`, **70** `FETCH_SESSION_ID_NOT_FOUND` (retriable), **71** `INVALID_FETCH_SESSION_EPOCH` (retriable). Only 56, 70 and 71 extend `RetriableException` in the Java client | 56–60 1.0, 61–71 1.1 | `Common/Errors/*`, `KafkaException::$codeToClassMap` and its test, which pins the range 1–55 |
| Producer | idempotent, transactional | the broker keeps the **last five batches** per producer id and partition (`ProducerStateManager.NumBatchesToRetain = 5` @ 1.0.2 and 1.1.1; 0.11 kept one), so a duplicate of any of them is answered as the original append and the "duplicate of an older batch is 45" quirk of 0.11 is gone; **59 `UNKNOWN_PRODUCER_ID`** arrives when the broker lost the producer's state (its records fell below the log start offset) and the 1.x Java producer answers it by resetting the sequence numbers of that partition when `log_start_offset` of the Produce v5 answer shows why; `max.in.flight.requests.per.connection` up to 5 with idempotence is a Java-client concern, this client sends one request at a time | 1.0 | `Producer\Internals\TransactionManager`, `Client::produce()` |
| Consumer | `read_committed` | **incremental fetch sessions** (optional, see Fetch v7): a consumer that keeps a session sends only the partitions whose fetch offset changed and gets back only the partitions that have data; 70/71 mean "start a new full fetch". Offline replicas in the metadata | 1.1 | `KafkaConsumer`, `Consumer\Internals\SubscriptionState`, `Client::fetchPartitions()` |
| Admin | topics, configs, records, groups | `createPartitions()`, `deleteConsumerGroups()` (the Java name), `describeLogDirs()`, `alterReplicaLogDirs()`, the config synonyms and sources, dynamic broker configuration through `alterConfigs()` (`describeConfigs()` of a broker resource answers the dynamic and static values with their source) | 1.0 / 1.1 | `Admin\AdminClient`, `Admin\Config*` |
| Transport | PLAINTEXT, SSL, SASL_PLAINTEXT/SASL_SSL with PLAIN | the same, with the framed `SaslAuthenticate` exchange after a v1 handshake and a real error code for wrong credentials | 1.0 | `SocketStream`, `Common\Security\*` |

Three engine-level facts, all of them good news: no new primitive type (the wire of 1.x is int8/16/32/64,
boolean, string, nullable string, bytes, arrays and the zigzag varints of the record batch, all of which the engine
has), no new message format, and the request header is still `api_key api_version correlation_id client_id`
(the flexible versions with tagged fields are Kafka 2.4). Everything in 1.x is "one more `…V<n>` subclass, one
more field guarded by `static::VERSION`, one more request class" on the pattern of the 0.11 line.

## The ApiVersions answer of a 1.1.1 broker

The literal answer of a Kafka 1.1.1 container (the image recipe above, built in the scratchpad of the 0.11 session
and probed with a raw ApiVersions v0 frame on 2026-09-10): `errorCode = 0`, **43 apis**, keys 0 to 42. It matches
the `ApiKeys`/`schemaVersions()` of the sources at the tag 1.1.1 in every row, with one difference from the 0.11.0.3
answer that is not a new version: **ControlledShutdown (7) is served as v0–v1 again** (a 0.11.0.3 broker reported
`MinVersion = 1`), so the "key 7 is the one row whose minimum is not 0" statement of the 0.11 document is no longer
true and `ApiVersionProbeTest::SERVED_APIS` changes in that row as well. `inter.broker.protocol.version` and
`log.message.format.version` of the container are `1.1-IV0`.

| ApiKey | Name | Min | Max | 0.11.0.3 | What changed |
|---|---|---|---|---|---|
| 0 | Produce | 0 | 5 | 0–3 | v4, v5 |
| 1 | Fetch | 0 | 7 | 0–5 | v6, v7 |
| 2 | ListOffsets | 0 | 2 | 0–2 | – |
| 3 | Metadata | 0 | 5 | 0–4 | v5 |
| 4 | LeaderAndIsr | 0 | 1 | 0 | broker→broker |
| 5 | StopReplica | 0 | 0 | 0 | – |
| 6 | UpdateMetadata | 0 | 4 | 0–3 | broker→broker |
| 7 | ControlledShutdown | **0** | 1 | 1 | v0 served again |
| 8 | OffsetCommit | 0 | 3 | 0–3 | – |
| 9 | OffsetFetch | 0 | 3 | 0–3 | – |
| 10 | FindCoordinator | 0 | 1 | 0–1 | – |
| 11 | JoinGroup | 0 | 2 | 0–2 | – |
| 12–16 | Heartbeat, LeaveGroup, SyncGroup, DescribeGroups, ListGroups | 0 | 1 | 0–1 | – |
| 17 | SaslHandshake | 0 | 1 | 0 | v1 |
| 18 | ApiVersions | 0 | 1 | 0–1 | – |
| 19 | CreateTopics | 0 | 2 | 0–2 | – |
| 20 | DeleteTopics | 0 | 1 | 0–1 | – |
| 21–28 | DeleteRecords, InitProducerId, OffsetForLeaderEpoch, the transaction apis | 0 | 0 | 0 | – |
| 29–31 | DescribeAcls, CreateAcls, DeleteAcls | 0 | 0 | 0 | – (still 54 without an authorizer) |
| 32 | DescribeConfigs | 0 | 1 | 0 | v1 |
| 33 | AlterConfigs | 0 | 0 | 0 | broker resources accepted |
| 34 | AlterReplicaLogDirs | 0 | 0 | – | new |
| 35 | DescribeLogDirs | 0 | 0 | – | new |
| 36 | SaslAuthenticate | 0 | 0 | – | new |
| 37 | CreatePartitions | 0 | 0 | – | new |
| 38 | CreateDelegationToken | 0 | 0 | – | new |
| 39 | RenewDelegationToken | 0 | 0 | – | new |
| 40 | ExpireDelegationToken | 0 | 0 | – | new |
| 41 | DescribeDelegationToken | 0 | 0 | – | new |
| 42 | DeleteGroups | 0 | 0 | – | new |

Raw frame of the answer, for the re-capture of `apiversions.response.v0` (client id `apiversions-probe`, correlation id 1):

```
0000010c0000000100000000002b000000000005000100000007000200000002000300000005000400000001000500000000000600000004000700000001000800000003000900000003000a00000001000b00000002000c00000001000d00000001000e00000001000f00000001001000000001001100000001001200000001001300000002001400000001001500000000001600000000001700000000001800000000001900000000001a00000000001b00000000001c00000000001d00000000001e00000000001f00000000002000000001002100000000002200000000002300000000002400000000002500000000002600000000002700000000002800000000002900000000002a00000000
```

## Baseline: the inherited suite of 0.11.x against a 1.1.1 broker

Run on 2026-09-10 with the `0.11.x` tree at `64d767c` against the 1.1.1 container above, all four listeners:
unit + compliance untouched (a broker is not involved), integration **426 tests, 21 failures, 0 skips**. Every
failure is an expectation written for a 0.11.0.3 broker; all of them belong to T1 unless the row says otherwise,
and each is a behaviour the 1.x document has to record:

| Failing test | What the 1.1.1 broker does | Owner |
|---|---|---|
| `ApiVersionProbeTest` (6 + 3): `SERVED_APIS`, the two table tests, and "closes the connection" for Produce v4, Fetch v6, Metadata v5, LeaderAndIsr v1, UpdateMetadata v4, SaslHandshake v1 | serves the 43-key table above; the providers derive from `SERVED_APIS`, so the table is the only edit | T1 |
| `ApiVersionProbeTest::testControlledShutdownAnswersEveryVersionItIsSent` (v2 above the table) and `AdminApiTest::testTheBrokerAnnouncesOnlyVersionOneOfControlledShutdown` | **ControlledShutdown is an ordinary Java-schema api now**: v0 is served (`MinVersion = 0`) and a version above the table **closes the connection** like every other api — the "last Scala api does not check its version" section of the 0.11 document is history | T1 |
| `AdminGroupApiTest::testDescribeGroupReportsAwaitingSyncWhileTheLeaderHasNotPublishedTheAssignment` | the group state **`AwaitingSync` is called `CompletingRebalance`** (1.0); `DescribeGroups` answers the new name, so the constant, the document's group-state list and the test move | T1 |
| `TopicAdminApiTest::testATopicWithoutPartitionsIsRefused` | the message is `Number of partitions must be larger than 0.` (capital N, trailing dot), same code 37 | T1 |
| `IdempotentProducerTest::testADuplicateOfABatchThatIsNoLongerTheLastOneIsAnOutOfOrderSequence` | the **five-batch window**: a duplicate of a batch that is no longer the last one is answered as the original append, not with 45 (`ProducerStateManager.NumBatchesToRetain = 5`) | T6 (the idempotent producer of 1.x), T1 keeps the suite green by pinning the new answer |
| `FetchApiTest::testAPartitionWhoseRecordsCarryHeadersCanNotBeReadByAFetchBelowVersionFour` | a Fetch below v4 of a partition whose records carry headers is answered with the error code **0** instead of the -1 of 0.11 — a 1.x broker down-converts such a batch (what it does with the headers is for T3 to measure with `DumpLogSegments` and a raw Fetch v3: dropped, or the batch skipped) | T3, T1 pins the answer |
| `ConfigsApiTest` (6): a fresh topic reports entries of its own; the broker resource is no longer read-only throughout; `alterConfigs()` of a topic does not leave exactly the two options; `validate_only` and a null value are not followed by an empty own-options list; a broker resource is no longer refused with the 0.11 message | **KIP-226**: `DescribeConfigs` v0 on a 1.1 broker derives `is_default` from the config *source* (an option whose value comes from the broker's static configuration is not "default" any more), and `AlterConfigs` **accepts a broker resource** and validates it per option (`Cannot update these configs dynamically: Set(log.retention.hours)` for a static option; a dynamic one such as `log.cleaner.threads` is applied). T4 owns the semantics and the v1 of the api; T1 only makes the six tests state what the broker answers | T4 (T1 pins) |

Not failing, worth noting: the whole message-format, transaction, `read_committed`, SASL/PLAIN (raw exchange after
a v0 handshake still works on 1.1.1), quota, DeleteRecords and group-membership coverage of 0.11 passes unchanged,
and the 229 vectors replay unchanged — a 1.1.1 broker speaks every version the four lines captured.

## Environment recipe (the one of the 0.11 line, re-targeted)

1. **Docker**: `nohup dockerd >/tmp/dockerd.log 2>&1 &` if `docker info` fails — and check `docker info` again before
   every gate, the daemon died once in the middle of the 0.11 session and once in the handoff work. Copy
   `docker/kafka-0.11.0.3/` to `docker/kafka-1.1.1/`, set `KAFKA_VERSION=1.1.1` and `KAFKA_DIST=kafka_2.11-1.1.1`
   (the Scala 2.11 tarball exists on archive.apache.org; JDK 8 of the base image is what 1.1 wants), keep the four
   listeners, `jaas.conf`, the `ca/` proxy-CA mechanism and the `transaction.state.log.*=1` settings of `start.sh`
   (a one-broker cluster still needs them), and point `docker-compose.yml` at it (container `kafka-1-1-1`). Check
   whether the `server.properties` of 1.1.1 still ends without a trailing newline — `start.sh` appends one either
   way, keep that. Before building behind the sandbox proxy: `cp /root/.ccr/ca-bundle.crt docker/kafka-1.1.1/ca/proxy-ca.crt`.
   The download of the tarball took six to ten minutes through the proxy in this session.
2. **Broker settings the suite depends on** are those of the 0.11 image. For the new apis add what they need:
   `delegation.token.master.key=<any string>` and `sasl.enabled.mechanisms=PLAIN,SCRAM-SHA-256` if the delegation
   tokens are in scope (a SCRAM user is created with `kafka-configs.sh --zookeeper localhost:2181 --alter
   --add-config 'SCRAM-SHA-256=[password=…]' --entity-type users --entity-name <user>`), and a second `log.dirs`
   entry if AlterReplicaLogDirs should do more than answer 57.
3. **Dependencies**: `tools/dev/vendor-from-source.sh` once per session (about ten minutes), then `cp -a vendor/`
   into every agent worktree; never `composer update` in the sandbox.
4. **Sources**: `git clone --depth 1 --branch 1.1.1 https://github.com/apache/kafka` (and `1.0.2` for the
   attribution of a version to a release). `Protocol.java` is no longer the schema authority in 1.x: the schemas
   are the `schemaVersions()` arrays of `clients/src/main/java/org/apache/kafka/common/requests/*Request.java` and
   `*Response.java`, the api table is `protocol/ApiKeys.java`, the errors `protocol/Errors.java`.
5. **Probe before cutting tickets**: a raw ApiVersions v0 frame (twenty lines of PHP over a socket, or
   `Client::apiVersions()`) gives the served table — paste it into the epic, it is the contract — and the inherited
   integration suite against the new broker gives the baseline failures, all of which belong to T1.
6. **Recreate the broker between waves** (`docker compose down -v && docker compose up -d --wait`); every full
   suite run creates about a hundred topics.

## Ticket plan (waves of up to four Opus agents in isolated worktrees)

Wave 1 — foundation and the two independent pieces
- **T1 broker, surface, docs**: `docker/kafka-1.1.1/`, the protocol document renamed to `docs/protocol/1.1.md`
  with every `@see` updated (rule 7 of `docs/CASCADE.md`; the naming drops the patch level, as `0.11.0.md` did),
  `ApiKeys` 34–42, the error codes 56–71 with their classes and retriable flags, the api-key table pinned by
  `ApiVersionProbeTest` (its providers derive from `SERVED_APIS`, so the table is the only edit), the baseline
  failures, README/CHANGELOG headers. As in the 0.11 line, the coordinator can put `ApiKeys` and the error codes
  into a foundation commit before the wave.
- **T2 SaslHandshake v1 and SaslAuthenticate (36)**: the framed token exchange, the 58 with a message for wrong
  credentials, `SaslTransportTest` over both SASL listeners, the transport section of the document. Independent of
  everything else.
- **T3 Produce v4/v5, Fetch v6/v7, Metadata v5**: the largest ticket — `log_start_offset` on the produce answer,
  `offline_replicas`, the Fetch v7 frame with `session_id`/`epoch`/`forgotten_topics_data` and the top-level error
  code, and the **decision on fetch sessions** (below). Frozen contract: `FetchedPartition` and
  `ProduceResponsePartition` gain fields, nothing loses one.

Wave 2 — the admin apis and the producer semantics
- **T4 DescribeConfigs v1, dynamic broker configs, CreatePartitions (37), DeleteGroups (42)**: `AdminClient`
  methods with the Java names, the synonyms and sources, the broker resource that AlterConfigs now accepts.
- **T5 DescribeLogDirs (35) and AlterReplicaLogDirs (34)**: classes, vectors, `AdminClient::describeLogDirs()`;
  `alterReplicaLogDirs()` verified as far as one log directory allows (57), or against a second `log.dirs` entry.
- **T6 the idempotent producer of 1.x**: the five-batch window, 59 `UNKNOWN_PRODUCER_ID` and the sequence reset it
  triggers, `log_start_offset` in the decision, and the transactional producer re-verified against the new
  coordinator (no wire change).
- **T7 delegation tokens (38–41)** — only if the owner wants them (open question 3).

Wave 3 — the finish
- **T8 fetch sessions in the consumer** if wave 1 decided to implement them (`FetchSessionHandler` of the Java
  client: session id and epoch per broker, the set of partitions the broker knows, `forgotten_topics_data` for
  the ones that left the assignment, a full fetch on 70/71).
- **T9 docs, compliance, README matrix, CHANGELOG, examples, release notes**, and the handoff for the line above
  (Kafka 2.0: flexible versions are still far away, 2.0 is Produce v6, Fetch v8, OffsetCommit v4 … and the
  removal of the zookeeper-based admin paths).

Every ticket: byte-exact vectors from the real broker, integration tests with unique topic/group/transactional-id
names, the whole gate green before the PR, and a PR against the integration branch with "Closes #n". The
coordinator merges each PR locally with a merge commit, runs the whole gate against the broker, pushes, closes the
issue by hand; the line goes to `main` as one PR at the end — but note that this time the integration branch is
cut from `main` **and** every fix that lands on `0.11.x` in the meantime arrives through a cascade PR, which has
to be merged into the integration branch before the final PR.

## Pitfalls of the 0.11 session (do not rediscover them)

* **The gate script must not `cd` into its own directory.** The first version of the coordinator's `gate.sh` did
  `cd "$(dirname "$0")"` and silently gated the scratchpad; run it with an explicit worktree path and check that the
  file counts it prints are the repository's.
* **`pkill -f <pattern>` kills the shell that runs it** when the pattern appears in that shell's own command line;
  three coordinator commands died that way. Use `ps aux | grep "[p]attern"` to look and a pid to kill.
* **Run `docker compose down -v` and `docker compose up -d --wait` as separate commands** from anything else; a
  daemon that dies leaves `docker info` failing and the compose commands hanging.
* **`subscribe_pr_activity` on every agent PR floods the coordinator with events**; the agents subscribe
  themselves when they open a PR, so unsubscribe after the merge and stop a finished agent (`TaskStop`) — its
  leftover sleep timers keep re-waking otherwise.
* **Identifiers of a previous design are a rule, not a suggestion.** The foundation commit re-invented the varint
  types under new names and had to be re-done to the identifiers the pre-schema `main` had (`ByteUtils`,
  `Stream::readVarint()`, `TYPE_VARINT_ZIGZAG` …). For 1.x that trap is gone — the reference is the 0.11 code
  itself — but the equivalent one exists: the Java client's names for what is new (`createPartitions`,
  `deleteConsumerGroups`, `describeLogDirs`, `ConfigSource`, `FetchSessionHandler`).
* **The broker answers, not the plan.** Five statements of the 0.11 handoff were wrong and the broker corrected
  them (ApiVersions v1 exists; a Produce v3 answer is the v2 frame; Produce v3 accepts magic 2 only; the
  CreateTopics config value is nullable in every version; error 46 is unreachable). Expect the same rate here:
  the table above is derived from the sources, and each ticket verifies its own rows.
* **Timestamps in tests come from the clock.** Time-based retention deletes a segment by the largest timestamp
  it holds (5-minute check on the container); a test that stamps records with a fixed date in the past loses its
  data mid-run. `OffsetsByTimestampTest` was the last one fixed.
* **`DocumentationSyncTest` checks every `@see … section "…"` against the headings**, so renaming a section means
  moving every reference and the `section` field of the vector file with it — plan the renames into the ticket
  that owns the api.
* **Zero skips is the state of the integration suite.** A skip is a regression (the 0.11 line found five hidden
  behind a stale container name).
* **A `read_committed` consumer needs the isolation level in Offsets v2 as well as in Fetch** — it was nearly
  missed; the 1.x consumer keeps it.
* **The recurring merge-conflict spots are still the same four**, plus README and CHANGELOG: the "Wire vectors"
  preamble, the end of the vectors section, the `use` list of `Client.php`, the provider list of
  `ProtocolVectorTest`, the api-key table. All "keep both"; the `### <vector id>` blocks and the api sections never
  conflicted.
* **The shared broker is recreated between waves**, and one broker cannot produce every error code: 49, 52, 53
  and 30 need a second coordinator or an authorizer, 51 is transient. Say which codes are implemented from the
  sources alone, as the 0.11 document does.
* **The PHP 8.5 CLI of the sandbox runs the tracing JIT** and miscompiles hot pure-PHP byte loops: always
  `php -d opcache.jit=0 vendor/bin/phpunit`. `ByteUtils::crc32c()` uses the native `hash('crc32c')` for that
  reason; keep new byte loops out of PHP where ext/hash or `pack()` can do them.

## Open questions for the owner

1. **Which Kafka release the line speaks** — 1.1.1 is assumed above; 1.0.2 is the smaller alternative.
2. **Fetch sessions: frame only, or the consumer too.** The frame of Fetch v7 is a day's work and keeps the
   consumer sending session-less full fetches, which every 1.x broker serves; the incremental sessions of KIP-227
   save bandwidth for a consumer with many partitions and are a ticket of their own (T8).
3. **Delegation tokens (38–41).** The four apis are small, but a token is only *usable* with SASL/SCRAM, which
   this client does not implement (SASL/PLAIN only); the apis can be implemented and verified over the
   SASL_PLAINTEXT listener with a `delegation.token.master.key`, leaving the SCRAM authentication with a token as
   a limitation — or the whole group can be left out like the ACL apis of 0.11.
4. **The ACL apis 29–31 are still out** (decision 3 of the 0.11 epic). A second container with an
   `authorizer.class.name` and `allow.everyone.if.no.acl.found=true` would let a 1.x ticket add them without
   touching the shared broker; the 1.x error codes 53 and 65 belong to the same family.
5. **`main`'s README, CHANGELOG and document say `0.11.x` until T1 of the 1.x line renames them** — the cascade
   brings the 0.11 texts up verbatim, and rule 7 of `docs/CASCADE.md` makes the rename the first ticket, as it was
   for every line before. If the owner wants `main` to announce the 1.x line before that, it is a one-file change
   of the README title and its first paragraph.
