Changelog
==
All notable changes to `lisachenko/kafka-client` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and every line of
this repository follows the Apache Kafka release it speaks rather than semantic versioning of its
own: `main` is the **2.x line** and implements the **Kafka 2.8.2 wire protocol** — the last release
of the 2.x major, so everything Kafka 2.0 to 2.8 added — and nothing above it. The lines below it are
`1.x` (Kafka 1.1.1), `0.11.x` (Kafka 0.11.0.3), `0.10.x` (Kafka 0.10.2.2), `0.9.x` (Kafka 0.9.0.1)
and `0.8.x` (Kafka 0.8.2.2), and every line is merged upwards into the next one, so the sections
below accumulate: what a line added stays true of every line above it.

Unreleased — the 2.x line (Kafka 2.8.2)
---------------------------------------

The 2.x line, built on `main` on top of the finished 1.x line (branched off as `1.x`). Everything
below is verified against a real Apache **2.8.2** broker (`docker/kafka-2.8.2/`, four listeners)
and documented in [docs/protocol/2.8.md](docs/protocol/2.8.md). The plan of the line, and its
release record once it is complete, is [docs/handoff/main.md](docs/handoff/main.md).

### Added

- **The Kafka 2.8.2 broker of the line** — `docker/kafka-2.8.2/` (`eclipse-temurin:11-jre`,
  `kafka_2.13-2.8.2`) with the four listeners, the two log directories, the delegation-token key
  (`delegation.token.secret.key`, the 2.x name of `delegation.token.master.key`) and the
  `transaction.state.log.*=1` settings of the 1.1.1 image; `inter.broker.protocol.version` and
  `log.message.format.version` are `2.8-IV1`; `docker-compose.yml` builds it as the container
  `kafka-2-8-2`. The 1.1.1 image is gone from this branch (it lives on `1.x`).
- **The api keys 43–64** of `ApiKeys.java` @ 2.8.2 in `Protocol\ApiKeys`, and **the error codes
  72–104** of `Errors.java` @ 2.8.2 in `KafkaException`, one exception class each (`ListenerNotFoundException`
  … `InconsistentClusterIdException`); 72, 74, 80, 83, 84, 100 and 103 are `InvalidMetadataException`s
  and 75, 78, 88 and 89 plain `RetriableException`s in the Java client, and here. The code 90
  `PRODUCER_FENCED` is `TransactionalProducerFencedException`, because `ProducerFencedException` is
  the published name of the code 47.
- **`tools/dev/gate.sh`** — the whole local gate (php -l, cs, phpstan, unit + compliance, integration
  on the four listeners) for any worktree path, JIT off.

### Changed

- **The protocol document is `docs/protocol/2.8.md`**, renamed from `docs/protocol/1.1.md` with every
  `@see` reference; `docs/handoff/main.md` (the 1.x record) is `docs/handoff/1.x.md` now, and the
  plan of the 2.x line (`docs/handoff/2.0.x.md`) took its place as `docs/handoff/main.md`.

### Kafka 2.0 — Added

The first milestone of the line (PRs #117, #118, #119, #122): what Kafka 2.0 added on the wire —
almost only the version bumps of KIP-219 — and its one runtime change, the client-side throttle wait.

- **ApiVersions v2** (Kafka 2.0, KIP-219) — the frame of v1, byte for byte, and the promise that the
  client waits out a `throttle_time_ms` itself, because a broker answers a throttled request of a
  bumped version **before** it mutes the channel. `ApiVersionsRequest`/`ApiVersionsResponse` are the
  v2 now and `Client::apiVersions()`/`AdminClient::getApiVersions()` send it; the new
  `ApiVersionsRequestV1`/`ApiVersionsResponseV1` keep the version Kafka 0.11 added, next to the
  existing `…V0`. Two new wire vectors (`apiversions.request.v2`, `apiversions.response.v2`).
- **The four version bumps of Kafka 2.0 (KIP-219)** — **Produce v6**, **Fetch v8**, **ListOffsets v3**
  and **Metadata v6**, each with a byte-identical schema and its own class, its own wire vector pair
  captured from the 2.8.2 broker and its own version-equivalence test: `ProduceRequestV5`/`ProduceResponseV5`,
  `FetchRequestV7`/`FetchResponseV7`, `OffsetsRequestV2`/`OffsetsResponseV2` and
  `MetadataRequestV5`/`MetadataResponseV5` keep the versions the 1.x line sent.
- **KIP-219 in `Client`** — a broker that throttles a request now answers **first** and mutes the
  channel for the reported `throttle_time_ms`, so the client remembers the moment the throttle of each
  broker ends and sleeps whatever is left of it before its next request to that broker, exactly as the
  Java `NetworkClient` does. **`ClientConfig::THROTTLE_WAIT`** (`throttle.wait`, `true` by default)
  switches the waiting off and leaves the stall on the broker side. Measured against a real client
  quota in `tests/Integration/QuotaThrottleTest.php`.
- **OffsetForLeaderEpoch v1 (KIP-279)** — the response partition gained the `leader_epoch` the answered
  `end_offset` belongs to (`OffsetForLeaderEpochResponsePartition::$leaderEpoch`, `UNDEFINED_EPOCH` = -1);
  `OffsetForLeaderEpochRequestV0`/`OffsetForLeaderEpochResponseV0` keep version 0, whose answer has no
  such field.
- **KIP-283** — `tests/Integration/DownConversionTest.php` measures the down-conversion matrix of a
  2.8.2 broker and the topic option **`message.downconversion.enable=false`**, which refuses a fetch
  that would need a conversion with **35** `UNSUPPORTED_VERSION` (not 43) per partition.
- **The Kafka 2.0 versions of the ten group apis (KIP-219)** — OffsetCommit **v4**, OffsetFetch
  **v4**, FindCoordinator/GroupCoordinator **v2**, JoinGroup **v3**, Heartbeat **v2**, LeaveGroup
  **v2**, SyncGroup **v2**, DescribeGroups **v2**, ListGroups **v2** and DeleteGroups **v1**. Not
  one of them adds a field: from those versions on a throttled broker sends the answer **first** and
  mutes the channel for the delay afterwards, so a client that sends them honours `throttle_time_ms`
  itself. Every bump has a class of its own (`OffsetCommitRequestV3`, `GroupCoordinatorRequestV1`,
  `JoinGroupRequestV2`, `HeartbeatRequestV1`, `LeaveGroupRequestV1`, `SyncGroupRequestV1`,
  `DescribeGroupsRequestV1`, `ListGroupsRequestV1`, `DeleteGroupsRequestV0` and their answers) and a
  **wire vector pair captured from the 2.8.2 container**; `Client`, `KafkaConsumer` and `AdminClient`
  send the new versions.

### Kafka 2.0 — Changed

- **`Client` speaks the Kafka 2.0 versions**: Produce v6 for the message format v2 (v2 stays for the
  legacy message sets), Fetch v8 (with and without a fetch session), ListOffsets v3 and Metadata v6.
- **A 2.8.2 broker fills `last_stable_offset` for a `read_uncommitted` fetch too**, where a 1.1.1
  broker answered -1; only the `aborted_transactions` array still distinguishes the isolation levels.
- **A legacy message set in a Produce v3 or higher request is answered with 87 `INVALID_RECORD` per
  partition** instead of costing the connection, as it did on a 1.1.1 broker.
- **OffsetForLeaderEpoch answers the log end offset for the epoch the leader currently leads**, an
  empty partition included, where a 1.1.1 broker answered -1 (KAFKA-7415).

- **What a 2.x coordinator does differently**, measured on the container and written down in
  "Broker quirks and observations" of [docs/protocol/2.8.md](docs/protocol/2.8.md): a **`consumer`**
  group whose member metadata is not a real `Subscription` never leaves `PreparingRebalance` (KIP-345
  parses it, and the parse error is swallowed by the purgatory's timer thread); an error answer of
  JoinGroup carries the generation **-1** instead of 0; a successful FindCoordinator answer carries
  the error message **`"NONE"`** instead of null, and an unknown `coordinator_type` is answered with
  the error code **42** instead of costing the connection; the rebalance timeout is also the
  SyncGroup deadline (KAFKA-9752), so a member that joined with `rebalance_timeout = 0` is dropped at
  once; an OffsetCommit that leaves `retention_time` at -1 is stored with the `__consumer_offsets`
  value schema **v3**, which has no expiry at all (KIP-211), while an explicit retention still falls
  back to the schema v1 and is still honoured up to version 4 of the api.
- **The "API keys" section of the protocol document is the literal answer of a 2.8.2 broker**: the
  **56** keys 0–51, 56, 57, 60 and 61 with their version range, the first flexible version of every
  api, the Kafka minor that added every version above the 1.1.1 ceiling, and — while the line is
  built minor by minor — which of them this branch already implements.
  `tests/Integration/ApiVersionProbeTest.php` sends a real frame of every one of the 56 keys at its
  **maximum** version (with the request header v2 and a compact body for the 34 keys whose maximum is
  flexible, built by the raw probe fixture) and one frame above every one of them. Three inherited
  vectors were re-captured on the 2.8.2 container: `apiversions.response.v0`, `.v1` and
  `.v0.unsupported-version`.
- **What a 2.8.2 broker does with a frame it cannot serve, re-measured** (documented in "An api the
  broker does not serve closes the connection"): a version above the table is refused by the
  generated message class (`UnsupportedVersionException: The OFFSET_COMMIT protocol does not support
  version 9`), an api key of the **controller** listener with `Received request api key VOTE which is
  not enabled`, a key above `ApiKeys.java` with `Unexpected api key: 65`, and a flexible body with a
  wrong compact length or a missing tag buffer with a `BufferUnderflowException` — every one of them
  with a closed connection. The **35** of an unknown ApiVersions version now carries the ApiVersions
  row itself (KIP-511) instead of an empty array, and the request that asks for it has to carry the
  request header **v2**, because the broker derives the header version from the version it was asked
  for.

### Kafka 2.1

The second milestone of the line (PRs #121, #120, #128): the leader epochs of KIP-320 on the wire and in the
consumer, the zstd codec of KIP-110, KIP-211 and the version bumps that carry them.

- **Fetch v9 and v10** (KIP-320, KIP-110) — every partition entry of a v9 request carries a
  `current_leader_epoch` **between** the partition id and the fetch offset (`FetchRequest.json` @ 2.8.2
  is the field order, not the prose of the KIP), and v10 states that the client understands a
  zstd-compressed record batch. The answer is unchanged since v7. `FetchRequest`/`FetchResponse` are the
  v10 now, `FetchRequestV9`/`FetchRequestV8` and their answers keep the lower ones, and a partition may
  be given as an `[offset, epoch]` pair everywhere a fetch offset is taken
  (`Client::fetchPartitions()`, `fetchPartitionsWithSessions()`, `FetchSessionHandlerBuilder::add()`).
- **ListOffsets v4** (KIP-320) — a `current_leader_epoch` in every partition of the request and a
  `leader_epoch` **behind** the offset of every partition of the answer, so a listed offset carries the
  epoch it was resolved in. `OffsetsRequest`/`OffsetsResponse` are the v4, `OffsetsRequestV3` keeps the
  Kafka 2.0 one, and `OffsetAndTimestamp::$leaderEpoch` hands the epoch to the caller.
- **Metadata v7** (KIP-320) — a `leader_epoch` between the leader id and the replicas of every
  partition. `MetadataRequest`/`MetadataResponse` are the v7, `PartitionMetadata`/`TopicMetadata` carry
  the field and `PartitionMetadataV5`/`TopicMetadataV5` the v5-and-v6 entry.
- **OffsetForLeaderEpoch v2** (KIP-320) — a `current_leader_epoch` in front of the epoch that is asked
  about and a `throttle_time_ms` at the head of the answer, the version an ordinary **consumer** sends.
  `Client::offsetsForLeaderEpochs()` is the new entry point.
- **Produce v7** (KIP-110) — the v3 body once more, and the version a record set compressed with zstd
  needs: `ProduceRequest.validateRecords` @ 2.8.2 refuses the codec below it, and a 2.8.2 broker answers
  the partition with **76** rather than closing the connection. `ProduceRequest`/`ProduceResponse` are
  the v7, `ProduceRequestV6`/`ProduceResponseV6` the Kafka 2.0 pair.
- **The zstd codec** (KIP-110) — `CompressionCodec::ZSTD` (the compression type 4 of the message format
  v2) through **`ext-zstd`**, a suggested dependency of `composer.json`;
  `ProducerConfig::COMPRESSION_TYPE_ZSTD` is refused with a clear message when the extension is missing,
  and a zstd batch that a fetch brings in then raises `UnsupportedCompressionTypeException`, the
  client-side half of the code 76.
- **KIP-320 in the consumer** — the position of a partition is now an offset **and** the leader epoch it
  was taken at. `Cluster` remembers the newest `leader_epoch` of every partition and never applies an
  answer that moves it backwards (`updateLastSeenEpochIfNewer()`, `lastSeenLeaderEpoch()`),
  `SubscriptionState` keeps the position epoch, the current leader epoch and a validation flag per
  partition, and every `poll()` stamps its positions with the metadata epoch, validates the partitions
  whose epoch moved with an OffsetForLeaderEpoch v2 and fetches with the epoch in every partition entry.
  An `end_offset` below the position is a **log truncation**: `auto.offset.reset` resets the position,
  and with `none` the new `LogTruncationException` reaches the caller with the offset it stood at and the
  offset the leader answered. The codes **74** `FENCED_LEADER_EPOCH` and **75** `UNKNOWN_LEADER_EPOCH`
  refresh the metadata and leave the position alone.
- **19 wire vectors** captured on the `kafka-2-8-2` container for all of it, with the annotated dumps in
  the protocol document: the Fetch v9/v10 pairs and the 75 of a fenced epoch, the ListOffsets v4 pair,
  the Metadata v7 pair, the OffsetForLeaderEpoch v2 pair and its 75, the Produce v7 pair, and the three
  frames of the zstd rule (the **76** of a Fetch v9 against a `compression.type=zstd` topic, the same
  partition served to a Fetch v10, and the **76** of a Produce v6 whose record set is zstd).
- **New sections of [docs/protocol/2.8.md](docs/protocol/2.8.md)**: "The leader epoch (KIP-320)",
  "Version 10 and the zstd codec (KIP-110)", "KIP-320 in the consumer: leader epochs and truncation
  detection" and "The zstd codec (Kafka 2.1, KIP-110)", plus four measured broker quirks — the 75 that a
  one-broker container can produce and the 74 it cannot, the zstd refusal that is decided by the **topic
  configuration** and not by the records, the 76 of a produce that stays on an open connection, and the
  epoch 0 of every partition of the container.
- **OffsetCommit v5 (KIP-211)** — the version that **removes** `retention_time` from the frame. The
  committed offsets of a group expire `offsets.retention.minutes` after the **group** became empty
  from Kafka 2.1 on, so a per-commit retention has no place any more: the field has the versions
  `2-4` and is not sent as -1. `OffsetCommitRequestV4` is the last version that writes it, and a v5
  commit is stored with the `__consumer_offsets` value schema v3, which has no expiry at all.
- **OffsetCommit v6 and OffsetFetch v5 (KIP-320)** — the `committed_leader_epoch` of a committed
  offset: the epoch of the leader it was read from, so that a consumer that resumes from it can be
  told that the log was truncated behind its back. It lives on
  **`Consumer\OffsetAndMetadata::$leaderEpoch`** as a nullable int (`null` is the -1 of "not known",
  the empty `Optional` of the Java `OffsetAndMetadata.leaderEpoch()`), travels through
  `Client::commitGroupOffsets()` and comes back on
  `Protocol\Data\OffsetFetchResponsePartition::$leaderEpoch`, whose `toOffsetAndMetadata()` is the
  way into the value object. `OffsetCommitRequestV5`/`…V4`, `OffsetFetchRequestV4` and the
  `…PartitionV0`/`…TopicV0` entries keep the versions below; six more wire vectors were captured
  from the container, and the broker stores whatever epoch it is given — 74 and 75 are answered by
  the fetch path, not by the coordinator.
- **DeleteTopics v3 (Kafka 2.1)** — the frame of v2 with a higher version field; the version is the client's
  promise that it understands the error code **73** `TopicDeletionDisabled`, which a cluster with
  `delete.topic.enable=false` answers instead of the 42 a lower version keeps getting.
  `DeleteTopicsRequestV2`/`DeleteTopicsResponseV2` keep the version Kafka 2.0 added.
- **TxnOffsetCommit v2 (KIP-320)** — a `committed_leader_epoch` per partition, between the offset and the
  metadata; it comes from `Consumer\OffsetAndMetadata::$leaderEpoch` and is written as -1 when the client does
  not know it. `TxnOffsetCommitRequestV1` and the `…PartitionV0`/`…TopicV0` entries keep the versions below.
  The coordinator stores the epoch without validating it — 74 and 75 belong to the apis that read it back.


### Kafka 2.2

The third milestone of the line (PRs #123, #124, #130): the second join of KIP-394, the error code 78 of KIP-207,
the session lifetime of KIP-368, the broker epoch of KIP-380 and the new ElectLeaders api.

- **JoinGroup v4 (KIP-394)** — the version that refuses a **first join**. A request with an empty member id is
  answered immediately with the error code **79** (`MemberIdRequired`) and the member id the coordinator
  generated, and the client sends the same request again with that id; the coordinator no longer adds a member it
  cannot identify to a rebalance, and a client that never comes back leaves the group `Empty` with 0 members
  instead of holding a rebalance up. `Consumer\Internals\ConsumerCoordinator` does that second join by itself,
  immediately and without counting the refusal as a failed attempt, exactly as the Java
  `AbstractCoordinator.handleJoinResponse` does; `Client::joinGroup()` reports the code as a
  `MemberIdRequiredException` whose context carries the assigned id under **`assignedMemberId`** and leaves the
  second join to its caller. `JoinGroupRequestV3`/`JoinGroupResponseV3` keep the version below it, and four wire
  vectors of the exchange were captured from the container.
- **The error code 81 `GroupMaxSizeReached`** of the same KIP is documented from the broker sources and is not
  asserted against the container: `group.max.size` defaults to 2147483647 and is not a dynamically updatable
  broker config in Kafka 2.8, so it cannot be lowered without a restart.
- **ListOffsets v5** (KIP-207) — the version 4 frames in both directions (`ListOffsetsRequest.json` @ 2.8.2:
  "Version 5 is the same as version 4") and **one more error code in the answer**: **78**
  `OFFSET_NOT_AVAILABLE`. A leader whose high watermark has not caught up with the start offset of the epoch it
  was just elected in can not say where the end of its log is; `Partition.fetchOffsetForTimestamp` @ 2.8.2 raises
  the error for a **client** request (a follower is exempt) that asks for the latest offset or for a timestamp
  beyond the last fetchable one, and `KafkaApis.handleListOffsetRequest` sends it only to a version 5 or higher
  request — every lower one is answered **5** `LEADER_NOT_AVAILABLE` for the same state. Both codes are
  retriable, but 78 says the leader is there and will know in a moment, so a client retries the same broker
  instead of walking the cluster for a leader that was never missing. `OffsetsRequest`/`OffsetsResponse` are the
  v5 now and `Client::listOffsets()` sends it; `OffsetsRequestV4`/`OffsetsResponseV4` keep the Kafka 2.1 pair.
  Two wire vectors (`offsets.*.v5.latest`) with their annotated dumps, the section "Offsets API (key 2, v0 to
  v5), a.k.a. ListOffset" of the protocol document and a broker quirk for the substitution — the one rule of this
  line that is read from the broker sources rather than measured, because a one-broker container never re-elects
  a leader.
- **SaslAuthenticate v1 (KIP-368)** — a `session_lifetime_ms int64` at the end of the **answer**: the time after
  which the broker stops serving a connection that has not re-authenticated. The SASL path of `SocketStream`
  sends the v1 and keeps the value on `SaslAuthenticateResponse::$sessionLifetimeMs` and
  `SocketStream::getSaslSessionLifetimeMs()`; the re-authentication itself is documented from the sources and
  belongs to the SaslAuthenticate v2 of Kafka 2.5. `SaslAuthenticateRequestV0`/`…ResponseV0` keep Kafka 1.0's frame.
- **ControlledShutdown v2 (KIP-380)** — a `broker_epoch int64` behind the broker id.
  `AdminClient::controlledShutdown()` takes it and defaults it to `UNKNOWN_BROKER_EPOCH` (-1), the only epoch
  the controller does not compare; `ControlledShutdownRequestV1` keeps the version Kafka 0.9 added.
- **ElectLeaders (key 43) v0 (KIP-183)** — the api that replaced the ZooKeeper node
  `/admin/preferred_replica_election`, added as `ElectPreferredLeaders`. `AdminClient::electLeaders()` looks the
  controller up, repeats a 41 once and answers a `KafkaException|null` per partition; `Admin\ElectionType` carries
  `PREFERRED` and `UNCLEAN`, and the unclean election is refused until the v1 of KIP-460 exists.

### Kafka 2.3

The fourth milestone of the line (PRs #125, #126, #132): static membership (KIP-345), the authorized operations
of KIP-430, reading from a follower (KIP-392) and the IncrementalAlterConfigs api (KIP-339).

- **Static membership (KIP-345)** — a consumer configured with the new
  **`ConsumerConfig::GROUP_INSTANCE_ID`** (`group.instance.id`) carries that name in the
  `group_instance_id` of **JoinGroup v5, SyncGroup v3, Heartbeat v3 and OffsetCommit v7**, which are the
  versions this client sends now. The coordinator then remembers the member behind the name: a consumer
  that restarts joins under the same identity, **keeps its partitions and costs the group no rebalance**
  (measured: the generation does not change and the SyncGroup hands the old assignment back), and a
  static consumer **does not send LeaveGroup** when it is closed. A second consumer that joins under the
  same instance id takes the identity over and every request of the first one is answered **82**
  (`FencedInstanceId`): `FencedInstanceIdException` is fatal and reaches the application out of `poll()`
  and `commitSync()`. A join that names an instance id is never refused with the 79 of KIP-394.
- **The authorized operations of a group (KIP-430)** — `DescribeGroupsRequest` v3 carries the boolean
  `include_authorized_operations` and every group entry of the answer the 32-bit `authorized_operations`
  bit set, on `DescribeGroupResponseMetadata::$authorizedOperations`; `AdminClient::describeGroup()` and
  `describeGroups()` take the flag as their last argument, defaulted to `false`. The bits are the codes of
  `AclOperation`: a 2.8.2 broker without an authorizer answers **328** (READ, DELETE, DESCRIBE) and a
  request that does not ask is answered `-2147483648`
  (`DescribeGroupResponseMetadata::OPERATIONS_NOT_REQUESTED`), not the empty bit set.
- The version below each of the five is kept as its own class — `JoinGroupRequestV4`/`JoinGroupResponseV4`,
  `SyncGroupRequestV2`/`SyncGroupResponseV2`, `HeartbeatRequestV2`/`HeartbeatResponseV2`,
  `OffsetCommitRequestV6`/`OffsetCommitResponseV6`, `DescribeGroupsRequestV2`/`DescribeGroupsResponseV2` —
  together with the DTO versions `JoinGroupResponseMemberV0` and `DescribeGroupResponseMetadataV0`, and
  twelve wire vectors of the new frames were captured from the container.
- **LeaveGroup stays at v2**: the batch leave of KIP-345 is LeaveGroup v3, a Kafka 2.4 api.
- **Fetch v11** (KIP-392, reading from a follower) — a `rack_id` as the **last** field of the request, behind the
  forgotten topics, and a `preferred_read_replica` in every partition entry of the answer, **between** the
  aborted transactions and the record set. The consumer names its rack
  (`ConsumerConfig::CLIENT_RACK`, `client.rack`, the empty string by default) and the **leader** answers which
  replica to read that partition from; `FetchedPartition::$preferredReadReplica` carries it, `-1` being "read
  from me". `FetchRequest`/`FetchResponse` are the v11 now, `FetchRequestV10`/`FetchResponseV10` keep the Kafka
  2.1 pair and `FetchResponsePartitionV5`/`FetchResponseTopicV5` the partition entry of the versions 5 to 10.
  Measured on the container: without a `replica.selector.class` the answer is always `-1`, whatever rack the
  request names.
- **Metadata v8** (KIP-430, authorized operations) — two booleans in the request
  (`include_cluster_authorized_operations`, then `include_topic_authorized_operations`, behind the
  `allow_auto_topic_creation` of version 4) and two `int32` bitfields in the answer: one at the end of every
  topic entry, one at the end of the frame. The new **`Common\AclOperation`** is both halves of the bitfield —
  the thirteen operation codes of `AclOperation` @ 2.8.2, `fromBitField()`, `toBitField()`, `isAuthorized()` and
  `describe()` — and `TopicMetadata::$authorizedOperations` and
  `MetadataResponse::$clusterAuthorizedOperations` carry the two fields. `AclOperation::NOT_REQUESTED`
  (`Integer.MIN_VALUE`) is "you did not ask", which is **not** the empty set; `Client`'s metadata asks for
  neither. The same bitfield is what DescribeGroups v3 gained in the same release.
  `MetadataRequestV7`/`MetadataResponseV7` and `TopicMetadataV7` keep the Kafka 2.1 frames.
- **OffsetForLeaderEpoch v3** (KIP-392) — a `replica_id` at the **head** of the request, in front of the topics
  array: a follower sends its own broker id, a consumer `-1`
  (`OffsetForLeaderEpochRequest::CONSUMER_REPLICA_ID`) and the default of the field is `-2`, the debug client
  that may see offsets beyond the high watermark. The answer is unchanged — "Version 3 is the same as version 2"
  — and `OffsetForLeaderEpochRequestV2`/`OffsetForLeaderEpochResponseV2` keep it.
- **7 wire vectors** captured on `kafka-2-8-2` against the topic `t2-23-vectors`: the Metadata v8 pair with both
  booleans on, the same answer with them off, the Fetch v11 pair and the OffsetForLeaderEpoch v3 pair, each with
  its annotated dump.
- **New sections of [docs/protocol/2.8.md](docs/protocol/2.8.md)**: "Reading from a follower (v11, KIP-392)" and
  "The authorized operations (v8, KIP-430)" — with the measured bitfields of an unsecured broker, **8096** for
  the cluster and **3576** for a topic, which are the *supported* operations of the resource type — plus the
  version paragraphs of the three apis and two more broker quirks.
- **IncrementalAlterConfigs (key 44) v0 (KIP-339)** — changes **single options** of a topic or a broker, where
  `AlterConfigs` carries the whole configuration and resets everything a caller forgot to send back (Kafka 2.3
  deprecated it for this one). `Admin\AlterConfigOp` carries the operations `SET`, `DELETE`, `APPEND` and
  `SUBTRACT` — the last two only for a list option — and `AdminClient::incrementalAlterConfigs()` answers a
  `KafkaException|null` per resource. A resource is validated and applied as a whole.

1.x — the 1.x line (Kafka 1.1.1)
--------------------------------

The 1.x line, built on top of the `0.11.x` line it was cascade-merged from. Everything below is
verified against a real Apache **1.1.1** broker (`docker/kafka-1.1.1/`, four listeners) and
documented in [docs/protocol/2.8.md](docs/protocol/2.8.md), whose **314** wire vectors
[`tests/Compliance`](tests/Compliance) replays through the protocol classes — the 85 frames this
line captured and the 229 of the four lines below, which a 1.1.1 broker still speaks. What the line
delivered, how it was verified and what the line above it starts from is in
[docs/handoff/1.x.md](docs/handoff/1.x.md).

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
  `epoch -1`), which a 1.1.1 broker serves exactly as it serves a Fetch v6; the sessions themselves
  are driven by the consumer, see the KIP-227 entry below.
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
- **The idempotent producer of Kafka 1.x: `UNKNOWN_PRODUCER_ID` (59) is repaired instead of
  reported** — the broker drops the state of a producer id when every record it wrote into a
  partition is deleted (`DeleteRecords`, or a retention run), and answers the next batch of that
  producer with the code 59 rather than with the 45 of a 0.11 broker.
  `Producer\Internals\TransactionManager` now keeps the **offset of the last record the broker
  acknowledged** for every topic-partition (`lastAckedOffset()`, `updateLastAckedOffset()`, fed by
  a new `$baseOffset` argument of `batchCompleted()`) and decides on it, exactly as
  `TransactionManager.canRetry()` @ 1.1.1 does: the new **`canRetryBatch()`** answers `true` for a
  59 whose `logStartOffset` is `-1` (the partition moved away from the broker, so the same batch is
  sent again unchanged) and for one whose `logStartOffset` is **above** that offset — the records
  really were deleted — after **`startSequencesAtBeginning()`** has numbered *that* partition from
  the sequence 0 again, keeping the producer id and every other partition. `Client::produce()` sends
  the repaired batch once more, within `retries` and `retry.backoff.ms`; a 59 that this cannot
  explain reaches `batchFailed()` as the `OutOfOrderSequence` it is a subclass of. It holds for a
  transactional producer too — the 1.1.1 coordinator accepts the batch that starts the partition
  over under the epoch of the open transaction, so a deletion under an open transaction is no longer
  an abort.
- **The `log_start_offset` of a refused partition travels with its exception** —
  `Client::produce()` puts it into the context of the `KafkaException` of every failed partition of
  a Produce answer, so an application that catches a `TopicPartitionRequestException` can read
  `getContext()['logStartOffset']` next to the topic and the partition.
- **Wire vectors of what 1.x changed for the producer** — four Produce **v5** frames captured on the
  1.1.1 container with the client id and topic `t6-idempotent`: the request and the answer of a
  duplicate of the batch **four batches back** (error code 0, the base offset of the original append
  and its stored timestamp — the five-batch window, where a 0.11 broker answered 45), and the
  request and the answer of the batch that follows a `DeleteRecords` of the whole partition (error
  code **59** with `log_start_offset = 5`).
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
- **DescribeConfigs v1 (key 32) — the config source and the synonyms of KIP-226, Kafka 1.1.** The
  client sends **version 1** now: the request carries the trailing `include_synonyms` boolean
  (`AdminClient::describeConfigs($resources, $configNames, $includeSynonyms)`), and every entry of
  the answer replaces the `is_default` boolean of version 0 with a `config_source` int8 and gains
  the list of the places the broker looked for that value. New `Admin\ConfigSource` (the ids of
  `DescribeConfigsResponse.ConfigSource` @ 1.1.1: `UNKNOWN` 0, `TOPIC_CONFIG` 1,
  `DYNAMIC_BROKER_CONFIG` 2, `DYNAMIC_DEFAULT_BROKER_CONFIG` 3, `STATIC_BROKER_CONFIG` 4,
  `DEFAULT_CONFIG` 5) and `Admin\ConfigSynonym`; `Admin\ConfigEntry` gained `$source` and
  `$synonyms`, and its `$isDefault` is derived from the source exactly as `ConfigEntry.isDefault()`
  does it. `Admin\Config::ownValues()` is new and is the set a read-modify-write has to send back
  through AlterConfigs — since KIP-226 `nonDefaultValues()` also reports the options that only the
  **broker** configuration sets. The version 0 of the api stays available as
  `DescribeConfigsRequestV0`/`DescribeConfigsResponseV0` (with `…ResponseResourceV0` and
  `…ResponseConfigEntryV0`), and a client that reads it derives the source back from the boolean
  and the resource type, which is what the Java client does.
- **The dynamic broker configuration of KIP-226 through AlterConfigs (key 33)** — the frame did not
  change, the broker did: a **broker** resource is accepted now, where a 0.11 broker refused every
  one of them with 42. `ConfigResource::defaultBroker()` names the cluster-wide default (the
  resource type 4 with an **empty** name, `/config/brokers/<default>` in ZooKeeper), which every
  broker of the cluster picks up and reports with the source `DYNAMIC_DEFAULT_BROKER_CONFIG`, while
  a value set for one broker wins over it with `DYNAMIC_BROKER_CONFIG`. Only the options of
  `DynamicBrokerConfig.AllDynamicConfigs` can be changed at runtime; anything else is answered with
  42 and `Cannot update these configs dynamically: Set(…)`, and the validation refuses the whole
  resource, not the single option.
- **CreatePartitions (key 37, v0, KIP-195, Kafka 1.0)** — `CreatePartitionsRequest`/`Response`,
  `Data\CreatePartitionsRequestTopic`/`…ResponseTopic` and the value object `Admin\NewPartitions`
  with the Java factory `increaseTo($totalCount, $newAssignments)`.
  `AdminClient::createPartitions($newPartitions, $timeoutMs, $validateOnly)` sends the request to
  the active controller and repeats it once on 41, exactly like `createTopics()`, and reports one
  entry per topic without throwing. The count is what the topic should have **afterwards**: the api
  can only grow a topic (37 `Topic already has 3 partitions.` otherwise), and an assignment names
  the brokers of every ADDED partition (39 when its length or width does not fit).
- **DeleteGroups (key 42, v0, KIP-229, Kafka 1.1)** — `DeleteGroupsRequest`/`Response` and
  `Data\DeleteGroupsResponseGroup`. `AdminClient::deleteConsumerGroups($groupIds)` looks a
  coordinator up for every group, sends one request per coordinator and reports one entry per group:
  `null` when the group and its committed offsets are gone, the code **68** (`GroupNotEmpty`) for a
  group that still has a member and **69** (`GroupIdNotFound`) for one the coordinator does not
  know. A deleted group disappears from `listGroups()`, its offsets answer -1 in an OffsetFetch and
  `describeGroup()` reports it as `Dead`.
- **Wire vectors of the four apis** — `docs/protocol/vectors/create-partitions.json` and
  `delete-groups.json` (new, eight and six frames), plus eight DescribeConfigs v1 and six
  AlterConfigs frames captured on the 1.1.1 container: a topic with and without `include_synonyms`,
  the broker resource with a dynamic option and a sensitive one, the cluster-wide default resource,
  the accepted and the refused AlterConfigs of a broker, the growth of a topic, an assignment with
  `validate_only`, the 37 of a shrink, the 3 of an unknown topic, and the 0/68/69 of DeleteGroups.
  The six version 0 vectors of DescribeConfigs are replayed through the new `…V0` classes.
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
- **The incremental fetch sessions of KIP-227 in the consumer (Kafka 1.1)** — `KafkaConsumer` now
  holds one fetch session per broker it reads from. New `Consumer\Internals\FetchSessionHandler`
  (the `org.apache.kafka.clients.FetchSessionHandler` of the Java client: `newBuilder()` →
  `FetchSessionHandlerBuilder::add(TopicPartition, int $fetchOffset)` → `build()` gives the
  `FetchRequestData` of the next request with its `toSend`, `toForget`, `sessionPartitions` and
  `metadata`; `handleResponse()` says whether the answer may be read and moves the epoch on,
  `handleError()` puts the handler back to a full fetch), and the new
  `Client::fetchPartitionsWithSessions()`, which the consumer fetches through and which returns
  **only the partitions the brokers answered** — an incremental answer leaves out everything that
  has no news, and the state of those partitions stays valid. The Java `PartitionData` triple is a
  single fetch offset here, because a Fetch request of this client carries one `MaxBytes` for every
  partition and a `LogStartOffset` that only a follower fills in. `Client::fetchPartitions()` keeps
  its signature and its session-less behaviour for the bare client, and
  `Client::getFetchSessionHandlers()` reports the sessions a client holds. The first request to a
  broker opens the session with a full fetch, every following one states only the positions that
  moved, a partition that leaves the assignment (a rebalance, `pause()`, a deleted topic) travels in
  `forgotten_topics_data`, and the error codes **70** and **71**, a broker that hands out no session
  at all and a request that was never answered are all handled inside the client: a `poll()` never
  sees a session error. Verified against the 1.1.1 container in
  `tests/Integration/FetchSessionConsumerTest.php`, including a real two-member rebalance and a
  session cache filled to its 1000 slots.
- **Wire vectors of the consumer side of a session** (`fetch.json`, four more Fetch v7 frames with
  the client id and topic `t8-vectors`): `fetch.request.v7.incremental-forgotten` — an incremental
  fetch that states the two positions that moved **and** forgets a third partition, the frame a
  rebalance produces — with its answer, and `fetch.request.v7.close-existing` — the recovery from a
  session error: the client's own session id with the epoch 0 — with the answer that carries a
  **new** session id and every partition of the request.
- **`examples/delegation-tokens.php`** — one token's whole life against a SASL listener of the
  container: created with a renewer and a maximum lifetime, described, renewed (which lands on the
  maximum lifetime rather than on the period that was asked for) and removed, with the **62** of
  expiring it a second time.
- **The release record of the line** — [docs/handoff/1.x.md](docs/handoff/1.x.md) is the release
  notes of the 1.x line (what was built, how it was verified, the deviations the broker forced, the
  known limitations) with the plan it was built from kept below them, and
  [docs/handoff/main.md](docs/handoff/main.md) is the handoff of the next line: what Kafka 2.0.1
  adds over 1.1.1 api by api, the error codes, a ticket plan and the pitfalls of this session.

### Changed

- **The protocol document is `docs/protocol/2.8.md`** (renamed from `docs/protocol/0.11.0.md`, as
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
  for anything older), while a **first** batch of a producer id the broker has no entry for that
  does not start at the sequence 0 is **59**, where 0.11.0.3 answered 45 — the 45 is left for an
  entry that exists, i.e. a gap in the sequence or an epoch bump that does not restart at 0; a Fetch
  below v4 of a partition whose records carry headers is **served with
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
- **The "Idempotent producer" section of the README and the sections "The idempotent producer" and
  "Transactions" of the protocol document** describe the 1.x producer: the five-batch window, the
  error code 59 with the `log_start_offset` that decides what to do about it, and the whole
  behaviour table re-measured against the 1.1.1 container. The transaction apis 24-28 are unchanged
  at version 0 and every observation of the 0.11 line was re-measured on the 1.1.1 coordinator with
  the same result, down to the defaults `transactional.id.expiration.ms = 604800000` and
  `transaction.abort.timed.out.transaction.cleanup.interval.ms = 60000`.
- **The consistency pass over the documentation of the line** — `docs/protocol/2.8.md` no longer
  names a ticket anywhere except at the consumer half of the fetch sessions, "Broker quirks and
  observations" is grouped by api (with new groups for the KIP-226 configuration, CreatePartitions
  and DeleteGroups, and for the delegation tokens), the "Wire vectors" preamble states the counts of
  the line, and "What is not in Kafka 1.1.1" now also names the replication apis 4, 5 and 6.
  `docs/protocol/vectors/README.md` lists what each file captured on the 1.1.1 container, the README
  matrix and the feature and limitation tables are complete, and `docs/CASCADE.md` and `CLAUDE.md`
  record the finished line.

### Fixed

- **`LogDirsApiTest` no longer depends on the speed of the disk.** The two tests that watch a replica move
  in flight caught it on the container, where an 8 MB partition takes a quarter of a second, and missed it
  on a CI runner that copied the same partition in less than one request round trip. They now bound the
  mover with the dynamic broker option `replica.alter.log.dirs.io.max.bytes.per.second` (KIP-113) through
  `alterConfigs()` of the broker resource — 1 MB/s, eight seconds for the move — and remove it again in
  `tearDown()`. CI also runs the integration suite with the SSL and SASL listener variables now, so it
  reports the same 549 tests with zero skips as the local gate.
- **Seven integration tests were silently skipped against the 1.1.1 broker.** Three fixtures still
  named the container of the line below (`kafka-0-11-0-3`) and the SSL tests still pointed at its
  certificate, so `MessageFormatV1Test`, `RecordBatchV2Test` and the quota tests skipped themselves
  instead of running — the same pitfall the 0.11 line found. The fixtures name `kafka-1-1-1` and
  `docker/kafka-1.1.1/ssl/broker.crt` now, and the suite runs with **zero** skips.
- **Two docblocks asserted what a 1.1.1 broker contradicts** — `ApiVersionsResponse` claimed the
  answer holds "34 keys 0 to 33" and `ControlledShutdownRequestV0` that the broker reports
  `minVersion = 1` for the api key 7. Both are corrected against the api table of the container; no
  behaviour changed.
- **Seven examples still pointed at the line below.** `idempotent-producer.php` and
  `record-headers.php` carried an `@see docs/protocol/0.11.0.md`; they and `producer.php` and
  `transactional-producer.php` printed `docker exec kafka-0-11-0-3 …` commands that no container of
  this branch answers; `ssl.php` and `sasl.php` read the broker certificate from
  `docker/kafka-0.11.0.3/ssl/`, the image directory of the line below; and `admin.php` announced
  itself as an example "for the Kafka 0.11.0.3 protocol". All of them name the 1.1.1 document,
  container and image directory now — `DocumentationSyncTest` does not scan `examples/`, so nothing
  had caught it.

Unreleased — the 0.11.x line (Kafka 0.11.0.3)
-------------------------------------------

The 0.11 line, built on top of the `0.10.x` line it was cascade-merged from. Everything below was
verified against a real Apache **0.11.0.3** broker (`docker/kafka-0.11.0.3/`, four listeners) and
is documented byte for byte in [docs/protocol/2.8.md](docs/protocol/2.8.md), whose 229 wire
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

- **`docs/protocol/2.8.md` is the grammar of Kafka 0.11.0.3.** The api-key table is the literal
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
byte in [docs/protocol/2.8.md](docs/protocol/2.8.md), with 120 wire vectors in
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
  `@see docs/protocol/2.8.md, section "…"` of the sources names a heading that exists.
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
- **The protocol document is `docs/protocol/2.8.md`** and describes Kafka 0.10.2.2: the api-key
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
