Changelog
==
All notable changes to `lisachenko/kafka-client` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and every line of
this repository follows the Apache Kafka release it speaks rather than semantic versioning of its
own: `main` is the **4.x line**, built towards the **Kafka 4.x wire protocol** one Kafka minor at a time on top
of the finished 3.x line, verified against a Kafka **4.3.1** KRaft node; today the client speaks **Kafka 4.2**
(the milestone of the line reached so far) and the KIP-848 consumer protocol. The lines
below it are `3.x` (Kafka 3.9.2), `2.x` (Kafka 2.8.2), `1.x` (Kafka 1.1.1), `0.11.x` (Kafka 0.11.0.3), `0.10.x`
(Kafka 0.10.2.2), `0.9.x` (Kafka 0.9.0.1) and `0.8.x` (Kafka 0.8.2.2), and every line is merged upwards
into the next one, so the sections below accumulate: what a line added stays true of every line above it.

Unreleased — the 4.x line (towards Kafka 4.3.1)
-----------------------------------------------

The 4.x line, built on `main` on top of the finished 3.x line (branched off as `3.x`), on the integration branch
`feature/beautiful-johnson-elv5yg` (epic [#213](https://github.com/lisachenko/kafka-client/issues/213)). The plan
of the line is [docs/handoff/main.md](docs/handoff/main.md). Current milestone: **Kafka 4.2**. One milestone
follows (4.3), and the KIP-932 share consumer as the last wave.

### Added

- **The Kafka 4.3.1 node of the line** — `docker/kafka-4.3.1/` (`eclipse-temurin:17-jre`, `kafka_2.13-4.3.1`) in
  **KRaft** combined mode with a **dynamic quorum** (`controller.quorum.bootstrap.servers`, `kafka-storage.sh format
  --standalone`, as the release formats a combined node): the four client listeners, the CONTROLLER listener 9096,
  the two log directories, the delegation-token secret key, the `StandardAuthorizer` and the SASL users of the 3.x
  node, and the one-node settings of the share-group state topic. Every feature is finalized at the default of the
  release: `metadata.version` 4.3-IV0, `kraft.version` 1, `transaction.version` 2, `group.version` 1,
  `share.version` 1, `streams.version` 1, `eligible.leader.replicas.version` 1. `docker-compose.yml` builds it as the
  container `kafka-4-3-1`; `docker/kafka-3.9.2/` is gone (it lives on `3.x`).
- **The api keys 88–92** in `Protocol\ApiKeys` (Kafka 4.1: `STREAMS_GROUP_HEARTBEAT`, `STREAMS_GROUP_DESCRIBE`,
  `DESCRIBE_SHARE_GROUP_OFFSETS`, `ALTER_SHARE_GROUP_OFFSETS`, `DELETE_SHARE_GROUP_OFFSETS`), read off
  `ApiKeys.java` @ 4.3.1.
- **The error codes 128–133** with one exception class each, read off `Errors.java` at the tags:
  `InvalidRegularExpressionException` (128, 4.0; the Java class is `InvalidRegularExpression`),
  `RebootstrapRequiredException` (129, 4.0, KIP-1102), `StreamsInvalidTopologyException`,
  `StreamsInvalidTopologyEpochException`, `StreamsTopologyFencedException` (130–132, 4.1, KIP-1071) and the
  retriable `ShareSessionLimitReachedException` (133, 4.1, KIP-932).

### Changed

- **The protocol document is `docs/protocol/4.3.md`**, renamed from `docs/protocol/3.9.md` with every `@see`
  reference: a new head, the 4.3.1 node and its features among the sources, "What 4.3.1 adds to the 3.9.2 protocol"
  (the four minors, the versions KIP-896 removed, the keys 88–92 and the codes 128–133, the owner's decisions), and
  the api-key table rebuilt from the ApiVersions answer of the node — **75 keys**, fifteen of them starting above
  v0 — with a "What Kafka 4.x added" column and every version this line has not reached marked *not yet
  implemented on this line*.
- **`ApiVersionProbeTest` pins the 4.3.1 table**: a frame of every key at its maximum version (the share-group,
  share-state, streams-group and share-offset apis included), one above it, and one of **every version Kafka 4.0
  removed** — all of which close the connection, Produce v0 to v2 too although the answer lists them (KAFKA-18659);
  the seven finalized features of ApiVersions v4; the 126 `DuplicateVoter` and the 127 `VoterNotFound` of the
  raft-voter apis on a `kraft.version` 1 quorum, where the 3.9.2 node answered 35.
- **`ApiVersionsRequest::CLIENT_SOFTWARE_VERSION` is `4.3`** (KIP-511), the line this client speaks towards.
- `CLAUDE.md`, `README.md`, the CI workflow and every fixture and example point at the 4.3.1 node.

### Known failures of the inherited suite

The foundation measured the 3.x suite on the 4.3.1 node — 140 failing tests in 31 classes, listed with an owner in
the epic ([#213](https://github.com/lisachenko/kafka-client/issues/213)) — and the four tickets of the 4.0 wave
brought every one of them to what the node answers (see "Kafka 4.0 — Changed" below). **None is left.**

- **`main` is the 4.x line**: the finished 3.x tree was branched off as `3.x` (protected, tagged `3.0.2` … `3.9.2`
  by the owner) and wired into the cascade (`3.x → main`); the record of the 3.x line moved to
  [docs/handoff/3.x.md](docs/handoff/3.x.md) and the plan of the 4.x line took its place as
  `docs/handoff/main.md`; the tables of tag points and milestone commits left the handoff records — the tags of
  the repository are the record.

### Kafka 4.2 — Added

Milestone `chore(4.x): Kafka 4.2 complete`, PRs #228 (T4), #229 (T1), #230 (T2) and #231 (T3).

- **ListOffsets v11** (KIP-1023): the target time `OffsetsRequest::EARLIEST_PENDING_UPLOAD_TIMESTAMP` (`-6`) and
  `AdminClient::listEarliestPendingUploadOffsets()`, the `OffsetSpec.earliestPendingUpload()` of the Java admin client;
  the client sends v11 wherever it sent v10, and `OffsetsRequestV10`/`OffsetsResponseV10` keep the version below. A
  node without remote storage answers the offset -1 with the code 0, and a v10 frame of the `-6` a per-partition 35.
  Produce, Fetch and Metadata have no 4.2 or 4.3 version.
- **OffsetCommit v10 and OffsetFetch v10** (KIP-848): every topic named by its **topic id**, resolved through
  `Cluster::topicIdsOf()` and named back in the answer, in both consumer coordinators and the admin offsets methods;
  the 100 of an unknown or deleted id reloads the metadata and is retried, and a topic the cluster gives no id goes
  out as v9 (the fallback of the Java `CommitRequestManager`). `OffsetCommitRequestV9`/`ResponseV9`,
  `OffsetFetchRequestV9`/`ResponseV9` and their `Data` keep-behinds keep the version below.
- **`AdminClient::alterConsumerGroupOffsets()`** over OffsetCommit v10: the commit of an administrator (generation -1),
  one exception or null per partition; a group with live members refuses it with the 25.
- **ShareFetch v2 and ShareAcknowledge v2** (KIP-1206, KIP-1222): the `$shareAcquireMode` (batch-optimized or
  record-limit) and the `$isRenewAck` of `Client::shareFetch()`, the `$isRenewAck` of `Client::shareAcknowledge()`,
  the acknowledge type 4 `ShareAcknowledgementBatch::RENEW` and the `ShareAcknowledgeResponse::$acquisitionLockTimeoutMs`
  of every answer; a renew extends the acquisition lock. `ShareFetchRequestV1`/`ResponseV1` and
  `ShareAcknowledgeRequestV1`/`ResponseV1` keep the version below.
- **AddRaftVoter v1**: `ack_when_committed`, the trailing `$ackWhenCommitted` of `AdminClient::addRaftVoter()`, which
  changes no refusal of the node; `AddRaftVoterRequestV0`/`ResponseV0` keep the version below.
- **WriteShareGroupState v1, ReadShareGroupStateSummary v1 and DescribeShareGroupOffsets v1** (KIP-1226), wire
  classes: the `DeliveryCompleteCount` of a share partition and the share-partition `lag`
  (`DescribeShareGroupOffsetsResponsePartition::$lag`), measured with real share traffic as end offset − start offset
  − delivery-complete count. The V0 classes keep the version below.
- **WriteTxnMarkers v2** (KIP-1228), wire only: the `transaction_version` of a marker
  (`WriteTxnMarkersRequestMarker::$transactionVersion`); the client listener answers it to a super user, and refuses a
  transaction-version-2 marker at the epoch of an open transaction with the 47. `WriteTxnMarkersRequestV1`/`ResponseV1`
  keep the version below.
- **76 wire vectors** captured on the `kafka-4-3-1` node (**278** on the 4.x line, **1413** in **74** files).

### Kafka 4.2 — Changed

- **The consumer and the admin offsets methods send OffsetCommit and OffsetFetch v10** on a node that serves them; the
  classic `ConsumerCoordinator` of the Java client @ 4.2.0 still sends v9.
- The probes of the streams apis 88 and 89 pin the answers of the 4.2 schema (a StreamsGroupHeartbeat leave carries an
  empty `Status` array where it carried null).
- `OffsetsByTimestampTest` probes an unknown target time with `-7`: Kafka 4.2 gave `-6` a meaning.
- The integration suites that send raw offset frames without topic ids (`GroupTypeListingApiTest`,
  `StaticMembershipApiTest`, `ThrottleTimeApiTest`) send them through the V9 classes.

### Kafka 4.1 — Added

Milestone `chore(4.x): Kafka 4.1 complete`, PRs #224 (T1), #225 (T2), #226 (T4) and #227 (T3).

- **Produce v13** (KIP-516 on the produce path): every topic named by its **topic id**, resolved through
  `Cluster::topicIdsOf()` and answered by id; the **100** `UnknownTopicId` of a stale id is retried by
  `Network\RetryPolicy`, and `produce()` reloads the metadata on it even without retries left. A transaction of the
  protocol v1 stays capped at `ProduceRequestV11`; `ProduceRequestV12`/`ProduceResponseV12` keep the v12 frame.
- **Fetch v18** (KIP-1166): the tagged `high_watermark` of a follower (tag 1 of every partition entry,
  `FetchRequestTopicPartition::$highWatermark`, default `Long.MAX_VALUE` and left off the wire by a consumer, whose
  frame is the v17 frame); `FetchRequestV17`/`FetchResponseV17` keep the version below.
- **AlterPartitionReassignments v1**: `allow_replication_factor_change`, the trailing
  `$allowReplicationFactorChange` of `Client::alterPartitionReassignments()` and
  `AdminClient::alterPartitionReassignments()`; `AlterPartitionReassignmentsRequestV0`/`ResponseV0` keep the version
  below.
- **ListTransactions v2** (KIP-1152): the `$transactionalIdPattern` of `AdminClient::listTransactions()`, a RE2/J
  match of the whole id, and the **128** `InvalidRegularExpressionException` of a pattern the node cannot compile;
  `ListTransactionsRequestV1`/`ResponseV1` keep the version below.
- **`AdminClient::listConfigResources()`** over ListConfigResources (key 74) **v1** (KIP-1142: the
  ListClientMetricsResources of 3.7 became a typed list of config resources — topics, brokers, broker loggers,
  client-metrics subscriptions and groups); the 35 of a type the node does not list;
  `ListClientMetricsResourcesRequestV0`/`ResponseV0` keep the version below.
- **The share-group wire of KIP-932 at v1** (keys 76–79): `Client::joinShareGroup()`, `shareGroupHeartbeat()`,
  `leaveShareGroup()`, `shareFetch()` (the share session of a connection and the acquired records with their delivery
  count) and `shareAcknowledge()` (the close of the share session with the epoch -1, which releases what it still
  holds), and `AdminClient::describeShareGroups()` / `describeShareGroup()` over ShareGroupDescribe v1; the codes 121
  to 123 observed on the node, 124 and 133 documented.
- **The share-group state apis 83–87 at v0, wire only** (the share coordinator's own apis): classes and vectors, no
  client method; the 42 of a partition never initialized (84), its initial state (87), the 31 of the SASL user
  `acltest`.
- **The share-group offset apis 90–92 at v0** (DescribeShareGroupOffsets, AlterShareGroupOffsets,
  DeleteShareGroupOffsets): the wire classes; their admin methods come with the share consumer. An AlterShareGroupOffsets
  creates the share group, the 69 of a classic group, the 68 of a group with members.
- **Wire vectors** captured on the `kafka-4-3-1` node: **102** more (**202** on the 4.x line, **1337** in **74**
  files).

### Kafka 4.1 — Changed

- **`Network\RetryPolicy` retries the 100 `UnknownTopicId`** (Produce v13 and a Fetch from v13).
- `VectorFile::names()` sorts the vector files by their basename.
- The integration suite waits for a serving leader of a fresh topic (`TopicMetadataProbe`) where a first request
  raced the 6 `NotLeaderForPartition`, and writes a group configuration before it lists one
  (`ConfigResourcesApiTest`).

### Kafka 4.0 — Added

Milestone `chore(4.x): Kafka 4.0 complete`, PRs #220 (T1), #221 (T4), #222 (T3) and #223 (T2).

- **Produce v12** (KIP-890 part 2, the v11 frame): sent outside transactions and inside a transaction of the
  protocol v2, and **capped at v11** inside a transaction of the protocol v1. One choice point,
  `Client::produceVersion()`, reads `TransactionManager::isTransactionV2Enabled()`. `ProduceRequestV11`/`ProduceResponseV11`
  keep the version below.
- **Metadata v13** (KIP-1102): the top-level `MetadataResponse::$errorCode`. `Cluster::reload()` asks the next
  bootstrap server on a non-zero code and throws the last one (`RebootstrapRequiredException` for the 129) when none
  answers without one; the node always answers 0. `MetadataRequestV12`/`ResponseV12` keep the version below.
- **ListOffsets v10** (KIP-1075): `timeout_ms`, filled with `request.timeout.ms`
  (`OffsetsRequest::DEFAULT_TIMEOUT_MS`, `getTimeoutMs()`); `OffsetsRequestV9`/`ResponseV9` keep the version below.
- **The transaction protocol v2 of KIP-890 part 2** in `Producer\Internals\TransactionManager`: it reads the finalized
  `transaction.version` from the ApiVersions answer of the transaction coordinator and, from level 2, sends no
  AddPartitionsToTxn and no AddOffsetsToTxn, commits offsets with **TxnOffsetCommit v5**, and ends every transaction
  with **EndTxn v5**, taking the producer id and epoch of the next transaction from its answer
  (`Client::endTxnBumpingEpoch()`, `Client::txnOffsetCommit(…, $transactionV2)`, `EndTxnResponse::$producerId`,
  `$producerEpoch`). The **v1 path is kept** for a 3.x node and a cluster below level 2; `EndTxnRequestV4`/`ResponseV4`
  and `TxnOffsetCommitRequestV4`/`ResponseV4` are its classes.
- **DescribeGroups v6** (KIP-1043): the `error_message` of a group entry and the **69** of a group the coordinator
  does not hold or that is a KIP-848 group. `DescribeGroupsRequestV5`/`ResponseV5` and
  `DescribeGroupResponseMetadataV4` keep the version below.
- **ConsumerGroupHeartbeat v1** (KIP-848, KIP-1082): **`KafkaConsumer::subscribeByPattern(SubscriptionPattern)`**, a
  RE2/J subscription the coordinator resolves (`group.protocol=consumer` only; the classic protocol throws
  `InvalidConfigurationException`), and a member id the consumer generates for itself in the base64 format of the Java
  client and keeps for its whole life; the **128** `InvalidRegularExpressionException` observed.
  `ConsumerGroupHeartbeatRequestV0`/`ResponseV0` keep the version below.
- **ConsumerGroupDescribe v1** (KIP-1099): the `member_type` of every member, `ConsumerGroupMemberDescription::$memberType`
  and `upgraded()`. V0 keep-behinds for the request, the response, the group and the member.
- **UpdateFeatures v2**: no per-feature results; `updateFeatures()` returns null for every feature of an accepted
  request and throws the one top-level refusal (95) of a 4.x controller. `UpdateFeaturesRequestV1`/`ResponseV1`.
- **DescribeCluster v2** (KIP-1073): `describeCluster(…, bool $includeFencedBrokers = false)`,
  `ClusterDescription::$fencedNodeIds` and `isFenced()`. `DescribeClusterRequestV1`/`ResponseV1`.
- **`AdminClient::addRaftVoter()` and `removeRaftVoter()`** over AddRaftVoter (80) and RemoveRaftVoter (81) v0
  (KIP-853) on the `kraft.version` 1 quorum; the **126** `DuplicateVoter` and the **127** `VoterNotFound` observed,
  and the 7 of an unreachable voter, which is never added.
- **100 wire vectors** captured on the `kafka-4-3-1` node (**1235** in **62** files, the new ones
  `add-raft-voter.json` and `remove-raft-voter.json`).

### Kafka 4.0 — Changed

- **`AdminClient::describeGroup()` / `describeGroups()` throw `GroupIdNotFoundException`** for a group the
  coordinator does not hold (DescribeGroups v6, KIP-1043), as the Java admin client of 4.0 does, where the 3.x line
  returned the state `Dead` with the code 0; the signatures are unchanged.
- **A `message.format.version` below 0.11.0 is refused client-side** (`InvalidConfigurationException`) when the
  leader's Produce row reaches v12: a 4.x node closes the connection on Produce v0 to v2 (KIP-896), although its
  ApiVersions answer still lists them (KAFKA-18659). A 3.x node still gets its Produce v2.
- **An empty transaction is no longer ended at the coordinator** (Java's `transactionStarted`): a 4.x node answers
  such an EndTxn with 48.
- **The inherited suite measures the 4.3.1 node.** Every version KIP-896 removed (Produce v0–v2, Fetch v0–v3,
  ListOffsets v0, OffsetCommit v0/v1, OffsetFetch v0, OffsetForLeaderEpoch v0/v1, CreateTopics v0/v1, DeleteTopics
  v0, DescribeConfigs v0, the ACL and token apis v0) is measured as a closed connection (`tests/Fixture/RemovedVersionProbe`),
  its classes and vectors stay; test data is written as record batches v2; the topic configs
  `message.format.version` and `message.downconversion.enable` are refused with the 40; a zstd topic is served to
  a Fetch below v10 (no 76 since 4.0); a follower Fetch v16 answer names the leader without its endpoint; the
  bidirectional online upgrade of a classic group to KIP-848 (`group.consumer.migration.policy`, 4.0); the
  asynchronous target assignment of 4.3; the 3400 operations of a group; the offset value schema v4 of 4.1; the share
  coordinator and its `group:topicId:partition` keys; the cluster-level `min.insync.replicas` of KIP-966 on the
  `broker:` resource and the 4.x broker messages; the connection-scoped client instance of KIP-714 (4.0); the 4.x
  answers of UpdateFeatures (an unknown feature is refused, the whole request refused at once).
- `DocumentationSyncTest` checks the `@see` references of the renamed document (it matched `3.9.md` and checked
  nothing after the rename) and refuses a reference to any other protocol document.

The 3.x line (Kafka 3.0 to 3.9 and the KIP-848 consumer, 3.9.2)
----------------------------------------------------------------

The 3.x line, built on `main` on top of the finished 2.x line (branched off as `2.x`), **complete** and
branched off as `3.x`. Everything
below is verified against a real Apache Kafka **3.9.2** node in **KRaft** mode (`docker/kafka-3.9.2/`,
broker and controller in one process, four client listeners) and documented in
[docs/protocol/4.3.md](docs/protocol/4.3.md). The record of the line — its release notes above the plan it was
built from — is [docs/handoff/3.x.md](docs/handoff/3.x.md); the record of the 2.x line is
[docs/handoff/2.x.md](docs/handoff/2.x.md). **The line is complete** (the foundation, the re-baseline wave T0, the 3.0 to 3.9 waves and the KIP-848 consumer wave are in; Kafka 3.4 added nothing a client sends).

### Added

- **The Kafka 3.9.2 node of the line** — `docker/kafka-3.9.2/` (`eclipse-temurin:17-jre`,
  `kafka_2.13-3.9.2`) in **KRaft** combined mode: `process.roles=broker,controller`, node id 1, a
  `CONTROLLER` listener on 9096 inside the container next to the four client listeners, the storage
  formatted at the first start (`kafka-storage.sh format`, metadata version `3.9-IV0`), the two log
  directories, the delegation-token secret key, the one-node `offsets.topic.*` and
  `transaction.state.log.*` settings, the **new group coordinator with the `consumer` rebalance
  protocol** of KIP-848 (`group.coordinator.rebalance.protocols=classic,consumer`) and, for the first
  time, an **authorizer** (`org.apache.kafka.metadata.authorizer.StandardAuthorizer` with
  `super.users=User:ANONYMOUS;User:admin;User:kafkatest`; the SASL user `acltest` is the one principal
  the ACLs really apply to). `docker-compose.yml` builds it as the container `kafka-3-9-2`; there is no
  ZooKeeper any more. The 2.8.2 image is gone from this branch (it lives on `2.x`).
- **The api keys 65–87** of `ApiKeys.java` @ 3.9.2 in `Protocol\ApiKeys` (DescribeTransactions … 
  ReadShareGroupStateSummary; key 56 keeps the published name `ALTER_ISR`), and **the error codes
  105–127** of `Errors.java` @ 3.9.2 in `KafkaException`, one exception class each
  (`TransactionalIdNotFoundException` … `VoterNotFoundException`); 106, 122 and 123 are
  `RetriableException`s in the Java client, and here. Read at the release tags: 105 is Kafka 3.0's,
  106 3.1's, 107–108 3.3's, 109–112 3.5's, 113 3.6's, 114–119 3.7's, 120 3.8's, 121–127 3.9's.
- **`tests/Fixture/ClientQuota` speaks the quota apis** (AlterClientQuotas, key 49) instead of shelling
  `kafka-configs.sh --zookeeper` into the container, which a KRaft node has no ZooKeeper for.

### Changed

- **The protocol document is `docs/protocol/3.9.md`**, renamed from `docs/protocol/2.8.md` with every
  `@see` reference, and its api-key table is the answer of the KRaft node on its client listeners:
  **61 keys** (0–3, 8–51, 55, 57, 60, 61, 64–66, 68, 69, 74, 75, 80, 81), pinned by
  `ApiVersionProbeTest`. The ZooKeeper-only apis 4–7 and 56 are not served on a KRaft node's client
  listener, the controller-only apis live on the controller listener, the client-metrics apis 71 and 72
  are hidden without a telemetry plugin and the early-access share-group apis 76–79 and 83–87 without
  `unstable.api.versions.enable`.
- **`docs/handoff/main.md` is the plan of the 3.x line** (the former `docs/handoff/3.x.md`); the record
  of the 2.x line is `docs/handoff/2.x.md`.
- **A KRaft node answers OffsetCommit v0 and OffsetFetch v0 with 35** (`Unsupported when using a
  Raft-based metadata quorum`), in every partition and before any validation.

### Changed (re-baseline wave T0, PRs #185, #186, #187, #188)

- **The inherited integration suite of the 2.x line is green on the KRaft node** — every one of the 74 tests the
  first run on `kafka-3-9-2` failed was measured on the node and either follows what it answers (the 2.8.2
  behaviour stays in `docs/protocol/3.9.md` as "on the 2.8.2 ZooKeeper broker", the node's answer next to it as a
  **(3.x)** item of "The 3.9.2 KRaft node of the 3.x line") or was dropped because a 3.x broker cannot do what it
  exercised. No wire change: the 676 inherited vectors replay unchanged. In short:
  - the record formats, Produce/Fetch, Metadata and the framing (#185): 61 rows in the api table, a `null` record
    set in a refused Fetch partition, an empty answer instead of a truncated message below `max_bytes`, the code
    **3** of an auto-created topic (no `LeaderNotAvailable` window: the controller elects the leader with the
    creation), the node id **1**, the sharded fetch-session cache whose top eight ids answer -1, and the long
    record-validation summary of `LogValidator`;
  - the cluster admin, token, SCRAM, election and reassignment apis (#186): delegation tokens are records of the
    metadata log (KIP-900, Kafka 3.6) that are renewable and describable a moment after the answer that created
    them, an expired token can still be expired, token visibility follows `super.users`, two changes of one user in
    one AlterUserScramCredentials request are 92 whatever the mechanism, the node finalizes `metadata.version` and
    refuses a feature update with 95 instead of 42, the reassignment and election refusals name the topic and the
    partition and an unknown partition is 3 where 2.8.2 answered 85;
  - the topic, config and quota admin apis (#187): the refusals of the KRaft controller in its own words, a
    `timeout_ms = 0` that no longer creates anything in the background (7 with a null message), the per-entry
    validation of DeleteTopics, the config refusals that moved between 40, 42 and -1, `message.format.version` as a
    default again, and a quota that reaches DescribeClientQuotas only after the broker replayed it, with the
    entities of the answer in no order;
  - the group apis, the offsets and the consumer on the new coordinator (#188): `group.initial.rebalance.delay.ms`
    is the 3000 ms default of `config/kraft/server.properties` (the first JoinGroup of a group is answered after
    3 s), a group the coordinator has just created is `Dead` for a moment before it is `Empty`, the `states_filter`
    of ListGroups is matched without regard to case or spaces, an `Empty` group outlives its last committed offset
    (a second OffsetDelete is 0, not 69), DescribeGroups answers the groups of a request in an order of its own,
    and the 12 of an oversized offset metadata is a 35 on a v0 commit because the KRaft refusal comes before any
    validation. `tests/Fixture/RawApiProbe::send()` takes a per-call timeout.
- **The two admin examples read the broker id off the cluster** instead of assuming the broker 0, which the KRaft
  node is not (`examples/create-topic.php`, `examples/admin-configs.php`); `examples/admin.php` says why
  `controlledShutdown()` is gone.

### Removed (re-baseline wave T0)

- **`AdminClient::controlledShutdown()`** (#186). ControlledShutdown (api key 7) is declared `["zkBroker",
  "controller"]` at Kafka 3.9.2: a KRaft node does not announce it on a client listener and closes the connection
  for every one of its four versions, so the method can never work against a node of this line. The message
  classes (`ControlledShutdownRequest`, `…RequestV0`, `…RequestV1`, `…RequestV2`, `…Response`, `…ResponseV2`,
  `ControlledShutdownResponsePartition`) and all four wire vectors stay: the api is on the 3.9.2 wire, on the
  controller listener, and `ApiVersionProbeTest` pins the refusal.
- **`offsets.storage = zookeeper`** (#188): `ClientConfig::OFFSETS_STORAGE`, `OFFSETS_STORAGE_KAFKA` and
  `OFFSETS_STORAGE_ZOOKEEPER`, the OffsetCommit v0 / OffsetFetch v0 branches of `Client::commitGroupOffsets()`,
  `Client::fetchGroupOffsets()` and `AdminClient::listGroupOffsets()`, and the option in the examples. A KRaft node
  answers both v0 requests with **35** in every partition (`requireZkOrThrow` @ 3.9.2), before any validation, so the
  ZooKeeper storage of Kafka 0.8.1 can never work against a node of this line. `OffsetCommitRequestV0`,
  `OffsetFetchRequestV0`, their responses and their vectors stay for the wire of the lines below.
- **The three integration tests that made the broker store a message format v0 or v1 log** (#185): KIP-724
  (Kafka 3.0) retired `message.format.version` and a 3.9.2 node stores the record batch v2 whatever the topic
  asks for. The message-set codecs v0/v1 and the Produce v0–v2 / Fetch v0–v3 versions of the client are
  **unchanged** — the node serves them and converts for them; only the direction changed: every conversion now
  happens on the way out, never on append.

### Kafka 3.0 — Added

The first milestone of the line (PRs #189, #190, #191, #192): what Kafka 3.0 added to the wire that a client
sends — two api keys, three version bumps — measured on the 3.9.2 KRaft node, with the KRaft re-measurement of the
admin, transaction and SASL surface next to it.

- **DescribeTransactions (key 65, v0) and ListTransactions (key 66, v0)** — the coordinator half of the KIP-664
  tooling that DescribeProducers (Kafka 2.8) opened: which transactional ids exist, in which state, and which
  partitions an open transaction still holds. `AdminClient::describeTransactions(array $transactionalIds)` looks
  every id up at its transaction coordinator (one frame per coordinator, the entries in request order, a per-entry
  error as the exception of its code in the place of the description) and answers `Admin\TransactionDescription`;
  `AdminClient::listTransactions(array $stateFilters = [], array $producerIdFilters = [], ?array
  &$unknownStateFilters = null)` asks every broker for the transactions of its own `__transaction_state`
  partitions and merges them into `Admin\TransactionListing`s; `Admin\TransactionState` is the string enum of the
  eight coordinator states plus `Unknown`. ListTransactions **v1** (the duration filter, Kafka 3.8) waits for its
  wave. Measured on the node: an id the coordinator does not know is **105** `TransactionalIdNotFoundException`
  per entry, an id the principal may not describe is **53** (answered by the coordinator lookup first), an empty
  id is **42** per entry, `beginTransaction()` never reaches the coordinator (an id is `Empty` until its first
  `AddPartitionsToTxn`), `transaction_start_time_ms` outlives the transaction it timed, the `state_filters` are
  matched **verbatim** where the `states_filter` of ListGroups is lower-cased, and the authorizer removes entries
  from a listing silently. 19 wire vectors in the two new files `describe-transactions.json` and
  `list-transactions.json`.
- **Offsets (ListOffsets) v7 (KIP-734)** — the special target time `OffsetsRequest::MAX_TIMESTAMP` (`-3`) asks for
  the offset of the record with the **largest timestamp** of a partition, which is the end of the log only while
  the timestamps of a log rise with its offsets; the answer is the one lookup of the api that carries a real
  timestamp next to the offset. `Consumer\KafkaConsumer::maxTimestampOffsets()` and
  `Admin\AdminClient::listMaxTimestampOffsets()` (the `OffsetSpec.maxTimestamp()` of the Java admin client) answer
  an `OffsetAndTimestamp` per partition, `null` for an empty log; the frame is the flexible v6 frame, kept by the
  new `OffsetsRequestV6`/`OffsetsResponseV6`. Measured on the node: a `-3` asked below version 7 — and any other
  negative target time the broker does not serve at that version (`-4`, `-5`) — is the per-partition **35** with
  the connection open (`timestampMinSupportedVersion` of `KafkaApis` @ 3.9.2, a check a 3.0 broker did not have),
  and `-3` on an empty log is the code 0 with `-1`/`-1`. 10 wire vectors.
- **OffsetFetch v8 and FindCoordinator v4 — the batched group apis of Kafka 3.0.** One OffsetFetch v8 asks for
  the committed offsets of **several consumer groups**, each with its own topic array and its own group-level
  error code (`Client::fetchOffsetsOfGroups()`, `AdminClient::listConsumerGroupOffsets()`; one group still travels
  as a one-element batch, and `OffsetFetchRequestV7`/`OffsetFetchResponseV7` keep the single-group frame); one
  FindCoordinator v4 (KIP-699) looks **several coordinators of one type** up and is answered one entry per key
  (`Client::getGroupCoordinators()`, `Client::getTransactionCoordinators()`,
  `Common\CoordinatorLookup::findCoordinators()`; `GroupCoordinatorRequestV3`/`GroupCoordinatorResponseV3` keep the
  single-key frame; key 10 keeps its published name and the coordinator struct takes the Java name
  `FindCoordinatorResponseCoordinator`). Measured on the node: a v4 answer carries **no** `error_message` (the
  versions below still answer `NONE`), the coordinator type 2 of the share groups (KIP-932) is **42** below
  version 6, an unknown group is not an error at v8 either, and an OffsetFetch v8 with an **empty** `groups`
  array is answered with **nothing at all** and strands the connection — the client refuses it with
  `InvalidRequestException` before it is sent. 20 wire vectors, and the new `BatchedGroupApiTest`.
- **The KRaft measurement of the admin, transaction and SASL surface** (documentation only): DescribeLogDirs
  applies the partition selection again (Kafka 3.7) and never reports the raft log, a log directory carries a
  KIP-858 `directory.id` and a replica move is a controller write, the KIP-890 partition verification refuses a
  transactional batch for a partition that was not added (48), `__transaction_state` and the producer id blocks
  come from the controller, DescribeCluster answers every principal and refuses with an empty operation bit
  field, DescribeProducers distinguishes three error messages, and the SASL exchange is unchanged while the
  `StandardAuthorizer` decides what follows it — plus the DescribeAcls v3 measurement the 3.3 ACL wave builds on
  (what the super users and the SASL user `acltest` are answered, and an annotated dump of a non-empty answer).
- **The load-sensitive tests of the suite wait for the node**: the fetch-session cache of a 3.9.2 node places a
  new session round-robin over eight shards, so `FetchSessionConsumerTest` fills it until eight requests in a row
  are refused; a ListOffsets and a DescribeConfigs of a fresh topic retry the retriable codes.

### Kafka 3.1 — Added

The second milestone of the line (PR #193): the one thing Kafka 3.1 added to the wire a client sends — the
**request side of the topic ids of KIP-516**, two years after Kafka 2.8 put the ids into the answers.

- **Fetch v13 (KIP-516)** — every topic of a request and of an answer is named by its **topic id**, in the topics
  array and in `forgotten_topics_data` alike; nothing else of the encoding changes (a version 13 fetch is the
  flexible version 12 body with 16 raw bytes of `uuid` where the compact name stood). `Cluster::topicIdOf()`,
  `topicIdsOf()` and `topicNameById()` keep the name ↔ id map, filled from every Metadata answer; `Client` and
  `KafkaConsumer` resolve the names of a request against it before every round — so a topic that was deleted
  and re-created travels under its new id after the metadata refresh its error triggers — and read the answer
  back through it; `FetchRequestV12`/`FetchResponseV12` keep the frame that names its topics. Measured on the
  node: the **100** `UnknownTopicId` of an id the node does not host is **per partition** (the zero uuid gets the
  same), and the **106** `FetchSessionTopicIdError` of Kafka 3.1 is produced in **both** directions of the mix —
  a session opened at v13 and continued at v12 by name, and the other way round — as a top-level error with the
  session id 0, after which the client starts over with a full fetch, as for the 70 and 71 of KIP-227.
- **Metadata v12 (KIP-516)** — the version at which a request **by topic id** is served: `MetadataRequest::byTopicIds()`
  and `AdminClient::describeTopicsByIds()`, the `describeTopics(TopicCollection.ofTopicIds(...))` of the Java
  admin client; `MetadataRequestV11`/`MetadataResponseV11` keep the version below it. Measured on the node: an
  id the cluster does not host is answered 100 with a **`null` topic name** (what version 12 made the field
  nullable for, `TopicMetadata::$topic` is nullable now), an answer of a by-id request fills the name in and is
  byte for byte the answer of the same question by name, a request that **mixes** ids and names loses the named
  half without a word, and a version below 12 asked by id is refused with a whole error response (zero brokers,
  a null cluster id, 42 per topic) — which is why `byTopicIds()` is a version 12 constructor.
- **The error code 106 `FetchSessionTopicIdError` is observed**, and `FetchSessionHandler` keeps the id → name
  map of its session (`rememberTopicIds()`, `getSessionTopicNames()`): an incremental fetch that moved no offset
  sends no topic at all and is still answered for the whole session.
- **Behaviour change**: a `FetchRequest` of version 13 needs the id of every topic it names and throws
  `UnknownTopicIdException` without one (the `$topicIds` parameter, also on `fromTopicPartitions()`);
  `FetchRequest::$topicPartitions` and `FetchResponse::$topics` are a plain **list** at that version, because an
  entry carries no name to index by. 20 wire vectors (10 of Fetch, 10 of Metadata), two new subsections of the
  grammar and three (3.x) items.

### Kafka 3.2 — Added

The third milestone of the line (PRs #194, #195): what Kafka 3.2 added to the wire a client sends — one field on
each half of the group membership protocol, and one on the answer of DescribeLogDirs.

- **JoinGroup v8 and LeaveGroup v5 (KIP-800)** — the `reason`: a nullable string at the end of a JoinGroup
  request ("the reason why the member (re-)joins the group") and one per entry of a LeaveGroup batch ("the reason
  why the member left the group"), a free text the coordinator writes into the log line of the rebalance it starts
  or of the member it removes, and nothing else; the client cuts it at 255 characters, as the Java client does.
  `Client::joinGroup()`, `Client::leaveGroup()`, `AdminClient::removeMembersFromConsumerGroup()` (whose entries
  say `member was removed by an admin` unless the caller gives a reason) and `KafkaConsumer::unsubscribe()` take
  one; the consumer sends the texts the Java consumer sends (`the consumer is being closed`, `the consumer
  unsubscribed from all topics`, `need to re-join with the given member-id: …`). Measured on the node: the reason
  of a join that starts no rebalance — the 79 of KIP-394, the static return below — is never logged; only
  `Preparing to rebalance group … ; client reason: …` and the explicit-leave line carry it.
- **JoinGroup v9 (KIP-814)** — the `skip_assignment` byte of the answer, between the leader id and the member
  id: `true` tells a **static** member that came back to a `Stable` group as its leader that the group keeps the
  assignment it already has, and `Consumer\Internals\ConsumerCoordinator` then publishes an **empty** assignment
  array with its SyncGroup and is answered the share the generation already agreed on. Measured on the node: the
  flag comes back in 10 ms with the generation unchanged, where the first join of the same instance waits the
  3 s of `group.initial.rebalance.delay.ms` — and **version 9 answers the new member id as the leader of a static
  takeover where version 8 answers the one it has just replaced** (`GroupMetadataManager.updateStaticMemberAndRebalance`
  @ 3.9.2 reads the leader after the swap on the KIP-814 branch and before it on the other), which is the one
  inherited assertion the version bump moved. The keep-behind classes `JoinGroupRequestV7`/`V8`,
  `JoinGroupResponseV7`/`V8`, `LeaveGroupRequestV4`, `LeaveGroupResponseV4` and `LeaveGroupRequestMemberV3` keep
  the frames below; 16 wire vectors.
- **DescribeLogDirs v3** — the one version Kafka 3.2 adds to the admin surface, and it is the answer that changes:
  "Version 3 adds the top-level ErrorCode field" of `DescribeLogDirsResponse.json` @ 3.2.3, an `int16` between
  `throttle_time_ms` and `log_dirs` next to the per-directory codes the version 0 already had (the request is the
  version 2 frame byte for byte). `DescribeLogDirsResponse::$errorCode` carries it and
  `AdminClient::describeLogDirs()` raises it as the exception of the whole request;
  `DescribeLogDirsRequestV2`/`DescribeLogDirsResponseV2` keep the frame below it. Measured on the node as the SASL
  user `acltest`: the refusal of a principal that may not `Describe` the `CLUSTER` resource is **31** with an empty
  directory array at v3 (17 bytes), the empty array alone at v2 (15 bytes) and at v0/v1 (16 bytes) — below the
  version 3 a refusal and a broker without a single log directory are the same bytes, which is why the Java admin
  client guessed the 31 from an empty map and, at 3.2.3, still does whenever the new field is 0. Nine wire vectors.

### Kafka 3.3 — Added

The fourth milestone of the line (PRs #196, #197), and the one that brings an api this package never had: the
ACL apis, measured against a real authorizer for the first time.

- **DescribeAcls (29), CreateAcls (30) and DeleteAcls (31) at the version 3 of Kafka 3.3**, the first line of this
  package to implement them — the lines below left them out because a broker without an `authorizer.class.name`
  answers all three with 54, and the 3.9.2 node of this line runs the `StandardAuthorizer` of KRaft with
  `super.users=User:ANONYMOUS;User:admin;User:kafkatest`, the SASL user `acltest` being the principal the acls
  are written for. `AdminClient::describeAcls()`, `createAcls()` and `deleteAcls()` with `Common\AclBinding`,
  `AclBindingFilter`, `ResourcePattern(Filter)`, `AccessControlEntry(Filter)` and the enumerations
  `ResourceType` (with the `USER` resource of KIP-373), `PatternType` and `AclPermissionType`;
  `Common\AclOperation` gains `CREATE_TOKENS` and `DESCRIBE_TOKENS`. The client sends the version 3 and keeps no
  lower one: an api that starts on this line gets the versions the node was measured at. Measured on the node:
  a write of an acl is a controller write forwarded in an Envelope (58), so a refused CreateAcls or DeleteAcls
  is worded with a request object of the controller listener (31 in every entry, where the DescribeAcls refusal
  is a top-level 31 naming the client's listener); a creation with the filter pattern type `ANY` is -1 with a
  null message, an empty resource name or a `CLUSTER` resource under another name the ordinary 42; a successful
  creation carries the empty error message, a successful deletion filter a null one; and one integration test
  proves an acl works — `acltest` is refused the Metadata of a topic with 29 until a `DESCRIBE` acl for it exists.
  30 wire vectors in the three new files `describe-acls.json`, `create-acls.json` and `delete-acls.json`.
- **UpdateFeatures v1 (KIP-778)** — the `upgrade_type` of a feature update in the place of the `allow_downgrade`
  boolean (`Admin\UpgradeType`: upgrade, safe downgrade, unsafe downgrade) and the top-level `validate_only`,
  with which the controller answers what it would do and writes nothing — `AdminClient::updateFeatures()` takes
  both and keeps its published signature (a boolean `true` is the safe downgrade, as the deprecated Java
  constructor reads it). Measured on the node without ever lowering its finalized `metadata.version`: the dry
  run really writes nothing, the upgrade type 0 has a refusal of its own (95, `The controller does not support
  the given upgrade type.`), a KRaft broker forwards the api to the controller (KIP-590), so the 41 of a
  non-controller is not producible and the 31 of an unprivileged principal carries the forwarded request, and the
  top-level `error_message` of the answer is the empty string on the node where the 2.8.2 broker wrote a null.
- **DescribeQuorum v0 and v1 (key 55, KIP-595 and KIP-836)**, implemented for the first time — the 2.8.2 broker
  of the line below did not serve the key on a client listener and the foundation only probed it:
  `AdminClient::describeMetadataQuorum()` answers the leader, the epoch, the high watermark and the state of
  every voter and observer of the metadata quorum, with the two timestamps KIP-836 added to a replica state
  (`Admin\QuorumInfo`, `Admin\ReplicaState`; the -1 of the wire is `null`). Measured on the node: the combined
  node is one voter and no observer, and the leader reports the **current time** in both of its own timestamps
  where the specification text announces -1 — `LeaderState.describeReplicaState` @ 3.3.2 sets both for the local
  id; an ordinary topic is 3 per partition with `leader_id` 0, an empty topic array nine bytes of answer, an
  unprivileged principal a top-level 31 with no topic.
- **DescribeLogDirs v4 (KIP-827)** — the `total_bytes` and `usable_bytes` of the volume each log directory sits
  on, at the end of every directory entry, as `Admin\LogDirInfo::$totalBytes`/`$usableBytes` with
  `hasVolumeSizes()` (-1 below v4); the two directories of the node are two paths of one filesystem and answer
  the same pair. `DescribeLogDirsRequestV3`/`ResponseV3` keep the frame of Kafka 3.2.
- **CreateDelegationToken v3 and DescribeDelegationToken v3 (KIP-373)** — a token for another principal
  (`AdminClient::createDelegationToken(..., $owner)`, the owner principal in front of the renewers) and the
  requester of a token in both answers (`Admin\TokenInformation::$tokenRequester`,
  `isIssuedForAnotherPrincipal()`); `TokenInformation::ownerOrRenewer()` counts the requester, as Kafka 3.3
  does. Measured on the node: asking for a token of another owner without the `CREATE_TOKENS` acl is **65**
  (`DelegationTokenAuthorizationFailed`), not 31; the requester sees the token in a describe but a renew or an
  expire by the requester is still **63** — the controller's `allowedToRenew` @ 3.9.2 counts the owner and the
  renewers only. The keep-behind `…RequestV2`/`…ResponseV2` classes keep the frames of Kafka 2.4 and 2.5.
- The error codes **107** `IneligibleReplica` and **108** `NewLeaderElected` of Kafka 3.3 belong to AlterPartition
  (56), a broker-to-controller api a client listener does not serve: declared at the foundation, never observed.
  73 wire vectors in all (26 of T1, 47 of T4): 836 in 54 files.

### The KIP-848 consumer — Added

The last wave of the line (PR #211): the new consumer protocol as real client apis, the owner's decision
taken as the last one — share groups stay out, and the codes 110 to 113 are now observed.

- **The new consumer protocol of KIP-848** (ConsumerGroupHeartbeat **68**, Kafka 3.5; ConsumerGroupDescribe
  **69**, Kafka 3.7). `ConsumerConfig::GROUP_PROTOCOL` switches a `KafkaConsumer` from the classic membership
  protocol to `consumer`, where a single api replaces JoinGroup, SyncGroup, Heartbeat and LeaveGroup and the
  **coordinator** computes the assignment: `Internals\ConsumerGroupHeartbeatCoordinator` is the second
  implementation of the new `Internals\ConsumerCoordinatorInterface`, honours the `heartbeat_interval_ms` the
  broker dictates, resolves the **topic ids** of an assignment through `Cluster`, acknowledges what it owns and
  rejoins with the epoch 0 and a fresh member id after a 110, a 113 or a 25. `ConsumerConfig::GROUP_REMOTE_ASSIGNOR`
  names the server-side assignor (`uniform` or `range` on a 3.9.2 node; anything else is the **112**), and
  `partition.assignment.strategy` is not used on this path at all. A rebalance is **incremental**:
  `ConsumerRebalanceListener::onPartitionsRevoked()` sees only the partitions that were really taken away and
  `onPartitionsAssigned()` only the ones that were added. The member epoch takes the place of the generation and
  travels in the OffsetCommit **v9** and OffsetFetch **v9** the client already sent.
  `AdminClient::describeConsumerGroups()` answers an `Admin\ConsumerGroupDescription` with the group epoch, the
  assignment epoch, the assignor and both assignments of every member; `describeGroups()` (key 15) answers a group
  of this type the state `Dead`, so a caller routes by the `group_type` of a ListGroups v5. Measured on the node:
  the 42 of a (re-)join whose `topic_partitions` is not the empty array, the **110** of a stale epoch in both
  directions, the **111**, the **112**, the **69** of a classic group, the static leave of the epoch **-2** that
  costs the group no rebalance, and the classic JoinGroup a `consumer` group still accepts — the online upgrade
  path of the KIP. `ConsumerGroupHeartbeatApiTest`, `ConsumerGroupDescribeApiTest`, `Kip848ConsumerTest` and
  `ConsumerGroupHeartbeatCoordinatorTest`; `tests/Fixture/ConsumerGroupHeartbeatProbe` is gone, the suites of
  the 3.6, 3.7 and 3.8 waves use the real classes.
- 50 wire vectors in the two new files `consumer-group-heartbeat.json` (34) and `consumer-group-describe.json`
  (14) and in `describe-groups.json` (2, the classic describe of a KIP-848 group): 1135 in 60 files, 466 of the
  line.

### Kafka 3.9 — Added

The ninth and last minor milestone of the line (PRs #208, #210, #209): the last five version bumps a 3.9.2
node serves a client, the probes of the apis this line leaves out, and the audit of the api-key table.

- **ApiVersions v4 (KAFKA-17011)** — the version this client sends. The bump declares no field: a supported
  feature whose `min_version` is 0 is reported only to a v4 request, which on a KRaft node is `kraft.version`
  (0…1) — a v3 answer of the same node omits the feature entirely, 499 against 518 bytes.
  `ApiVersionsRequestV3`/`ApiVersionsResponseV3` keep the flexible v3 of KIP-511. ApiVersions is not on the
  bootstrap or SASL path of this client, so the bump changed nothing there.
- **DescribeQuorum v2 (KIP-853)** — `AdminClient::describeMetadataQuorum()` sends it: a nullable `error_message`
  at the top level and per partition, a `replica_directory_id` in front of the log end offset of every replica
  state, and the top-level `nodes` array of node ids and listener endpoints. New `Admin\QuorumNode` and
  `Admin\RaftVoterEndpoint` (`QuorumInfo.Node` and `RaftVoterEndpoint` of the Java client), `QuorumInfo::$nodes`
  and `node()`, `ReplicaState::$replicaDirectoryId` and `replicaDirectoryIdAsString()`; the keep-behinds
  `DescribeQuorumRequestV1`/`DescribeQuorumResponseV1` and the V1 chain of data classes. Measured on the node:
  both error messages are the empty string, not null; `nodes` names the CONTROLLER listener of the node
  (`localhost:9096`), and is empty for a topic the node could not describe; every directory id is the zero uuid
  because a static voter set has none; the per-partition 3 finally carries words.
- **`BinarySchema::TYPE_UINT16`** — the `uint16` of the JSON specifications (two big-endian bytes read without
  the sign), added for the listener port of a DescribeQuorum v2 answer, the first and only field of the
  client-facing protocol that uses it.
- **The raft-voter apis 80/81 and the share-group apis 76–79, 83–87 are measured and stay out** (owner
  decision, no classes): AddRaftVoter and RemoveRaftVoter refuse every frame in three stages — the 104 of a
  foreign cluster id, the 42 of a voter key the api cannot read and the **35** of a well-formed one, because the
  node has finalized `kraft.version` at 0 (the static `controller.quorum.voters` of KIP-595); every share-group
  frame closes the connection. The codes 121–127 are therefore declared and unreachable on this node.
- **The final table audit of the line** — the rows of 1, 2, 10, 18, 55, 80–82 and 76–87 in both api-key
  tables, the "Implemented here" legend, the decision list (55 is implemented, 52–54 are probed), the
  finalized-features epoch that moves with every write, the error rows 121–127 and both release summaries:
  every "not yet implemented on this line" left in the tables is now the KIP-848 consumer of 68/69 alone.
- **Fetch v17 (KIP-853)** — the first version of this api that declares a tagged field inside a **partition** entry
  of the request: `ReplicaDirectoryId` (tag 0, a uuid), the log directory a **follower** keeps its replica of that
  partition in, so that the metadata quorum of KIP-853 can identify a voter by more than its node id while its
  membership changes. `Data\FetchRequestTopicPartition::$replicaDirectoryId` is the field,
  `FetchRequest::__construct(..., ?string $replicaDirectoryId = null)` the new last argument — one directory for
  every partition entry of the request — and `FetchRequest::getReplicaDirectoryId()` reads it back. A **consumer
  writes nothing**: the zero uuid is the default of the field and a tagged field whose value is its default is left
  off the wire, so the frame of `Client::fetchPartitions()` is the version 16 frame with another number in its
  header. `FetchResponse.json` @ 3.9.2 declares no field for the version at all. `FetchRequestV16`/`FetchResponseV16`
  keep the version below, `Data\FetchRequestTopicPartitionV12` and `Data\FetchRequestTopicV13` the entries that have
  no place for the tag.
- **ListOffsets v9 (KIP-1005)** — the flexible frame of the versions 6, 7 and 8 a fourth time and a fifth special
  target time, `OffsetsRequest::LATEST_TIERED_TIMESTAMP` (**-5**): the last offset of a partition that has been
  moved to remote storage, the upper end of the range whose lower end the `-4` of KIP-405 names.
  `AdminClient::listLatestTieredOffsets()` is the `OffsetSpec.latestTiered()` of the Java admin client, which has no
  consumer counterpart there and none here; `Client::fetchTopicPartitionOffsets()` and `AdminClient::listOffsets()`
  accept the target time as well. `OffsetsRequestV8`/`OffsetsResponseV8` keep the version of the local log start
  offset.
- **What the node answers** — the directory id of a Fetch v17 is parsed and **ignored** by every fetch of an
  ordinary topic (`KafkaRaftClient.handleFetchRequest` @ 3.9.2 is its only reader, for `__cluster_metadata` on the
  controller listener), so a real directory id, an unknown one and none at all are answered byte for byte the same,
  and a fetch that claims to be a follower is still the **6** or the **75** of the one-broker cluster with the
  leader hint of KIP-951; a fetch session opened at v16 is continued at v17 without a word; and the `-5` of
  KIP-1005 answers the error code **0** with the timestamp, the offset and the leader epoch `-1` on a node without
  remote storage — on a two-record log as on an empty one — where a `-5` at version 8, and any target time below
  `-5` at every version, is the per-partition **35**.
- **FindCoordinator v6** (KIP-932): `GroupCoordinatorRequest`/`GroupCoordinatorResponse` speak the version 6, the
  keep-behinds `GroupCoordinatorRequestV5`/`GroupCoordinatorResponseV5` the version below it. The version adds no
  field to either half of the api — it only lets `coordinator_type` say **2**, the share coordinator, which
  `GroupCoordinatorRequest::COORDINATOR_TYPE_SHARE` and `MIN_SHARE_VERSION` name. Share groups stay out of the
  line: no method ever sends that type, because a 3.9.2 node answers it the retriable **15**
  `CoordinatorNotAvailable` out of a branch that carries the KIP-932 to-do comment, where every version below 6
  answers the **42** `InvalidRequest`. The three coordinator types are authorized against three resources —
  `DESCRIBE` on the group, `DESCRIBE` on the transactional id and `CLUSTER_ACTION` on the cluster, the **30**,
  the **53** and the **31** of an unprivileged principal — and the version gate is checked before the acl.
  14 wire vectors.
- 42 wire vectors in all (12 of T1, 16 of T2, 14 of T3): 1085 in 58 files.

### Kafka 3.8 — Added

The eighth milestone of the line (PRs #204, #205, #206, #207): the first api of this protocol that pages, the
abortable transaction error of KIP-890 on every transaction api and on Produce, the group types of KIP-848 in a
listing, and the duration filter of ListTransactions.

- **DescribeTopicPartitions (key 75, v0, KIP-966)** — the api `kafka-topics.sh --describe` speaks since Kafka 3.8:
  the topics of a cluster with the **eligible leader replicas** next to every partition, and the first api of this
  protocol that **pages**. `AdminClient::describeTopicPartitions(array $topics = [], int $responsePartitionLimit =
  2000, ?DescribeTopicPartitionsCursor $cursor = null)` walks the `next_cursor` until the listing is complete and
  answers `Admin\TopicDescription` objects (with `Admin\TopicPartitionInfo`, the ELR fields included) or a
  `KafkaException` per topic, indexed by the topic name — the node sorts its answer and loses the order of the
  request. The Data classes `DescribeTopicPartitionsRequestTopic`, `…ResponseTopic`, `…ResponsePartition` and
  `…Cursor` carry the Java names @ 3.9.2, and the cursor is the first **nullable structure** of this protocol,
  which is neither a nullable array nor a nullable string: `MessageDataGenerator` writes an int8 in front of the
  structure, `-1` for null and `1` for present, and `Protocol\NullableStruct` is that notation in the schema engine
  (size, read and write in `BinarySchema`, a branch in the vector flattener). Measured on the node: the two ELR
  arrays come back **empty, never null** (`Replicas.toList(partition.elr)`); `next_cursor` names the **first
  partition that is missing**, the partition 0 of the next topic when a whole topic did not fit, and the
  `response_partition_limit` is clamped into `[1, max.request.partition.size.limit]` (the 2000 of the field's own
  default); an empty topic array is every topic of the cluster; `topic_authorized_operations` is reported
  **unasked** (3576 for `ANONYMOUS`); a refused cursor is a per-topic **42** for every topic of the request, a
  cursor past the end the code 0 with no partition, an illegal name the **17**, and the **empty** topic name a
  **-1** with a `NullPointerException` in the node's log; the authorizer answers a named topic `acltest` may not
  describe with the **29** behind the answered ones and the empty topic array with an empty list. 28 wire vectors
  in the new file `describe-topic-partitions.json`.
- **ListTransactions v1 (KIP-994)** — "Version 1: adds DurationFilter to list transactions older than specified
  duration": `AdminClient::listTransactions(..., int $durationFilterMs = -1)` sends v1;
  `ListTransactionsRequestV0`/`ResponseV0` keep the version below. Measured on the node: the reference time is
  `txnStartTimestamp`, the start of the transaction and not its last update, so a `CompleteCommit` entry is still
  selected by the age of the transaction it committed — and an id that has only ever run `InitProducerId` carries
  the start time **-1** and passes **every** filter. 8 wire vectors.
- **Produce v11 — the abortable transaction error of KIP-890.** Neither half of the api gains a field:
  `ProduceRequest.json` and `ProduceResponse.json` @ 3.8.1 carry the one comment "Version 11 adds support for
  new error code TRANSACTION_ABORTABLE (KIP-890)", so a version 11 frame is a version 10 frame with another
  number in its header. What the version states is that the client understands the **120**
  `TransactionAbortable` in a partition of the answer — "abort this transaction and carry on with the same
  transactional id" — where every version below it is answered the fatal-looking **48** `InvalidTxnState`.
  `KafkaApis.handleProduceRequest` @ 3.9.2 decides it on the api version alone and `AddPartitionsToTxnManager`
  maps the 120 back to the 48 for everybody below, which the two frames of `produce.*.unverified-partition`
  show side by side. `Client::produce()` sends v11; `ProduceRequestV10`/`ProduceResponseV10` keep the leader
  discovery of KIP-951 below it. The code needs no branch of its own in the producer:
  `TransactionManager::batchFailed()` takes it down the abortable path, `commitTransaction()` refuses and
  `abortTransaction()` leaves the producer usable with the same transactional id. The node finalizes no
  `transaction.version` feature at all (only `metadata.version` 21 and `kraft.version` 0), so the 120 of this
  line is the one the partition verification of KIP-890 part 1 produces, not the transaction protocol v2 of
  part 2, which is not in 3.9. 6 wire vectors.
- **ListGroups v5 (KIP-848)** — the `types_filter` of the request and the `group_type` of every entry of the
  answer. `AdminClient::listGroups()`, `listAllGroups()` and `listConsumerGroups()` take the types as a second
  optional argument next to the states (the `withTypes()` of the Java `ListGroupsOptions`), and
  `Protocol\Data\ListGroupResponseProtocol` carries the type with the constants `TYPE_CLASSIC`, `TYPE_CONSUMER`,
  `TYPE_SHARE` and `TYPE_UNKNOWN`. The field is what tells a classic group from a group of the new consumer
  protocol: the `protocol_type` of both is `consumer`. Measured on the node: the type strings are `classic` and
  `consumer`; the types filter is **parsed** and not compared (`CONSUMER` matches), a name the enum does not
  define is an **empty** answer and never an error, the two filters are combined with *and*, and the type
  outlives the group's members (an `Empty` KIP-848 group is still `consumer`). `ListGroupsRequestV4`,
  `ListGroupsResponseV4` and `ListGroupResponseProtocolV4` keep the version of KIP-518. 16 wire vectors.
- **FindCoordinator v5 (KIP-890)** — the version that promises the error code 120 `TransactionAbortable` and adds
  no field. Every coordinator lookup of `Client` and `AdminClient` sends it; `GroupCoordinatorRequestV4`/
  `GroupCoordinatorResponseV4` keep the version below it. No FindCoordinator of a 3.9.2 node ever answers the
  120 (`KafkaApis.handleFindCoordinatorRequest` @ 3.9.2 has no path that writes it). 6 wire vectors.
- **The transaction protocol of KIP-890 part 2, the wire half: InitProducerId v5, AddOffsetsToTxn v4, EndTxn v4
  and TxnOffsetCommit v4** are the versions the client now sends (`Client::initProducerId()`,
  `addOffsetsToTxn()`, `endTxn()` and `txnOffsetCommit()`, signatures unchanged, so `TransactionManager` and
  `KafkaProducer` speak them without a change of their own), and **AddPartitionsToTxn v5** gets its classes and
  vectors as a broker version (`Client::addPartitionsToTxn()` keeps v3; the node authorizes every version from 4
  on as `CLUSTER_ACTION` and answers a client the same top-level 31 at v5 as at v4). None of the five declares a
  field — every message specification @ 3.8.1 carries the one sentence "adds support for new error code
  TRANSACTION_ABORTABLE (KIP-890)" — so every new request is the frame of the version below it with another
  number in its header. Keep-behinds `InitProducerIdRequestV4`/`ResponseV4`, `AddPartitionsToTxnRequestV4`/
  `ResponseV4`, `AddOffsetsToTxnRequestV3`/`ResponseV3`, `EndTxnRequestV3`/`ResponseV3` and
  `TxnOffsetCommitRequestV3`/`ResponseV3`. Measured on the node: `transaction.version` is not even a *supported*
  feature of a 3.9.2 node (`TransactionVersion.latestProduction` at the metadata version 3.9-IV0 is TV_0; TV_1
  and TV_2 need IBP_4_0_IV0), so the behaviour half of KIP-890 part 2 — the implicit AddPartitionsToTxn of a
  Produce, the epoch bump on EndTxn — is out of reach on this line; the 120 of this node is the partition
  verification of part 1, gated on the api version, and exactly one of the four client bumps crosses that gate:
  a **TxnOffsetCommit v4** whose `__consumer_offsets` partition the open transaction does not hold is answered
  the **120** where the same commit at v3 is answered the **48** (`GroupCoordinator.handleTxnCommitOffsets` @
  3.9.2, `apiVersion >= 4`) — an abortable error instead of a fatal one for a producer that forgot its
  `addOffsetsToTxn()`. InitProducerId, AddOffsetsToTxn and EndTxn verify no partition and answer the 90, 49 and
  48 they always answered; AddPartitionsToTxn v5 answers the 120 of a `verify_only` as its v4 does; and an
  InitProducerId that carries the *last* epoch of a transactional id is read as the retry of a bump and answered
  the current pair with the code 0. 24 wire vectors.
- 88 wire vectors in all (36 of T1, 6 of T2, 22 of T3, 24 of T4): 1043 in 58 files.

### Kafka 3.7 — Added

The seventh milestone of the line (PRs #201, #202, #203): the leader discovery of KIP-951 in the answers of the
producer and the consumer, the endpoint type of a cluster without ZooKeeper, the fetch half of the KIP-848
member epoch, and the client-metrics apis of KIP-714 as wire classes.

- **Produce v10 and Fetch v16 — the leader discovery of KIP-951.** Neither raise touches the request
  (`ProduceRequest.json` @ 3.7.2: "Version 10 is the same as version 9 (KIP-951)"; `FetchRequest.json`: "Version
  16 is the same as version 15 (KIP-951)"): what both versions state is that the client understands where a
  refused partition went. Produce v10 is the first version of that api with tagged fields, and it declares two —
  the `current_leader` of a partition entry (`Data\ProduceResponseCurrentLeader`,
  `ProduceResponsePartition::$currentLeader`, a leader id and epoch, -1 by default) and the `node_endpoints` of
  the body (`Data\ProduceResponseNodeEndpoint`, `ProduceResponse::$nodeEndpoints`, keyed by node id) — which a
  broker writes for the 6 `NotLeaderForPartition` alone; Fetch v16 adds the missing half of a hint the partition
  entry has carried since v12, the top-level tagged `node_endpoints` (`Data\FetchResponseNodeEndpoint`,
  `FetchResponse::$nodeEndpoints`). Both hints travel into the context of the exception of the refused partition
  (`currentLeaderId`, `currentLeaderEpoch`, `currentLeaderHost`, `currentLeaderPort`); re-routing the batch or the
  fetch to that endpoint instead of refreshing the metadata is a change of the send and the fetch path that waits
  for a wave of its own. `ProduceRequestV9`/`ProduceResponseV9` (with `ProduceResponseTopicV8` and
  `ProduceResponsePartitionV8` for the entry without the tag) and `FetchRequestV15`/`FetchResponseV15` keep the
  versions below. Measured on the node: a one-broker cluster **does** write the hint — not into a Produce answer
  (an accepted batch is the v9 answer byte for byte, and the one produce refusal a client can provoke, the 3 of a
  partition that does not exist, carries nothing) but into a Fetch one, through the follower fetch of KIP-903: a
  v16 fetch that claims to be the replica 1 is refused 6 and the node names *itself*, `current_leader` 1 / 0 and
  `node_endpoints` 1 ⇒ 127.0.0.1:9092 with a null rack (110 bytes against the 76 of the same refusal at v15);
  `KafkaApis.handleFetchRequest` @ 3.9.2 fills the `current_leader` in from v16 on only, although the tag is on the
  wire since v12; the 75 of an epoch above the partition's carries no hint, the 74 is not reachable on a log that
  never left the epoch 0, and a fetch session opened at v15 continues at v16. 13 wire vectors, among them the one
  **constructed** Produce answer of the shape such a node can never write (the fifth constructed vector of the
  document).
- **DescribeCluster v1 — the endpoint type of KIP-919** (`AdminClient::describeCluster()`, which now sends the
  version 1). A cluster without ZooKeeper has brokers *and* controllers, and the new `endpoint_type` byte says
  which set the answer describes: `Admin\EndpointType` (`Unknown`, `Broker`, `Controller`, the Java enum of
  3.9.2) is the second, optional argument of the method, `ClusterDescription::$endpointType` and
  `describesControllers()` say which half came back, and `DescribeClusterRequestV0`/`DescribeClusterResponseV0`
  are the frames one version lower. The answer gains the error codes **114** `MismatchedEndpointType` and
  **115** `UnsupportedEndpointType`. Measured on the node: the type 2 on a broker listener is the 114 (`The
  request was sent to an endpoint of type BROKER, but we wanted an endpoint of type CONTROLLER`), the types 3
  and 0 are the 115 with the byte in their message, and the unknown type is checked *before* the mismatch;
  both refusals come back as the code and the message alone, with an empty cluster id, no broker and the schema
  default 1 in `endpoint_type`. And a KRaft node answers **a broker** in `controller_id`, not its controller —
  `KafkaApis` maps the cached controller id to a random alive broker, because the real controller has no
  endpoint a client may use.
- **OffsetFetch v9 (KIP-848)** — `OffsetFetchRequest.json` @ 3.7.2: "Version 9 is the first version that can be
  used with the new consumer group protocol (KIP-848). It adds the MemberId and MemberEpoch fields. Those are
  filled in and validated when the new consumer protocol is used." Every entry of the `groups` array of version
  8 gained a nullable `member_id` (default `null`) and a `member_epoch` (int32, default `-1`) behind its group
  id; the answer is unchanged and only promises two more codes, "can return STALE_MEMBER_EPOCH and
  UNKNOWN_MEMBER_ID errors when the new consumer group protocol is used", both of them **group-level** codes
  whose entry names no topic. `Client::fetchGroupOffsets()`, `Client::fetchOffsetsOfGroups()`,
  `AdminClient::listGroupOffsets()` and `AdminClient::listConsumerGroupOffsets()` keep their signatures and send
  v9 with the two fields at their defaults — what a classic member and every administrative reader send, and
  what `ConsumerGroup::validateOffsetFetch` @ 3.9.2 accepts without looking a member up; the new
  `Client::fetchGroupOffsetsAsMember()` and `OffsetFetchRequest::forMember()` fill them for a member of a KIP-848
  group, and `OffsetFetchRequestV8`/`OffsetFetchResponseV8`/`OffsetFetchRequestGroupV8` keep the batch of Kafka
  3.0 for a broker that does not serve 9. Measured on the node with a classic group and a KIP-848 group created
  by the hand-built ConsumerGroupHeartbeat of the 3.6 wave (now `tests/Fixture/ConsumerGroupHeartbeatProbe`): a
  **classic** group ignores both fields whatever they hold, a member of a **KIP-848** group is answered the
  **113** `StaleMemberEpoch` for an epoch below *and* above the one the coordinator holds — the epoch **-1** next
  to a member id included, because the check is skipped only when the id is null *and* the epoch is negative —
  and the **25** `UnknownMemberId` for an id the group does not hold, the empty string among them; a **null**
  member id with an epoch of 0 or more is the one shape the node answers **-1** `UnknownServerError` (a
  `NullPointerException` in the coordinator's member table, logged nowhere, the connection unharmed); a group the
  coordinator does not know is the **0** with the offset -1 of every version below, never the 69 that
  OffsetCommit v9 has; and a member of a KIP-848 group may **read** its offsets with version 8, where it may not
  **commit** below version 9. 20 wire vectors.
- **The client-metrics apis 71, 72 and 74 of KIP-714 as wire classes** — `GetTelemetrySubscriptionsRequest`/
  `Response`, `PushTelemetryRequest`/`Response`, `ListClientMetricsResourcesRequest`/`Response` and
  `Protocol\Data\ClientMetricsResource`. **Wire only by decision of the owner**: there is no client method and
  no telemetry emitter, because a PHP process that lives for one request has nothing to report over a
  300-second push interval. What is documented instead is what a node **without** a receiver plugin answers,
  measured with a `client-metrics` resource written and removed by hand: key 71 hands out a client instance id,
  the codecs zstd/lz4/gzip/snappy, the 300000 ms default interval, a 1048576-byte limit and an empty metric
  list, and refuses a second request inside the interval with 89; key 72 accepts an empty *and* a non-empty
  blob with 0 and drops it, and refuses a foreign subscription id with **117**, the zero instance id and a push
  after a `terminating` one with 42, an unknown codec with 76 and an oversized blob with **118**; key 74
  answers the names of the subscriptions and nothing about their contents. The keys 71 and 72 are hidden from
  the ApiVersions answer of such a node and answered all the same; 74 is listed like any other api. 26 wire
  vectors in three new files, 12 more of DescribeCluster v1.
- Of the error codes **114–119** of Kafka 3.7, the 114, 115, 117 and 118 are **observed**; 116
  `UnknownControllerId` and 119 `InvalidRegistration` are controller codes, declared and never producible on a
  client listener. 71 wire vectors in all (38 of T1, 20 of T3, 13 of T2): 955 in 57 files.

### Kafka 3.6 — Added

The sixth milestone of the line (PR #200): one version, raised without touching a byte, and the first code of the
KIP-848 protocol this package has observed.

- **OffsetCommit v9 (KIP-848)** — `OffsetCommitRequest.json` @ 3.6.2: "Version 9 is the first version that can be
  used with the new consumer group protocol (KIP-848). The request is the same as version 8"; the answer "is the
  same as version 8 but can return STALE_MEMBER_EPOCH when the new consumer group protocol is used and
  GROUP_ID_NOT_FOUND when the group does not exist for both protocols". The version is a promise about the answer:
  the same release renamed the second field of the request from `generation_id` to
  `generation_id_or_member_epoch` — the same four bytes with a second meaning — and a published identifier is
  never renamed for a rename in the Java client, so the field stays `OffsetCommitRequest::$generationId` and a
  classic member goes on writing its generation into it. `Client::commitGroupOffsets()`, and with it
  `KafkaConsumer::commitSync()`, the auto-commit and every offset the consumer's coordinator commits, send v9;
  `OffsetCommitRequestV8`/`OffsetCommitResponseV8` keep the flexible frame of Kafka 2.4 for a broker that does not
  serve 9. Measured on the node, with a classic group and a KIP-848 group created by a hand-built
  ConsumerGroupHeartbeat (key 68, the api of the last wave of this line): a classic member's own generation is
  **0** at v8 and v9 alike, a wrong generation the **22** at both — the branch of KIP-848 is chosen by the group
  type, never by the api version — an unknown member the **25**, and a group the coordinator does not know
  answers **22** at v8 but **69** `GroupIdNotFound` at v9, unless the frame carries the generation -1 and an empty
  member id, for which both versions create a *simple* group and answer 0. A member of a KIP-848 group commits
  its member epoch: the current one is **0**, an epoch below **or above** it the **113** `StaleMemberEpoch`
  (`validateMemberEpoch` @ 3.9.2 compares for equality), and a v8 commit of such a member is a **per-partition 35**
  ("OffsetCommit version 9 or above must be used by members using the modern group protocol"). 18 wire vectors:
  884 in 54 files.
- The error code **113** `StaleMemberEpoch` of Kafka 3.6 is **observed**, the one code of the KIP-848 protocol a
  classic client can be answered before the protocol itself is implemented.

### Kafka 3.5 — Added

The fifth milestone of the line (PRs #198, #199): the two wire halves of tiered storage, the replica state of a
follower fetch, and the first version of an api that a client does not send.

- **Fetch v14 (KIP-405)** — the frame of the version 13, field for field and byte for byte; the version is the
  promise to understand the error code **109** `OffsetMovedToTieredStorage`, with which a tiered broker answers a
  fetch of an offset that only the remote log still holds. Not producible on the node of this line (a container
  without remote storage answers the ordinary **1** at v13, v14 and v15 alike), so the code stays declared, and
  `Errors\OffsetMovedToTieredStorageException` is what a client sees on a tiered broker.
  `FetchRequestV13`/`FetchResponseV13` keep the version below it.
- **Fetch v15 (KIP-903)** — the first version of this api that takes a field out of the frame: the deprecated
  top-level `replica_id` (`versions: 0-14`) is replaced by the tagged `replica_state` of a replica id and a replica
  epoch (`Data\FetchRequestReplicaState`, `FetchRequest::$replicaEpoch`, `getReplicaId()`/`getReplicaState()`),
  written as the tag 1 of the body and left off the wire by a consumer (`-1`/`-1`), whose v15 frame is four bytes
  shorter than its v14 one; a follower's tagged structure is 13 bytes. Measured on the node: a follower fetch is
  authorized here (`super.users` contains `User:ANONYMOUS`) and the one broker is the leader, so
  `Partition.followerReplicaOrThrow` @ 3.9.2 answers **75** when the partition named a `current_leader_epoch` and
  **6** when it did not, for every replica id and epoch alike — the replica epoch is never reached, so a one-node
  cluster cannot show the fencing of KIP-903 itself; a `replica_state` of `-1` with an epoch and the debugging
  replica id `-2` are served as ordinary consumer fetches; and a fetch session opened at v13 continues at v15 with
  the same session id (the 106 fences the kind of name, not the version). `FetchRequestV14`/`FetchResponseV14`
  keep the version below it.
- **ListOffsets v8 (KIP-405)** — no field, and a fourth special target time: **`-4`**, the local log start offset
  (`OffsetsRequest::EARLIEST_LOCAL_TIMESTAMP`, accepted by `Client::fetchTopicPartitionOffsets()`,
  `KafkaConsumer::offsetsForTimes()` and `AdminClient::listOffsets()`; `AdminClient::listEarliestLocalOffsets()`
  asks it for a set of partitions). Measured on the node: without remote storage `-4` equals `-2` exactly on a
  filled log, and answers the offset **0** with the timestamp `-1` and the leader epoch 0 on an empty partition,
  where the `-3` of the max timestamp answers `-1`/`-1`; a `-4` at v7 and a `-5` (KIP-1005, Kafka 3.9) at v8 are
  both the per-partition **35**. `OffsetsRequestV7`/`OffsetsResponseV7` keep the version of the max timestamp.
- **AddPartitionsToTxn v4 (KIP-890) — the wire, not the client.** The version moves the transactional id, the
  producer id, the epoch and the topics into a `transactions` array with a `verify_only` flag per entry, and the
  answer into a top-level `error_code` plus one `results_by_transaction` entry per transaction
  (`AddPartitionsToTxnRequest.json` @ 3.5.2: *"Versions 3 and below will be exclusively used by clients and
  versions 4 and above will be used by brokers"*). `AddPartitionsToTxnRequest`/`Response` speak it, with the new
  structures `Data\AddPartitionsToTxnTransaction` and `Data\AddPartitionsToTxnResult`, `forTransactions()` (which
  refuses an empty batch — a 3.9.2 node never answers one, at v4 as at the OffsetFetch v8 of Kafka 3.0) and
  `resultOf()`, which reads one transaction out of either shape; `AddPartitionsToTxnRequestV3`/`ResponseV3` keep
  the frame **this client sends** — `Client::addPartitionsToTxn()` stays at v3 until the v5 of Kafka 3.8.
  Measured on the node: the key is announced `0-5` on the client listeners, a principal without `CLUSTER_ACTION`
  is refused the whole request with the **31**, a `verify_only` of a partition the transaction does not hold (or
  with no open transaction at all) is answered **120** `TransactionAbortable` — the code declared for Kafka 3.8,
  observed here for the first time — a stale epoch **90**, an unknown topic **3**, and a batch is answered in
  completion order; codes the requesting broker maps to the 48 and the 47 before a producer sees them.
- The error codes **110** `FencedMemberEpoch`, **111** `UnreleasedInstanceId` and **112** `UnsupportedAssignor` of
  Kafka 3.5 belong to ConsumerGroupHeartbeat (68), the KIP-848 api that is the last wave of the line: declared at
  the foundation, observed with that wave. 30 wire vectors in all (10 of T4, 20 of T2): 866 in 54 files.

### Kafka 3.4 — nothing on the wire a client sends

Kafka 3.4 (tag `3.4.1`) raised only the broker-to-broker and broker-to-controller apis of the ZooKeeper
migration (KIP-866: LeaderAndIsr v7, StopReplica v4, UpdateMetadata v8 and BrokerRegistration v1, none of them
served on a client listener) and added no error code and no client-facing version: verified at the tag against
`3.3.2`, message by message. The milestone is a record only; the client of the 3.3 milestone is the client of
the 3.4 one.

Unreleased — the 2.x line (Kafka 2.8.2)
---------------------------------------

The 2.x line, built on `main` on top of the finished 1.x line (branched off as `1.x`), **complete** and
branched off as `2.x`. Everything below was verified against a real Apache **2.8.2** broker
(`docker/kafka-2.8.2/` on that branch, four listeners) and documented in `docs/protocol/2.8.md` (on
`2.x`; on `main` the document continues as [docs/protocol/4.3.md](docs/protocol/4.3.md)). The record of
the line is [docs/handoff/2.x.md](docs/handoff/2.x.md).

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

- **The protocol document is `docs/protocol/3.9.md`**, renamed from `docs/protocol/1.1.md` with every
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
  "Broker quirks and observations" of [docs/protocol/4.3.md](docs/protocol/4.3.md): a **`consumer`**
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
- **New sections of [docs/protocol/4.3.md](docs/protocol/4.3.md)**: "The leader epoch (KIP-320)",
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
- **New sections of [docs/protocol/4.3.md](docs/protocol/4.3.md)**: "Reading from a follower (v11, KIP-392)" and
  "The authorized operations (v8, KIP-430)" — with the measured bitfields of an unsecured broker, **8096** for
  the cluster and **3576** for a topic, which are the *supported* operations of the resource type — plus the
  version paragraphs of the three apis and two more broker quirks.
- **IncrementalAlterConfigs (key 44) v0 (KIP-339)** — changes **single options** of a topic or a broker, where
  `AlterConfigs` carries the whole configuration and resets everything a caller forgot to send back (Kafka 2.3
  deprecated it for this one). `Admin\AlterConfigOp` carries the operations `SET`, `DELETE`, `APPEND` and
  `SUBTRACT` — the last two only for a list option — and `AdminClient::incrementalAlterConfigs()` answers a
  `KafkaException|null` per resource. A resource is validated and applied as a whole.

### Kafka 2.4

- **Flexible versions (KIP-482, Kafka 2.4) in the schema engine** — the compact types (`compact_string`,
  `compact_bytes`, `compact_[foo]`: an unsigned varint `length + 1`, `0` for `null`), the **unsigned varints**
  themselves (`Stream::readUnsignedVarint()`/`writeUnsignedVarint()`), the **tagged fields** of every structure
  (`Protocol\TaggedField`, declared in the scheme, written in ascending order of the tag and left out at their
  default), the **request header v2** and **response header v1** (`AbstractRequest::getHeaderVersion()` and
  `AbstractResponse::getHeaderVersion()`, the `ApiKeys.requestHeaderVersion()`/`responseHeaderVersion()` of the Java
  client), the `uuid` of KIP-516 (`TYPE_UUID`) and the one string that never becomes compact
  (`TYPE_STRING_NEVER_COMPACT`, the `client_id` of the header v2). Flexibility is a property of the **message**
  (`FlexibleSchemaInterface::isFlexible()`, `VERSION >= FLEXIBLE_VERSION`) that the engine hands down to every
  nested structure, so a `Data` class needs no change to serve a flexible version - the same scheme is written
  plainly in one version of an api and compactly in the next. Unknown tagged fields are kept
  (`PreservesUnknownTaggedFields`) so that a frame of a later broker survives a decode and encode round trip. The
  two exceptions of the protocol are two overrides: ControlledShutdown v0 has no client id in its header, and the
  **ApiVersions answer keeps the response header v0** whatever its version is (KIP-511).
- **`Protocol\InlineStruct`** in the schema engine — a scheme entry for a nested object the **specification does
  not have**, whose fields belong to the structure around it (`'owner' => new InlineStruct(KafkaPrincipal::class)`).
  It changes nothing in a plain version, where a group of fields and a nested structure are the same bytes, but in
  a flexible one it keeps the group from being given a tagged-field section of its own. The marker belongs to the
  **field**, not to the class: the same class is a real structure wherever the specification declares one. The first two places of Kafka 2.8.2 that need it are the **owner of a
  delegation token** (two flat fields of the answer that this package reads into a `KafkaPrincipal`, while the very
  same class *is* a structure in the request of that api) and the coordinator of a FindCoordinator v3 answer.
- **ApiVersions v3** (Kafka 2.4, KIP-511 + KIP-482 + KIP-584) — the first flexible frame this client sends: the
  request carries `client_software_name` = `lisachenko-kafka-client` and `client_software_version` = `2.8` as
  compact strings (a broker refuses a name that does not match `[a-zA-Z0-9](?:[a-zA-Z0-9\-.]*[a-zA-Z0-9])?` with
  the error code **42**, measured), and the answer carries the api table as a compact array and the features of
  KIP-584 as tagged fields (`supportedFeatures`, `finalizedFeaturesEpoch`, `finalizedFeatures` on
  `ApiVersionsResponse`, with `ApiVersionsSupportedFeature` and `ApiVersionsFinalizedFeature`). A ZooKeeper-backed
  2.8.2 broker answers exactly one of the three, the epoch `0`. `ApiVersionsRequestV2`/`ApiVersionsResponseV2` keep
  the version 2, and three new wire vectors show the flexible frames byte by byte (`apiversions.request.v3`,
  `apiversions.response.v3`, `apiversions.response.v3.invalid-software-name`).
- **The two partition-reassignment apis of KIP-455 (Kafka 2.4)** — `AlterPartitionReassignments`
  (key **45**, v0) and `ListPartitionReassignments` (key **46**, v0), the apis that took the
  reassignment of a partition out of the `/admin/reassign_partitions` znode and gave it to the
  controller. Both are **flexible from their version 0** and both must be sent to the **controller**,
  which answers `NOT_CONTROLLER` (41) otherwise; `AdminClient::alterPartitionReassignments()` and
  `AdminClient::listPartitionReassignments()` reload the cluster and retry once on it. A target
  replica set is a `Admin\NewPartitionReassignment` (or a plain list of broker ids) and `null`
  **cancels** the reassignment of that partition; the answer is one error per partition, so
  `alterPartitionReassignments()` returns `topic => [partition => KafkaException|null]` and throws
  only the top-level error. `Admin\PartitionReassignment` is what the list api answers, and it does
  the arithmetic of the three replica lists the broker sends (`getTargetReplicas()`,
  `getOriginalReplicas()`). Ten new wire vectors in the new
  `docs/protocol/vectors/alter-partition-reassignments.json` and
  `docs/protocol/vectors/list-partition-reassignments.json`, captured on the container together with
  every error the two apis answer: **85** `NO_REASSIGNMENT_IN_PROGRESS` (the code Kafka 2.4 added,
  and also what a cancellation of a partition that never existed answers, because the controller
  consults the reassignments in flight before it asks whether the partition exists), **39**
  `INVALID_REPLICA_ASSIGNMENT` for a replica set that names a broker that is not alive, and **3**
  `UNKNOWN_TOPIC_OR_PARTITION` per partition — never at the top level — for a topic the cluster does
  not have.
- **InitProducerId v2 and CreateDelegationToken v2 (Kafka 2.4)** — the first two **flexible** versions
  of apis this package already spoke: no field is added, the body of v1 is written compactly and the
  frame carries the request header v2 and the response header v1. `InitProducerIdRequest`/`Response`
  and `CreateDelegationTokenRequest`/`Response` are the v2 now (`TransactionManager` and
  `AdminClient::createDelegationToken()` send them), and the new `InitProducerIdRequestV1`,
  `InitProducerIdResponseV1`, `CreateDelegationTokenRequestV1` and `CreateDelegationTokenResponseV1`
  keep the version Kafka 2.0 bumped. Six new wire vectors, the v2 pair of each api and the two error
  answers of the token api.
- **OffsetDelete (key 47, v0, Kafka 2.4, KIP-496)** — the api that makes a coordinator forget the committed offsets
  of **single partitions** of a group without touching the group itself, where `deleteConsumerGroups()` can only
  throw the whole group away. It is the one thing Kafka 2.4 added **without** the flexible encoding — with
  SaslHandshake (17) it is one of the two client apis of this line the compact types never reach — and its answer
  opens with the **top-level error code before the throttle time**, which no other api of this protocol does.
  `OffsetDeleteRequest`/`OffsetDeleteResponse` with `OffsetDeleteRequestTopic`, `OffsetDeleteRequestPartition`,
  `OffsetDeleteResponseTopic` and `OffsetDeleteResponsePartition`, `Client::deleteGroupOffsets()` and
  **`AdminClient::deleteConsumerGroupOffsets(string $groupId, iterable $partitions)`** (the Java name), which takes
  `Common\TopicPartition`s, answers `topic => [partition => KafkaException|null]` and throws the group-level error,
  because an answer that carries one names no partition at all. Five new wire vectors in the new
  `docs/protocol/vectors/offset-delete.json`, and every state of a group measured on the container: an `Empty` group
  hands over every partition, a live `consumer` group answers **86** `GROUP_SUBSCRIBED_TO_TOPIC` for the topics its
  members are subscribed to and deletes the rest, a live group of any other protocol type is refused as a whole with
  **68** `NON_EMPTY_GROUP`, a group the coordinator does not know with **69** `GROUP_ID_NOT_FOUND`, and a topic the
  broker does not have is **3** per partition. Two quirks are pinned by tests: a partition that never had a
  committed offset is answered with **0** like one that had, and deleting the **last** committed offset of an
  `Empty` group transitions it to `Dead` and drops it, so the next request for it answers 69.
- **CreateTopics v4 (KIP-464)** — the frame of the versions 1 to 3, with a third way of describing a topic:
  `num_partitions` and `replication_factor` both -1 and an EMPTY assignment, which asks the broker for its own
  `num.partitions` and `default.replication.factor` (`NewTopic::withBrokerDefaults()`). The version check is
  **client-side**: a 2.8.2 broker resolves the -1 whatever version it was asked with, so
  `CreateTopicsRequest::__construct()` refuses the shape below version 4 exactly as
  `CreateTopicsRequest.Builder.build(version)` @ 2.8.2 does — a broker of Kafka 2.3 or below answers 37 or 38.
  `CreateTopicsRequestV3`/`CreateTopicsResponseV3` keep the version Kafka 2.0 added.
- **ElectLeaders v1 (KIP-460)** — an `election_type int8` in FRONT of the topic array of the request and a
  top-level `error_code int16` between the throttle time and the results of the answer.
  `Admin\ElectionType::UNCLEAN` is sendable now; the controller only acts on it for a partition whose leader is
  gone, so a healthy partition answers the same 84 as a preferred election.
  `ElectLeadersRequestV0`/`ElectLeadersResponseV0` keep the frames of Kafka 2.2.
- **The flexible versions of the admin apis (KIP-482)** — **CreateTopics v5**, **DeleteTopics v4**,
  **ElectLeaders v2**, **IncrementalAlterConfigs v1** and **ControlledShutdown v3** are the versions the client
  sends now. Four of the five are one `FLEXIBLE_VERSION` constant next to the `VERSION` and nothing else: the
  engine derives the compact strings and arrays, the tag buffer of every structure and the request header v2
  from it, and no nested DTO of these apis needed a change. Every version below keeps its own class —
  `CreateTopicsRequestV4`/`CreateTopicsResponseV4`, `DeleteTopicsRequestV3`/`DeleteTopicsResponseV3`,
  `ElectLeadersRequestV1`/`ElectLeadersResponseV1`, `IncrementalAlterConfigsRequestV0`/`…ResponseV0`,
  `ControlledShutdownRequestV2`/`ControlledShutdownResponseV2` and `CreateTopicsResponseTopicV1` — and
  `ControlledShutdownResponse` has a `VERSION` of its own for the first time.
- **The answer of CreateTopics v5 describes the topic it created (KIP-525)** — behind the error message it
  carries `num_partitions`, `replication_factor` and the whole `configs` array of the new topic (name, value,
  `read_only`, the config source of KIP-226 and `is_sensitive`), plus the tagged field 0
  `topic_config_error_code`. `Protocol\Data\CreateTopicsResponseTopicConfig` is the new entry DTO,
  `CreateTopicsResponseTopic` gained the four fields with `UNKNOWN = -1` for the versions below 5, and
  `Protocol\Kafka\Admin\CreatedTopic` is the value object the caller gets from the new
  `AdminClient::createTopicsWithResults()`; `AdminClient::createTopics()` keeps its contract (a
  `KafkaException|null` per topic) by answering their `error`.
- Measured on the container: a CreateTopics **v5** that asks for one partition with `retention.ms = 3600000` is
  answered with `num_partitions = 1`, `replication_factor = 1` and **26** configuration entries — 1
  `DYNAMIC_TOPIC_CONFIG` (the option the request set), 2 `STATIC_BROKER_CONFIG` (`segment.bytes` and
  `message.format.version`, which this image sets) and 23 `DEFAULT_CONFIG` — in 909 bytes; the same request with
  the -1/-1 of KIP-464 reports back the `num.partitions` of the image (3). A ControlledShutdown **v3** for the
  unknown broker 4242 is answered 8 in **13 bytes**, the smallest frame of the document: an empty compact array
  is one byte `01` and each tag buffer one byte `00`.
- **Ten wire vectors** of the five versions (one request/answer pair each, topic `t4-24f-vectors`), with
  annotated dumps in the flexible notation of the engine, and the five headings of `docs/protocol/3.9.md` moved
  to their new ranges.
- **LeaveGroup v3, the batch leave of KIP-345** — the request's single `member_id` is **replaced** by a list of
  member identities (`member_id` plus a nullable `group_instance_id` each), and the answer gains a matching
  member array behind its error code, one entry per member with an error code of its own. The top-level code is
  about the request alone and stays **0** even when every member was refused. `LeaveGroupRequest`/`Response` are
  version 3, `LeaveGroupRequestV2`/`LeaveGroupResponseV2` keep the single-member frame, and
  `Protocol\Data\LeaveGroupRequestMember`/`LeaveGroupResponseMember` are the entries.
- **`AdminClient::removeMembersFromConsumerGroup(string $groupId, iterable $members)`** — the api half of it, and
  the reason the version exists: a static member does not leave on its own, so an instance that is retired for
  good is removed by hand, **by its `group.instance.id`**. A member is named with `MemberToRemove::byInstanceId()`,
  `byMemberId()` or `byBoth()` (a plain string is an instance id, as in the Java admin client), and the result maps
  every member to its error or `null`, without throwing for a member that was refused.
- `Client::leaveGroup()` sends the one-element batch of a member that removes itself — optionally with its
  instance id — and reports the error of that entry as it always reported the error code of the answer; a static
  consumer still sends nothing at all when it is closed.
- Measured on the container: a static member removed by its instance id is gone **at once** (0), an instance id or
  member id the group does not have is **25** per entry (and so is every entry of a request against a group that
  does not exist, with the top-level code 0), a **pending** member of KIP-394 removes itself with **0**, and an
  empty batch is answered with 0 and an empty member array. Six wire vectors of the exchange were captured.
- **The flexible versions of the ten group apis (KIP-482)** — OffsetCommit **v8**, OffsetFetch **v6**,
  FindCoordinator **v3**, JoinGroup **v6**, Heartbeat **v4**, LeaveGroup **v4**, SyncGroup **v4**, DescribeGroups
  **v5**, ListGroups **v3** and DeleteGroups **v2**, every one of them "flexible, otherwise identical": the same
  fields with compact strings, byte arrays and arrays, a tagged-field section per structure, the request header v2
  and the response header v1. They are the versions this client sends now, and the version below each is kept as
  its own class (`OffsetCommitRequestV7`, `OffsetFetchRequestV5`, `GroupCoordinatorRequestV2`, `JoinGroupRequestV5`,
  `HeartbeatRequestV3`, `LeaveGroupRequestV3`, `SyncGroupRequestV3`, `DescribeGroupsRequestV3`/`V4`,
  `ListGroupsRequestV2`, `DeleteGroupsRequestV1` and their answers).
- **DescribeGroups v4 (KIP-345)** is the one version of the ten that changed a field: every member entry of the
  answer carries the nullable `group_instance_id` of its static member
  (`Protocol\Data\DescribeGroupResponseMember::$groupInstanceId`, with `DescribeGroupResponseMemberV0` for the
  versions below it). That is what lets `kafka-consumer-groups.sh --describe` - and `AdminClient::describeGroup()` -
  show which member id belongs to which instance.
- **The FindCoordinator answer is an `InlineStruct`** — `GroupCoordinatorResponseMetadata` groups the `node_id`,
  `host` and `port` that the specification writes flat into the answer, so the field declares the engine's
  `new InlineStruct(GroupCoordinatorResponseMetadata::class)` and the flexible version writes no tagged-field
  section of its own for the group.
- 22 wire vectors of the new versions were captured from the container, and the document gained the section
  "The flexible versions of the group apis (Kafka 2.4)".
- **Produce v8** (KIP-467) — the version that says **which** records of a refused batch were refused. Every
  partition entry of the answer gains a `record_errors` array of `[batch_index, batch_index_error_message]`
  pairs and an `error_message`, both behind the `log_start_offset`; the request body is unchanged.
  `ProduceRequest`/`ProduceResponse` are the v8 now and `Client::produce()` sends it,
  `ProduceRequestV7`/`ProduceResponseV7` keep the Kafka 2.1 pair and
  `ProduceResponsePartitionV5`/`ProduceResponseTopicV5` the partition entry of the versions 5 to 7. The new
  `Protocol\Data\ProduceResponseRecordError` is the Java `BatchIndexAndErrorMessage`, and
  `ProduceResponsePartition::$recordErrors` (keyed by the batch index) and `$errorMessage` carry the two fields.
  `Client::produce()` puts both into the context of the exception it raises for the partition, so an application
  that catches the **87** `InvalidRecordException` reads which record of its batch the broker refused instead of
  guessing. Measured on the container with a `cleanup.policy=compact` topic and a batch of three records, the
  second and the third of them without a key: the two are named with their batch indexes 1 and 2 and the
  message "Compacted topic cannot accept message without key…", the partition carries "One or more records have
  been rejected", and the very same request as a version 7 one comes back with the 87 and the offsets `-1`
  alone. Five wire vectors, the section "The record errors of a refused batch (v8, KIP-467)" of the protocol
  document, a broker quirk, unit tests and the new integration suite `RecordErrorsTest`.
- **Metadata v9** (KIP-482) — the first **flexible** version of an api the client sends: the same fields as
  version 8, written with compact strings and arrays, a tagged-field section at the end of every structure, the
  request header **v2** and the response header **v1**. `MetadataRequest`/`MetadataResponse` declare
  `FLEXIBLE_VERSION = 9` next to their `VERSION` and the engine of T1 does the rest;
  `MetadataRequestV8`/`MetadataResponseV8` keep the plain encoding, and `Client`/`Cluster` send version 9.
  One thing in this package had to change for it: the topics of the request were a plain array of **strings**
  here, while the specification has always declared them as a `[]MetadataRequestTopic` **structure** — invisible
  up to version 8, and one byte short per topic in a flexible frame, which the broker answers by closing the
  connection (measured). `Protocol\Data\MetadataRequestTopic` is that structure and `getTopics()` still answers
  the list of names. The vector pair `metadata.*.v9` is annotated down to every compact length and tag buffer.

### Kafka 2.5

- **JoinGroup v7 and SyncGroup v5 (KIP-559)** — the protocol of a generation travels in both directions now. The
  JoinGroup **answer** gained a nullable `protocol_type` in front of its `protocol_name`, and the name itself
  became nullable with it (`JoinGroupResponse::$protocolType`, `$groupProtocol`); the SyncGroup **request** and
  answer gained the same pair behind the `group_instance_id` (`SyncGroupRequest`'s two new trailing arguments,
  `SyncGroupResponse::$protocolType`/`$protocolName`). The JoinGroup request did not change at all - "Version 7
  is the same as version 6" - so `JoinGroupRequest` sends the version 6 bytes with the version field 7 and
  `JoinGroupRequestV6`/`JoinGroupResponseV6` and `SyncGroupRequestV4`/`SyncGroupResponseV4` keep the versions
  Kafka 2.4 added.
- **The two fields of a SyncGroup v5 are mandatory in the broker**: a version 5 that leaves either of them null
  is answered **23** (`InconsistentGroupProtocol`) by `areMandatoryProtocolTypeAndNamePresent()` before the
  coordinator is asked anything, and one that names a protocol the generation did not settle on gets the same
  code one check later. `Client::syncGroup()` therefore takes the pair as its last two arguments and
  `Consumer\Internals\ConsumerCoordinator` passes the `protocol_type` it joined with and the protocol name of the
  JoinGroup answer, exactly as `AbstractCoordinator` @ 2.8.2 does. Measured on the container: both null → 23 with
  a null type, a null name and an empty assignment **and the membership untouched**, a wrong name or a wrong type
  → 23 as well, the right pair → 0. And an error answer of a JoinGroup **v7** carries `null` in both fields where
  a v6 carries the empty string of `GroupCoordinator.NoProtocol` - measured on the 79 of KIP-394.
- **`Client::syncGroup()` falls back to the version 4 frame** when the caller names neither field: a version 5
  without them is refused with 23 before the coordinator reads the group, so a caller that does not know the
  protocol of the generation - every caller written before this release - keeps sending the version Kafka 2.4
  added, which carries no such field and which a 2.8.2 broker still serves. The consumer of this package always
  names both and always sends the version 5.
- **OffsetFetch v7 (KIP-447)** — the boolean `require_stable` behind the topic array asks the coordinator to hold
  back an offset whose transaction has not been committed yet and to answer that partition with the **retriable**
  error code **88** (`UnstableOffsetCommit`) instead. `OffsetFetchRequest` is the v7 now and takes the flag as its
  last argument (`forAllTopics()` too), `Client::fetchGroupOffsets()` passes it on, and
  `OffsetFetchRequestV6`/`OffsetFetchResponseV6` keep the flexible version of Kafka 2.4, whose answer can never
  carry the 88.
- **`KafkaConsumer` reads stable offsets when `isolation.level = read_committed`** — both for the positions it
  resolves after a rebalance and for `committed()` - and waits an 88 out with `retry.backoff.ms` before it
  reports it, because the code is retriable and the cure is the end of the transaction that holds the offset. A
  read-uncommitted consumer sends the flag off, which is the behaviour of every version below 7. (The Java
  consumer of 2.8 asks for stable offsets on *every* such fetch and uses an internal option to decide what an old
  broker costs; this client asks where an unstable offset could become a position.)
- Measured on the container, one partition of one group: with nothing pending both `require_stable = false` and
  `true` answer the committed offset; while a transactional commit of 42 is open the flag answers **88** with the
  offset -1 and the group-level code 0, and *without* the flag the same request answers the last **stable**
  offset 25 - never the pending one; once the transaction commits, the stable read answers 42.
- 14 wire vectors of the new frames were captured from the container, the document gained the sections "The
  protocol type and name of KIP-559 (Kafka 2.5)" and "Stable offsets and the 88 of KIP-447 (Kafka 2.5)", and the
  two integration suites `GroupProtocolApiTest` and `StableOffsetsApiTest` measure both halves against a real
  broker.
- **InitProducerId v3 and the epoch bump of KIP-360** — the request gains a `producer_id` and a `producer_epoch`,
  and the two meanings of that pair are the whole KIP: the **-1/-1** every version below sent asks for a new id,
  while the pair a producer already holds asks the coordinator for **the same id one epoch higher**. A
  transactional producer that hit an abortable error therefore no longer has to be thrown away: `abortTransaction()`
  rolls the transaction back and then sends that bump, and the sequence numbers of every partition start at zero
  again (`TransactionManager::transitionToAbortableError()` sets the flag, the abort acts on it — the model is
  `TransactionManager.bumpIdempotentEpochAndResetIdIfNeeded()` @ 2.8.2). An **idempotent** producer has no
  coordinator that remembers it and keeps asking for a new id with the -1/-1, as it always did.
  `InitProducerIdRequest`/`Response` are the version 3 with `InitProducerIdRequestV2`/`ResponseV2` for the
  flexible frame without the pair; `Client::initProducerId()` takes the two values as optional arguments.
- **TxnOffsetCommit v3 and the consumer group metadata of KIP-447** — the request gains a `generation_id`, a
  `member_id` and a `group_instance_id` (and is the first flexible version of the api), so that the group
  coordinator can refuse the commit of a consumer that has been rebalanced away instead of letting it write
  offsets for partitions another member owns by now. The new `Consumer\ConsumerGroupMetadata` carries the four
  values, `Consumer\KafkaConsumer::groupMetadata()` answers it — the `KafkaConsumer.groupMetadata()` of the Java
  client — and `KafkaProducer::sendOffsetsToTransaction()` takes it in place of the bare group id, which still
  works and means `ConsumerGroupMetadata::forGroup()`: the generation -1 with the empty member id, the "not a
  member" commit of every version below 3. `TxnOffsetCommitRequestV2`/`ResponseV2` keep the frame of version 2.
- **The flexible versions of five more apis (KIP-482)** — **CreatePartitions v2**, **SaslAuthenticate v2**,
  **RenewDelegationToken v2**, **ExpireDelegationToken v2** and **DescribeDelegationToken v2** are the versions
  the client sends now, each a `FLEXIBLE_VERSION` next to the `VERSION`, with a `…V1` class for the frame below
  it. The `throttle_time_ms` of the three token apis stays **last** — the flexible encoding moves no field — the
  SaslHandshake in front of a SaslAuthenticate stays the non-flexible **v1** (key 17 never became flexible), and
  the `owner` of a described token is the one `Protocol\InlineStruct` of these apis: two flat fields of the
  specification in one `KafkaPrincipal`, so no tag buffer follows it, while every renewer entry has one.
- Measured on the container (topic `t4-25-vectors`, group `t4-25-vectors-group`, transactional id
  `t4-25-vectors-tx`): a bump answers the same producer id with `epoch + 1`; a pair whose epoch the coordinator
  has left behind is **47** `InvalidProducerEpoch` with the id -1 and the epoch -1 (the 90 `ProducerFenced` of the
  Java client belongs to the version 4 of Kafka 2.7); a **null** transactional id with a real pair is answered 0
  with a brand-new id and the epoch 0, because `handleInitProducerId` returns on the null branch before it looks
  at the pair. A transactional commit of the current generation is **0**, of the generation before it **22**
  `IllegalGeneration` per partition, of a member id the group does not have **25** `UnknownMemberId`, and of the
  generation -1 with the empty member id **0**. SaslAuthenticate v2 was measured over SASL_PLAINTEXT and SASL_SSL
  and answers the session lifetime 0 in 22 bytes.
- **The assignment of CreatePartitions is a structure, and the flexible version says so** — `CreatePartitions
  Assignment` of `CreatePartitionsRequest.json` @ 2.8.2 has the single field `broker_ids`, which the plain
  encoding of the versions 0 and 1 cannot tell from the flat `list<list<int>>` this client wrote: a structure is
  neither counted nor delimited there. From the version 2 on it ends in a tagged-field section of its own, and a
  frame without it is one byte short — the broker **closes the connection without an answer**.
  `Protocol\Data\CreatePartitionsRequestAssignment` is that structure now, for every version, so the v0 and v1
  frames are unchanged to the byte and the v2 frame is accepted.
- **Twenty-one wire vectors** of the seven versions, with their annotated dumps, and two new subsections of
  [docs/protocol/4.3.md](docs/protocol/4.3.md): "Bumping the epoch (KIP-360)" and "The consumer group metadata of
  a transactional commit (KIP-447)".

*(Nothing on the Produce, Fetch, ListOffsets, Metadata and OffsetForLeaderEpoch apis: Kafka 2.5 raised none of
them. What the release added lives in the group and transaction apis.)*

### Kafka 2.6

- **ListGroups v4 (KIP-518)** — the api that had no request body at all for four versions got one: the
  `states_filter`, an array of group state names that bounds the answer to the groups in one of them, and every
  entry of the answer gained the `group_state` of that group. `ListGroupsRequest` is the v4 now and takes the
  states as its last argument, `ListGroupResponseProtocol::$groupState` carries the state (null for every version
  below 4, which does not report it), and `ListGroupsRequestV3`/`ListGroupsResponseV3` and
  `Protocol\Data\ListGroupResponseProtocolV0` keep the flexible version of Kafka 2.4.
- **`AdminClient::listGroups()` and `listAllGroups()` take the states**, and the new
  **`AdminClient::listConsumerGroups()`** - the name of the Java admin client - lists the groups of the whole
  cluster whose protocol type is `consumer` (`AdminClient::CONSUMER_PROTOCOL_TYPE`), with the same filter. Finding
  the empty groups of a cluster no longer costs one DescribeGroups per group.
- Measured on the container: an **empty** filter is every group the coordinator holds - 176 of them on the shared
  container, each with its state - `["Stable"]` answered exactly the one group of the capture,
  `["Empty", "PreparingRebalance"]` the other 175, and `["stable"]` in lower case answered the error code **0**
  with an **empty** array: `GroupCoordinator.handleListGroups` @ 2.8.2 compares the names with
  `states.contains(g.summary.state)`, so the filter is case sensitive and a name that is not a state at all is no
  match rather than an error. A null filter is the empty one (`KafkaApis`: "Handle a null array the same as
  empty"); this client sends the empty array.
- 4 wire vectors of the new frames were captured from the container, the document gained the section "The group
  states of KIP-518 (Kafka 2.6)", and the new integration suite `GroupStatesApiTest` measures the filter and the
  state of a group through its life.
- **DeleteRecords v2** (KIP-482) — the first **flexible** version of the api: not one field is added,
  `DeleteRecordsRequest.json` and `DeleteRecordsResponse.json` @ 2.8.2 both say "Version 2 is the first flexible
  version". The version 0 question travels with the request header **v2**, compact strings and arrays and a
  tagged-field section at the end of the body, of every topic entry and of every partition entry, and the answer
  with the response header **v1** and the same sections. `DeleteRecordsRequest`/`DeleteRecordsResponse` declare
  `FLEXIBLE_VERSION = 2` next to their `VERSION` and `AdminClient::deleteRecords()` sends it;
  `DeleteRecordsRequestV1`/`DeleteRecordsResponseV1` keep the plain frame of the versions 0 and 1. The vector
  pair `deleterecords.*.v2` was captured on the container and is annotated down to every compact length and tag
  buffer: the same question and the same answer as the version 1 pair in 48 and 42 bytes instead of 59 and 45.
- **Every integration suite deletes the topics it created** — `IntegrationTestCase` remembers every name it hands
  out in `uniqueTopicName()` and deletes them all with one DeleteTopics request in `tearDownAfterClass()`. Almost
  every suite of this repository created a unique topic per test and never deleted it again: **2505** of them were
  on the shared container after a few runs, 225 of a single class, across every ticket and every line of the
  cascade - which is how a log directory goes offline with "Too many open files" and leaves `__consumer_offsets`
  and `__transaction_state` without a leader. The cleanup is best effort: a topic that was never created, or that
  a test deleted itself, is answered with the error code 3 and ignored, and a broker that is gone never turns a
  green suite red.
- **The two client-quota apis of KIP-546** — `DescribeClientQuotas` (key **48**, v0) and `AlterClientQuotas`
  (key **49**, v0). Until Kafka 2.6 a client quota could only be read and written through **ZooKeeper**, which is
  why the quota fixture of the lines below shells `kafka-configs.sh` into the container; these two requests replace
  it. Both are **plain** frames although the release is well past KIP-482 —
  `DescribeClientQuotasRequest.json` @ 2.6.3 and @ 2.7.2 declare `"flexibleVersions": "none"` — and their flexible
  v1 is Kafka 2.8. `AdminClient::describeClientQuotas(ClientQuotaFilter)` answers `entity => [quota => value]` and
  `AdminClient::alterClientQuotas(array $alterations, bool $validateOnly = false)` reports one result per entity
  and throws nothing, because the alter api has **no top-level error code**. The value objects carry the Java
  names: `Admin\ClientQuotaEntity` (a map of entity type to name, `null` being the `<default>` entity),
  `ClientQuotaFilter`/`ClientQuotaFilterComponent` with the three match types, and
  `ClientQuotaAlteration`/`ClientQuotaAlterationOp` whose `remove()` is the api's `remove` flag. Nine new wire
  vectors in the new `docs/protocol/vectors/describe-client-quotas.json` and `alter-client-quotas.json`, and three
  broker behaviours measured: an unknown entity type is **35** (not 42) with `Custom entity type 'x' not
  supported` and a **null** entry array where a filter that matched nothing answers an empty one, an unknown quota
  key is **42** per entity, and an unknown **match type** escapes `DescribeClientQuotasRequest.filter()` as a plain
  `IllegalArgumentException` and comes back as the error code **-1** with a null message — a validation gap of the
  broker. `strict` was measured too: it excludes an entity that also carries a `user` part, and the non-strict
  filter keeps it.
- **`float64` in the schema engine** — `BinarySchema::TYPE_FLOAT64`, eight bytes of an IEEE 754 double in network
  order (`pack('E')`, `Type.FLOAT64` of the Java client), and the three double formats in the size table of
  `IO\AbstractStream`. The quota values of the keys 48 and 49 are the only fields of Kafka 2.8.2 that use it; like
  every fixed-width type it is untouched by the compact encoding.
- **DescribeConfigs v3 - the config type and the documentation of KIP-569** - the request gains an
  `include_documentation` boolean behind `include_synonyms`, and every entry of the answer gains a
  `config_type int8` and a nullable `documentation` string behind its synonyms. The type is the `ConfigDef.Type`
  of the option - `Admin\ConfigType` carries the ten values, with `CLASS` spelled `ConfigType::CLASS_NAME`
  because `class` is a reserved word in PHP and `nameOf()` answering the Java name - and the documentation is the
  prose of `ConfigDef.define(...)`. `ConfigEntry::$type` and `ConfigEntry::$documentation` hold them,
  `AdminClient::describeConfigs(..., bool $includeDocumentation = false)` asks for the second.
  `DescribeConfigsRequestV2`/`ResponseV2` (and `DescribeConfigsResponseConfigEntryV1`/`ResourceV1` below them)
  keep the frames of the versions 0 to 2.
- Measured on the container: the **type is filled whatever the flag says** -
  `ConfigHelper.createTopicConfigEntry` @ 2.8.2 ends in
  `.setDocumentation(configDocumentation).setConfigType(dataType.id)`, where only the documentation is behind
  `if (includeDocumentation)` - and an unrestricted request with the flag is **enormous**: every option of a topic
  is 26 entries and 9 284 bytes, every option of the **broker** 233 entries and 58 272 bytes, against 74 bytes for
  the one option the vectors of this document ask for. A client that wants a type or a help text should name its
  `configuration_keys`.
- **DescribeLogDirs v2 - the flexible version of KIP-482** - no field was added: the same request and answer in
  the compact encoding, with the request header v2 and a tagged-field section at the end of every structure. The
  nullable topic array of the request is the compact nullable one, so the `ff ff ff ff` that asks for every
  replica of every directory becomes a single `00` and the whole request is 15 bytes.
  `DescribeLogDirsRequestV1`/`ResponseV1` keep the frame of the versions 0 and 1.
- Measured on the container: a **2.8.2 broker ignores the selection of the request altogether** and groups every
  log of a directory into the answer, so a request that names one partition, one with an empty topic array and one
  with the null array are answered with the same frame - 16 391 bytes and 581 replicas when the version 2 pair was
  captured, where a 1.1.1 broker answered an empty array with 64 bytes. The empty array is therefore no longer the
  cheap "which disks does this broker have" of the 1.x line, and `describelogdirs.response.v2` is a **constructed**
  vector for the same reason as its version 1 counterpart.
- **Ten wire vectors**: the six DescribeConfigs v3 frames (the topics `t4-26-vectors` and `t4-26-own`) and the four
  DescribeLogDirs v2 frames (the topic `t4-26-logdirs`), each with its annotated dump, and the two headings moved
  to their new ranges.

### Kafka 2.7

- **Fetch v12** (KIP-482, KIP-595) — the first **flexible** version of the api and the version of the epoch
  validation in the fetch itself. The encoding half is the usual one: the request header **v2**, the response
  header **v1**, compact strings and arrays, a **compact record set** and a tagged-field section behind the body,
  every topic entry and every partition entry. `FetchRequest`/`FetchResponse` declare `FLEXIBLE_VERSION = 12` and
  are what `Client::fetchPartitions()`, `Client::fetchPartitionsWithSessions()` and `KafkaConsumer` send;
  `FetchRequestV11`/`FetchResponseV11`, `FetchRequestTopicV9`/`FetchRequestTopicPartitionV9` and
  `FetchResponseTopicV11`/`FetchResponsePartitionV11` keep the plain frames of the versions 9 to 11.
- **The `last_fetched_epoch` of KIP-595** — every partition entry of the request gained the epoch of the last
  record the fetcher really read, **between the fetch offset and the log start offset**
  (`FetchRequestTopicPartition::$lastFetchedEpoch`, `UNKNOWN_LAST_FETCHED_EPOCH = -1`). A caller states it as the
  **triple** `[offset, currentLeaderEpoch, lastFetchedEpoch]` in the partition map of `Client::fetchPartitions()`,
  next to the plain offset and the `[offset, currentLeaderEpoch]` pair of version 9
  (`FetchRequest::lastFetchedEpochOf()`); this client and the Java consumer @ 2.8.2 send -1 and keep detecting a
  truncation the KIP-320 way.
- **The three tagged fields of a partition entry of the answer** — `diverging_epoch` (tag 0,
  `Protocol\Data\FetchResponseDivergingEpoch`), `current_leader` (tag 1, `FetchResponseCurrentLeader`) and
  `snapshot_id` (tag 2, `FetchResponseSnapshotId`, KIP-630). The first of them is the answer to a fetch that
  stated an epoch the leader's log does not match: the largest epoch from which the two logs differ and the
  offset it ends at, which is the offset the fetcher truncates to. `FetchedPartition::$divergingEpoch` carries it
  to the caller; the other two belong to the raft replication of a KRaft quorum and are read from the protocol
  DTO.
- **The tagged `cluster_id` of the request** (tag 0 of the body, `FetchRequest::$clusterId`) — `null` by default,
  which leaves it off the wire; a broker that is given a wrong one answers **100** `INCONSISTENT_CLUSTER_ID`.
- Measured on the container and pinned by the new integration suite `FetchEpochValidationTest`: an epoch the log
  never had is **1** `OFFSET_OUT_OF_RANGE` with a high water mark of **-1**, not a divergence; a fetch offset
  **behind** the end of the stated epoch is the code **0**, an empty record set and the tagged
  `diverging_epoch [epoch 0, end_offset 1]`; the end of the epoch itself and a fetch that states no epoch at all
  are served as ever; and `current_leader` and `snapshot_id` stayed empty in every answer. Four wire vectors
  (`fetch.request.v12`, `fetch.response.v12` and the `last-fetched-epoch`/`diverging-epoch` pair), the section
  "Epoch validation in the fetch itself (v12, KIP-595)" of the protocol document and the two version 12 grammar
  blocks.
- **The two SCRAM credential apis of KIP-554 (Kafka 2.7)** — `DescribeUserScramCredentials` (key **50**) and
  `AlterUserScramCredentials` (key **51**), both flexible from their v0, which end the practice of writing SCRAM
  users into ZooKeeper by hand. The interesting half happens in the **client**: `Admin\UserScramCredentialUpsertion`
  takes a *password*, derives `Hi(password, salt, iterations)` of RFC 5802 with
  `hash_pbkdf2()` and sends only that, so the broker never sees a password and no answer can ever hand one out.
  `Admin\ScramMechanism` (an enum of the two mechanisms with their hash, key length and the 4096-iteration
  minimum), `ScramCredentialInfo`, `UserScramCredentialsDescription`, `UserScramCredentialDeletion` and the
  `UserScramCredentialAlteration` interface carry the Java names;
  `AdminClient::describeUserScramCredentials(?array $users = null)` and `::alterUserScramCredentials(array)` are
  the two calls. Eight new wire vectors in `describe-user-scram-credentials.json` and
  `alter-user-scram-credentials.json`, and the answers measured: **91** `RESOURCE_NOT_FOUND` for a user without a
  credential and for the deletion of one that is not there, **92** `DUPLICATE_RESOURCE` for the same user and
  mechanism twice, **93** `UNACCEPTABLE_CREDENTIAL` for an iteration count below the minimum — and the fact that
  the changes of one user are applied **all-or-nothing**, so one impossible change discards the possible ones of
  the same user.
- **UpdateFeatures (key 57, v0, Kafka 2.7, KIP-584)** — `AdminClient::updateFeatures(array $updates, int $timeoutMs =
  60000)`, sent to the **controller** and repeated once when it moved, with `Admin\FeatureUpdate` (whose `delete()` is
  the version level below 1 plus the downgrade flag the broker insists on). Its read half is not an api at all but the
  **tagged fields of the ApiVersions v3 answer**, which `AdminClient::describeFeatures()` reads into
  `Admin\FeatureMetadata`, `SupportedVersionRange` and `FinalizedVersionRange`. A ZooKeeper-backed 2.8.2 cluster
  finalizes nothing: it supports no feature, the finalized set is empty, the epoch is `0`, and every update is
  answered per feature with **42** `INVALID_REQUEST`. An empty update list is not refused at all — the controller
  iterates an empty collection and answers the top-level 0 with no result. Two new wire vectors in
  `update-features.json`.
- **The three topic apis of KIP-599** - **CreateTopics v6**, **DeleteTopics v5** and **CreatePartitions v3** - are
  the versions the client sends now. Not a field moves in two of them: the version is the client's promise that it
  understands the error code **89** `ThrottlingQuotaExceeded` and repeats the topics the *controller mutation
  quota* refused, where a broker used to **hold the request back** until the debt was paid.
  `AdminClient::onController()` sends the request again while any topic carries the 89, bounded by the `retries`
  of the configuration; the waiting is the KIP-219 promise of `Client`, which sleeps the rest of the throttle
  before the next request goes to that broker. Keep-behind classes for every version below.
- **DeleteTopics v5 also adds a field**: every topic result gains an `error_message` (a compact nullable string)
  behind its error code, and `Client::deleteTopics()` reads it into the exception of the topic, as CreateTopics
  has done since Kafka 0.11. `Protocol\Data\DeleteTopicsResponseTopicV0` is the result without it. Measured on
  the container: a **ZooKeeper** broker leaves the message `null` even for a topic the cluster does not have
  (error code 3) - `ZkAdminManager.deleteTopics` @ 2.8.2 builds its results from the error code alone.
- **The four transaction apis of KIP-588** - **InitProducerId v4**, **AddPartitionsToTxn v2**,
  **AddOffsetsToTxn v2** and **EndTxn v2** - change no byte either: what the version buys is the error code **90**
  `ProducerFenced` for a producer whose epoch the coordinator has left behind, where the versions below answer the
  47 `InvalidProducerEpoch`. The KIP splits the two meanings the 47 carried at once ("you were fenced" and "your
  epoch is one behind, ask for a bump" - the KIP-360 case of Kafka 2.5), and `TransactionManager` treats the 90
  exactly as the 47: a fatal state for the batch and for every transactional request. **The identifiers keep the
  published names**: the 47 stays `ProducerFencedException`, the 90 is `TransactionalProducerFencedException` -
  the Java client swapped those two names, this package does not move a published one.
- **The three transaction apis are NOT flexible in their version 2.** `"flexibleVersions": "none"` at the 2.7.2
  tag for AddPartitionsToTxn, AddOffsetsToTxn and EndTxn; the flexible version of each is the **3** that Kafka 2.8
  adds.
- Measured on the container with the transactional id `t4-27-vectors-tx`: an InitProducerId **v4** that names an
  epoch the coordinator really left behind is answered **90** with the id -1 and the epoch -1, while the pair one
  step behind the current epoch is still taken as the *retry of a bump* and answered 0 with the current pair
  (`prepareInitProducerIdTransit` @ 2.8.2 treats `expectedEpoch == currentEpoch - 1` as one), and the **EndTxn v2**
  of a producer that a second incarnation of the same id has fenced is answered **90** as well. The 89 of KIP-599
  cannot be produced on the shared container - it needs a `controller_mutation_rate` quota - so the retry is
  covered by the unit tests with a scripted controller.
- **Seventeen wire vectors**: the CreateTopics v6, CreatePartitions v3 and DeleteTopics v5 pairs (with the answer
  for a topic the cluster does not have) and the InitProducerId v4, AddPartitionsToTxn v2, AddOffsetsToTxn v2 and
  EndTxn v2 pairs (with the two frames that carry the 90), captured with the topic `t4-27-vectors` and the group
  `t4-27-vectors-group`, each with its annotated dump, and the seven headings moved to their new ranges.

### Kafka 2.8

- **DescribeCluster (key 60, v0, KIP-700)** — the cluster id, the controller and the brokers, asked for without
  naming a topic. Until this release a client that wanted those three had to send a request about *topics* with an
  empty topic array, which is what `AdminClient::describeClusterFromMetadata()` still does for a broker below 2.8;
  `AdminClient::describeCluster(bool $includeAuthorizedOperations = false)` sends the new api and answers
  `Admin\ClusterDescription`. The request is the **smallest of this protocol** - one boolean - and the flag is the
  only thing in it: without it `cluster_authorized_operations` is `Integer.MIN_VALUE`
  (`ClusterDescription::OPERATIONS_NOT_REQUESTED`), the default of the specification and not an error; with it the
  container answers **8096**, the seven `AclOperation` bits `AclEntry.supportedOperations(CLUSTER)` names, because
  it runs without an authorizer. Four wire vectors in the new `describe-cluster.json`.
- **DescribeProducers (key 61, v0, KIP-664)** — the first api of this protocol that reads out the
  `ProducerStateManager` of a partition, the table that makes the idempotent producer of KIP-98 work; before it,
  the only way to see which producer ids a partition remembered was `DumpLogSegments` on the broker's disk.
  `AdminClient::describeProducers(array $topicPartitions)` groups the partitions **by their leader** - the state
  lives in the log, so no other broker can answer for one - and reports `Admin\ProducerState` per partition, or
  the exception of that partition, because the api has no top-level error code. A partition the broker does not
  lead is **3**, and a partition that remembers no producer is the code 0 with an empty list.
- Measured on the container, and the surprise of the api: the two transaction fields of a producer state move on
  different beats. `current_txn_start_offset` is the base offset of the first batch of an open transaction and is
  cleared by its marker, but the `coordinator_epoch` next to it is written **by the marker**, so a producer whose
  very first transaction is still open is reported with the -1 of a producer that has none, and one whose second
  transaction is open still carries the epoch the previous marker wrote. An abort marker clears the first offset
  exactly as a commit marker does. `producer_epoch` is an `int32` in this api although it is an `int16` in every
  other one. Four wire vectors in the new `describe-producers.json`.
- **The flexible v1 of the two client-quota apis** — `DescribeClientQuotas` (48) and `AlterClientQuotas` (49) are
  the last pair of this line to become compact, two releases after the encoding arrived:
  `DescribeClientQuotas.json` @ 2.8.2 says `"validVersions": "0-1"` and `"flexibleVersions": "1+"`, where the same
  file @ 2.6.3 and @ 2.7.2 says `"flexibleVersions": "none"`. No field is added, so the base classes are the v1
  and `DescribeClientQuotasRequestV0`/`ResponseV0` and `AlterClientQuotasRequestV0`/`ResponseV0` keep the plain
  frame a 2.6 or 2.7 broker serves. The `float64` quota value is the same eight bytes in both encodings; what does
  differ is the `error_message` of a successful DescribeClientQuotas answer, which is the compact **empty** string
  in the v1 and the **null** string in the v0. Seven new wire vectors next to the plain ones in the two existing
  files.
- **Produce v9, ListOffsets v6 and OffsetForLeaderEpoch v4** (KIP-482) — the **flexible** versions of the three
  apis, and not one new field in any of them: the request header **v2**, the response header **v1**, compact
  strings and arrays, a tagged-field section behind the body and behind every structure, and - the one that
  matters for the size of a produce frame - a **compact record set**. The three pairs are what this client sends
  now; `ProduceRequestV8`/`ProduceResponseV8`, `OffsetsRequestV5`/`OffsetsResponseV5` and
  `OffsetForLeaderEpochRequestV3`/`OffsetForLeaderEpochResponseV3` keep the plain frames. Measured on the
  container: the same exchanges in 124/60, 53/54 and 48/46 bytes instead of 140/67, 56/57 and 51/49.
- **Metadata v10, the topic ids of KIP-516** — every topic entry of the answer carries the **16 raw bytes** of
  the id the topic was created with (`Common\TopicMetadata::$topicId`), and every topic entry of the request
  carries one in front of its name. The new `Protocol\Kafka\Common\Uuid` is the pair of functions that turns
  those bytes into the text form Kafka prints and back - a url-safe base64 without padding, 22 characters, which
  is what `Uuid.toString()` @ 2.8.2 and `kafka-topics.sh --describe` show, **not** the `8-4-4-4-12` hex of
  RFC 4122 - with `Uuid::ZERO` for the "no topic id" of the protocol. The **request** half of the KIP does not
  work on a 2.8.2 broker ("this functionality was not implemented on the server"), so this client writes the zero
  uuid and the real name, as the Java client does.
- **Metadata v11 (KIP-700)** — the version that takes a field **away**: `include_cluster_authorized_operations`
  is gone from the request and `cluster_authorized_operations` from the end of the answer, both declared `"8-10"`
  in the specification, because the cluster-wide question moved to the new DescribeCluster api (key 60). The
  per-topic bitfield is untouched. `MetadataRequest`/`MetadataResponse` are version 11 and what `Client` and
  `Cluster` send; `MetadataRequestV10`/`MetadataResponseV10` are the version to ask with when a caller wants the
  cluster bitfield from this api, and `MetadataRequestV9`/`MetadataResponseV9` and `TopicMetadataV8` keep the
  frames below it.
- Measured on the container: a Metadata v11 answers the real topic id (`PXjls0c8TyiESVVG2DNSKg` for the vector
  topic) and leaves `clusterAuthorizedOperations` at `NOT_REQUESTED` because the field is not on the wire at all,
  while the same question as a v10 with both booleans on answers the 8096 of the cluster and the 3576 of the
  topic. Ten wire vectors, the section "Topic ids (v10, KIP-516)" of the protocol document, the version 9/10/11
  grammar blocks of the four apis, the api-table rows 0, 2, 3 and 23, and the new integration suite
  `TopicIdsApiTest` - which also pins the point of KIP-516: a topic that is deleted and created again under the
  same name comes back with **another** id.
- **The topic ids of KIP-516 — CreateTopics v7 and DeleteTopics v6.** Every topic of a Kafka 2.8 cluster has an
  id that outlives its name: a topic that is deleted and created again under the same name is a different topic,
  and the id is what says so. The **answer** of CreateTopics v7 carries it between the name and the error code
  (`uuid`, 16 raw bytes that no length prefix precedes, so the compact encoding does not touch them) and
  `Admin\CreatedTopic::$topicId` hands it to the caller; the request of the version 7 is the frame of the
  version 6 byte for byte. `Protocol\BinarySchema::TYPE_UUID` is the new scheme type,
  `CreateTopicsResponseTopic::NO_TOPIC_ID` the `Uuid.ZERO_UUID` of a refused topic, and
  `CreateTopicsResponseTopicV5` the entry of the versions 5 and 6.
- **DeleteTopics v6 rebuilds the request**: the flat `[]TopicNames` of every version below becomes a
  `[]DeleteTopicState` (`Protocol\Data\DeleteTopicsRequestTopic`), a structure that names a topic **by its name** -
  with the zero id - **or by its id**, with a null name; the answer gains the `topic_id` and its name becomes
  nullable with it. `AdminClient::deleteTopics()` keeps taking names, and a caller that holds an id - from
  `CreatedTopic::$topicId` - hands `DeleteTopicsRequest` a `DeleteTopicsRequestTopic(null, $id)` instead.
  `DeleteTopicsRequestV5`, `DeleteTopicsResponseV5` and `DeleteTopicsResponseTopicV5` keep the flat frame of
  Kafka 2.7.
- **The error code 100** `UnknownTopicId` (`Common\Errors\UnknownTopicIdException`) gets its first sender here: an
  id no topic of the cluster carries is answered with it, where a *name* the cluster does not know is still the 3
  `UnknownTopicOrPartition`.
- **The seven first flexible versions of KIP-482 on this surface** — **DescribeConfigs v4**, **AlterConfigs v2**,
  **AlterReplicaLogDirs v2**, **WriteTxnMarkers v1**, **AddPartitionsToTxn v3**, **AddOffsetsToTxn v3** and
  **EndTxn v3**. Not one field moves in any of them: the request carries the header **v2** and the answer the
  header **v1**, every string and every array is compact, and every structure ends in a tagged-field section. The
  three transaction apis are the ones whose version 2 of Kafka 2.7 was still plain
  (`"flexibleVersions": "none"` at the 2.7.2 tag). Fourteen keep-behind classes hold the plain frames
  (`DescribeConfigsRequestV3`, `AlterConfigsRequestV1`, `AlterReplicaLogDirsRequestV1`, `WriteTxnMarkersRequestV0`,
  `AddPartitionsToTxnRequestV2`, `AddOffsetsToTxnRequestV2`, `EndTxnRequestV2` and their answers).
- **Measured on the container** with the topics `t4-28-vectors` and `t4-28-vectors-byid` and the transactional id
  `t4-28-vectors-tx`: a CreateTopics v7 answers a **real** id for a topic it created and the **zero** id for one
  it refused with 36 `TopicExists`; a DeleteTopics v6 **by name** is answered with the zero id, because
  `KafkaApis.handleDeleteTopicsRequest` @ 2.8.2 echoes the id of the *request* and looks up only the name of an
  id; a deletion **by id** is answered with the name the controller resolved; an unknown id is the **100**; and an
  entry that carries a name **and** a non-zero id fails the **whole** request with **42** `InvalidRequest`
  ("Topic name and topic ID can not both be specified."), leaving the other topics of that request untouched.
  The WriteTxnMarkers v1 frame is a real one as well - the container runs without an authorizer, so it serves the
  `ClusterAction` for anybody and the COMMIT marker was really appended.
- **Two engine details the topic ids needed**: an array that the scheme keys by a field - the topic results of
  DeleteTopics are keyed by their name - **appends** an entry whose key field is null instead of keying it by
  null, which is the only place of the protocol where a keyed name can be missing; and `Tests\Compliance`
  documents a `uuid` like every other raw byte field, as `{"$bytes": "<hex>"}`.
- **Twenty-two wire vectors**: the CreateTopics v7 pair with the answer of a refused topic, five DeleteTopics v6
  frames (by name, by id, and the 100 of an id no topic carries) and the flexible pairs of DescribeConfigs v4,
  AlterConfigs v2, AlterReplicaLogDirs v2, WriteTxnMarkers v1, AddPartitionsToTxn v3, AddOffsetsToTxn v3 and
  EndTxn v3, each with its annotated dump; the two DeleteTopics subsections of the document move into the
  DeleteTopics section, where they belong.

1.x — the 1.x line (Kafka 1.1.1)
--------------------------------

The 1.x line, built on top of the `0.11.x` line it was cascade-merged from. Everything below is
verified against a real Apache **1.1.1** broker (`docker/kafka-1.1.1/`, four listeners) and
documented in [docs/protocol/4.3.md](docs/protocol/4.3.md), whose **314** wire vectors
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

- **The protocol document is `docs/protocol/3.9.md`** (renamed from `docs/protocol/0.11.0.md`, as
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
- **The consistency pass over the documentation of the line** — `docs/protocol/3.9.md` no longer
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
is documented byte for byte in [docs/protocol/4.3.md](docs/protocol/4.3.md), whose 229 wire
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

- **`docs/protocol/3.9.md` is the grammar of Kafka 0.11.0.3.** The api-key table is the literal
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
byte in [docs/protocol/4.3.md](docs/protocol/4.3.md), with 120 wire vectors in
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
  `@see docs/protocol/3.9.md, section "…"` of the sources names a heading that exists.
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
- **The protocol document is `docs/protocol/3.9.md`** and describes Kafka 0.10.2.2: the api-key
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
