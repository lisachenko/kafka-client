# The 3.x line: Kafka 3.0 to 3.9 on a 3.9.2 KRaft node — the plan

**State: in development on `main`.** The line is built on the integration branch `feature/intelligent-fermi-nx4imu`
(the foundation commit is its first commit) and is merged into `main` as one pull request at the end; the release
notes are written above this plan when the line is complete, so that the file becomes the record of the line, as
[`docs/handoff/2.x.md`](2.x.md) is for the 2.x line.

## Decisions taken at the start of the line (the owner's)

1. **KRaft, one combined node** (`process.roles=broker,controller`, CONTROLLER on 9096 inside the container, the
   four client listeners 9092–9095 kept), `docker/kafka-3.9.2`, container `kafka-3-9-2`.
2. **The ACL apis 29–31 are in**, for the first time: the node runs the `StandardAuthorizer` with
   `super.users=User:ANONYMOUS;User:admin;User:kafkatest`, and the SASL user `acltest` is the principal the ACLs
   apply to.
3. **The KIP-848 consumer protocol is in** (keys 68, 69, OffsetCommit v9, OffsetFetch v9, ListGroups v5), as the
   last wave of the line, behind a `group.protocol=consumer` option of `KafkaConsumer`.
4. **Share groups (76–79, 83–87) are out** (early access). **Client metrics (71, 72, 74) wire only.** The raft/KRaft
   apis (52–55, 58, 59, 62–64, 67, 70, 73, 80–82) **probe only**. Tiered storage (ListOffsets v8, code 109) wire only.
5. **SASL stays PLAIN only**; SCRAM login only if a wave has spare capacity.
6. **Tags**: one per minor at the milestone commit, named after the last patch release of the minor (read off
   `git ls-remote --tags` before the first milestone; at the foundation: 3.0.2, 3.1.2, 3.2.3, 3.3.2, 3.4.1, 3.5.2,
   3.6.2, 3.7.2, 3.8.1, 3.9.2), created by the owner after the final merge into `main`.
7. Every attribution is read off the sources at the tag (message JSON, `ApiKeys.java`, `Errors.java`), never off
   memory; the node is the final authority.

## Tag points

The commit of each row is the `chore(3.x): Kafka 3.N complete` milestone on the integration branch; the rows are
filled in as the milestones land.

| Tag | Kafka | Milestone commit | Merged PRs |
|---|---|---|---|
| `3.0.2` | 3.0 | 617ebd8 | #185, #186, #187, #188 (the re-baseline wave T0), #189, #190, #191, #192 |
| `3.1.2` | 3.1 | 2a1a9b9 | #193 |
| `3.2.3` | 3.2 | 5e9bd7d | #194, #195 |
| `3.3.2` | 3.3 | fb9c5d0 | #196, #197 |
| `3.4.1` | 3.4 | 2c3793d | — (nothing a client sends: KIP-866 raised LeaderAndIsr, StopReplica, UpdateMetadata and BrokerRegistration only, verified at the tag) |
| `3.5.2` | 3.5 | 6f0b393 | #198, #199 |
| `3.6.2` | 3.6 | MILESTONE_SHA | #200 |
| `3.7.2` | 3.7 | _pending_ | |
| `3.8.1` | 3.8 | _pending_ | |
| `3.9.2` | 3.9 | _pending_ | |

## What the foundation measured

The node (`docker/kafka-3.9.2`, KRaft, metadata version `3.9-IV0`) lists **61 keys** on its client listeners:
0–3, 8–51, 55, 57, 60, 61, 64–66, 68, 69, 74, 75, 80 and 81 — the `broker` listener set of the JSON specifications
minus 71/72 (hidden while no client-telemetry receiver is configured, `ApiVersionsResponse.intersectForwardableApis`
@ 3.9.2) and minus 76–79 and 83–87 (`latestVersionUnstable`, hidden without `unstable.api.versions.enable`). The
ZooKeeper-only apis 4–7 and 56 are not served on a client listener of a KRaft node, so **ControlledShutdown, which
every line below probed, is gone from the table**, and `KafkaApis` @ 3.9.2 answers **OffsetCommit v0 and
OffsetFetch v0 with 35** (`metadataSupport.requireZkOrThrow`), which retires `offsets.storage = zookeeper` on this
line (the classes stay for the wire vectors of the lines below). The finalized-features epoch of the ApiVersions
answer is no longer 0. The table of `ApiKeys.java` @ 3.9.2 (88 keys) and the diff of every `*Request.json` between the
tags confirmed the plan below line by line; the retriable flag of the codes 105–127 is set on 106, 122 and 123 only.

# The original plan

This is the plan of the **3.x line**, written at the end of the 2.x line for the session that starts it. It is
what `docs/handoff/2.0.x.md` was for the 2.x line: the first ticket of the 3.x line renamed it to
`docs/handoff/main.md` (and the record of the 2.x line, the previous `docs/handoff/main.md`, to
`docs/handoff/2.x.md`), and at the end of the line the release notes are written above it, so that the file becomes
the record of the line. Read `CLAUDE.md`, `docs/CASCADE.md` and `docs/handoff/2.x.md` (the 2.x record: how the
last line was built, what its broker taught it and the pitfalls of its session) before this file.

Every number below was **derived from the Kafka sources at the release tags**, not remembered: the
`validVersions`, `flexibleVersions` and `listeners` of `clients/src/main/resources/common/message/*Request.json`
and the constants of `common/protocol/Errors.java` at `2.8.2`, `3.0.2`, `3.1.2`, `3.2.3`, `3.3.2`, `3.4.1`,
`3.5.2`, `3.6.2`, `3.7.2`, `3.8.1`, `3.9.2` and `4.0.0` (the script that produced them is a forty-line Python
diff of those files; rewrite it in the scratchpad of the session rather than trusting this document once the tags
move). The **KIP attributions** in this file are the ones the version comments of the JSON files name; where a
comment names none, the KIP is left out here and is the agent's to read off the sources at the tag — never off
memory (the 2.x session mis-attributed two versions from memory and corrected them from the tags).

## Where the line starts

* **`main` at `bc5dec5`** (the merge of PR #141), which is also the branch point of the protected branch **`2.x`**
  the owner created. `main` and `2.x` are identical there, so nothing has to be branched off: the 3.x line starts
  on `main` as the 2.x line started on `main` after `1.x` was branched off.
* Everything the 2.x line delivered: the flexible versions of KIP-482 in `Protocol\BinarySchema` (compact types,
  tagged fields, `InlineStruct`, `TYPE_UUID`, `TYPE_FLOAT64`, header v2/v1), the 64 api keys 0–64 with every
  client-facing one at the highest version a 2.8.2 broker serves, the error codes -1 … 104, the record batch v2
  with the zstd codec, the consumer with leader epochs and KIP-447, the transactional producer with the epoch bump
  of KIP-360 — see `docs/handoff/2.x.md` ("What was built, milestone by milestone").
* The tags of the 2.x line are the owner's: `2.0.1` … `2.8.2` on the nine milestone commits listed in
  `docs/handoff/2.x.md`. **Never move them.**

## The decision of the line: one branch, ten milestones, one broker

As in the 2.x line, **one branch carries the whole major**, built **one Kafka minor at a time**: every minor ends in
a gated milestone commit `chore(3.x): Kafka 3.N complete`, which is the tag point of the **last release of that
minor**. The tag names are the Kafka versions, as for every line before:

| Milestone | Tag | What the minor adds (client-facing; derived from the specs, see below) |
|---|---|---|
| 3.0 | `3.0.2` | keys 65 DescribeTransactions, 66 ListTransactions (KIP-664); ListOffsets v7 (max timestamp, KIP-734), OffsetFetch v8 (several groups in one request, KIP-709), FindCoordinator v4 (several keys in one request, KIP-699); error 105 |
| 3.1 | `3.1.2` | Fetch v13 and Metadata v12 (the topic ids of KIP-516 reach the request side); error 106 |
| 3.2 | `3.2.3` | JoinGroup v8/v9 and LeaveGroup v5 (the `reason` of KIP-800), DescribeLogDirs v3 (KIP-827 total/usable bytes) |
| 3.3 | `3.3.2` | the ACL apis 29–31 v3, DescribeLogDirs v4, CreateDelegationToken v3 and DescribeDelegationToken v3 (a token for another user, KIP-373), UpdateFeatures v1 (KIP-778 `validate_only`); errors 107, 108 |
| 3.4 | `3.4.1` | nothing client-facing (the ZooKeeper-migration bumps of the broker-to-broker apis, KIP-866) |
| 3.5 | `3.5.2` | key 68 ConsumerGroupHeartbeat (the new consumer protocol of KIP-848, preview); Fetch v14/v15 (KIP-903), ListOffsets v8 (earliest local offset of tiered storage, KIP-405), AddPartitionsToTxn v4; errors 109–112 |
| 3.6 | `3.6.2` | OffsetCommit v9 (the member epoch of KIP-848); error 113 |
| 3.7 | `3.7.2` | keys 69 ConsumerGroupDescribe (KIP-848), 71 GetTelemetrySubscriptions, 72 PushTelemetry, 74 ListClientMetricsResources (client metrics, KIP-714); Produce v10, Fetch v16 (leader endpoints, KIP-951), OffsetFetch v9 (KIP-848), DescribeCluster v1 (endpoint type, KIP-919); errors 114–119 |
| 3.8 | `3.8.1` | key 75 DescribeTopicPartitions; Produce v11, InitProducerId v5, AddPartitionsToTxn v5, AddOffsetsToTxn v4, EndTxn v4, TxnOffsetCommit v4 (the transaction protocol v2 of KIP-890), FindCoordinator v5, ListGroups v5 (group types of KIP-848), ListTransactions v1 (duration filter, KIP-994); error 120 |
| 3.9 | `3.9.2` | keys 76–79 share groups (KIP-932, **early access**), 80–82 raft voters (KIP-853), 83–87 share-group state (internal); Fetch v17, ListOffsets v9 (KIP-1005), FindCoordinator v6 (the share coordinator), ApiVersions v4; errors 121–127 |

(The tag of a minor is its *last patch release*, read off `git ls-remote --tags https://github.com/apache/kafka`
before the first milestone, never guessed. At the time of writing: 3.0.2, 3.1.2, 3.2.3, 3.3.2, 3.4.1, 3.5.2, 3.6.2,
3.7.2, 3.8.1, 3.9.2.)

The **broker of the line is Kafka 3.9.2**, the last 3.x release, which still serves every version 3.0 to 3.8
added (the JSON of 3.9.2 declares no lowest version above 0 for any api; that raise is Kafka 4.0's, see below). One
container verifies the whole line, as `kafka-2-8-2` did.

## The decision to take first: KRaft or ZooKeeper

Kafka 3.9 is the **last release that runs with ZooKeeper**; Kafka 4.0 is KRaft only. The 3.x line therefore
has to choose the mode of its container, and the choice decides which apis it can verify:

* A **KRaft** node (`process.roles=broker,controller`, one node, `kafka-storage.sh format` at start) serves the
  `broker` and `controller` listeners of the JSON `listeners` field — that is every client-facing api, **including
  the ones a ZooKeeper broker never lists**: 71/72/74 (client metrics), 75 (DescribeTopicPartitions), 76–79 and
  83–87 (share groups) and 80–82 (raft voters), plus the KRaft apis 52–55, 58, 59, 62–64 and 70/73 that the 2.x
  line could only probe. It has no ZooKeeper, so every `--zookeeper` tool of the briefs goes away (`kafka-configs.sh
  --bootstrap-server` for everything, `kafka-storage.sh --add-scram` for a SCRAM user at format time), and the
  `tests/Fixture/ClientQuota` shell-out of the lines below is replaced by the quota apis 48/49 the client already
  speaks.
* A **ZooKeeper** broker (`zkBroker` listener) is the continuity choice: the same container recipe as 2.8.2, the
  same tools, the same quirks. It does **not** serve keys 71, 72, 74, 75, 76–87 and lists 68/69 only with the new
  group coordinator enabled.

**Recommendation: KRaft, one combined node.** Kafka 4.x — the line after this one — is KRaft only, every api the 3.x
line adds is served there, and the ZooKeeper-only behaviours the 2.x document records (`ZkAdminManager`,
`--zookeeper` quotas) are the ones that disappear anyway. The cost is a rewritten `docker/kafka-3.9.2/start.sh` and
a first ticket that re-measures the quirks the 2.x document attributes to `KafkaApis` on a ZooKeeper broker (the
document keeps them as "on a ZooKeeper broker of 2.8.2" and adds the KRaft answer next to them). The owner decides;
the environment recipe below is written for KRaft with the ZooKeeper variant noted.

Whatever the mode, a **KRaft controller listener** must not be one of the four client listeners: keep PLAINTEXT
9092, SSL 9093, SASL_PLAINTEXT 9094, SASL_SSL 9095 for the clients and add CONTROLLER 9096 inside the container.

## What Kafka 3.x adds over 2.8.2, api by api

### The ApiVersions answer of a 3.9.2 node

The 88 keys 0–87 of `ApiKeys.java` @ 3.9.2, with the version range and the listeners of each request JSON. A
single KRaft node serves the `broker` and `controller` sets; a ZooKeeper broker the `zkBroker` set. The 2.8.2
column is the range the 2.x line implements.

| Key | API | Versions @ 3.9.2 | Listeners @ 3.9.2 | Versions @ 2.8.2 |
|---|---|---|---|---|
| 0 | Produce | 0-11 | zkBroker, broker | 0-9 |
| 1 | Fetch | 0-17 | zkBroker, broker, controller | 0-12 |
| 2 | ListOffsets | 0-9 | zkBroker, broker | 0-6 |
| 3 | Metadata | 0-12 | zkBroker, broker | 0-11 |
| 4 | LeaderAndIsr | 0-7 | zkBroker | 0-5 |
| 5 | StopReplica | 0-4 | zkBroker | 0-3 |
| 6 | UpdateMetadata | 0-8 | zkBroker | 0-7 |
| 7 | ControlledShutdown | 0-3 | zkBroker, controller | 0-3 |
| 8 | OffsetCommit | 0-9 | zkBroker, broker | 0-8 |
| 9 | OffsetFetch | 0-9 | zkBroker, broker | 0-7 |
| 10 | FindCoordinator | 0-6 | zkBroker, broker | 0-3 |
| 11 | JoinGroup | 0-9 | zkBroker, broker | 0-7 |
| 12 | Heartbeat | 0-4 | zkBroker, broker | 0-4 |
| 13 | LeaveGroup | 0-5 | zkBroker, broker | 0-4 |
| 14 | SyncGroup | 0-5 | zkBroker, broker | 0-5 |
| 15 | DescribeGroups | 0-5 | zkBroker, broker | 0-5 |
| 16 | ListGroups | 0-5 | zkBroker, broker | 0-4 |
| 17 | SaslHandshake | 0-1 | zkBroker, broker, controller | 0-1 |
| 18 | ApiVersions | 0-4 | zkBroker, broker, controller | 0-3 |
| 19 | CreateTopics | 0-7 | zkBroker, broker, controller | 0-7 |
| 20 | DeleteTopics | 0-6 | zkBroker, broker, controller | 0-6 |
| 21 | DeleteRecords | 0-2 | zkBroker, broker | 0-2 |
| 22 | InitProducerId | 0-5 | zkBroker, broker | 0-4 |
| 23 | OffsetForLeaderEpoch | 0-4 | zkBroker, broker | 0-4 |
| 24 | AddPartitionsToTxn | 0-5 | zkBroker, broker | 0-3 |
| 25 | AddOffsetsToTxn | 0-4 | zkBroker, broker | 0-3 |
| 26 | EndTxn | 0-4 | zkBroker, broker | 0-3 |
| 27 | WriteTxnMarkers | 0-1 | zkBroker, broker | 0-1 |
| 28 | TxnOffsetCommit | 0-4 | zkBroker, broker | 0-3 |
| 29 | DescribeAcls | 0-3 | zkBroker, broker, controller | 0-2 |
| 30 | CreateAcls | 0-3 | zkBroker, broker, controller | 0-2 |
| 31 | DeleteAcls | 0-3 | zkBroker, broker, controller | 0-2 |
| 32 | DescribeConfigs | 0-4 | zkBroker, broker, controller | 0-4 |
| 33 | AlterConfigs | 0-2 | zkBroker, broker, controller | 0-2 |
| 34 | AlterReplicaLogDirs | 0-2 | zkBroker, broker | 0-2 |
| 35 | DescribeLogDirs | 0-4 | zkBroker, broker | 0-2 |
| 36 | SaslAuthenticate | 0-2 | zkBroker, broker, controller | 0-2 |
| 37 | CreatePartitions | 0-3 | zkBroker, broker, controller | 0-3 |
| 38 | CreateDelegationToken | 0-3 | zkBroker, broker, controller | 0-2 |
| 39 | RenewDelegationToken | 0-2 | zkBroker, broker, controller | 0-2 |
| 40 | ExpireDelegationToken | 0-2 | zkBroker, broker, controller | 0-2 |
| 41 | DescribeDelegationToken | 0-3 | zkBroker, broker, controller | 0-2 |
| 42 | DeleteGroups | 0-2 | zkBroker, broker | 0-2 |
| 43 | ElectLeaders | 0-2 | zkBroker, broker, controller | 0-2 |
| 44 | IncrementalAlterConfigs | 0-1 | zkBroker, broker, controller | 0-1 |
| 45 | AlterPartitionReassignments | 0 | broker, controller, zkBroker | 0 |
| 46 | ListPartitionReassignments | 0 | broker, controller, zkBroker | 0 |
| 47 | OffsetDelete | 0 | zkBroker, broker | 0 |
| 48 | DescribeClientQuotas | 0-1 | zkBroker, broker | 0-1 |
| 49 | AlterClientQuotas | 0-1 | zkBroker, broker, controller | 0-1 |
| 50 | DescribeUserScramCredentials | 0 | zkBroker, broker, controller | 0 |
| 51 | AlterUserScramCredentials | 0 | zkBroker, broker, controller | 0 |
| 52 | Vote | 0-1 | controller | 0 |
| 53 | BeginQuorumEpoch | 0-1 | controller | 0 |
| 54 | EndQuorumEpoch | 0-1 | controller | 0 |
| 55 | DescribeQuorum | 0-2 | broker, controller | 0 |
| 56 | AlterPartition (was AlterIsr) | 0-3 | zkBroker, controller | 0 |
| 57 | UpdateFeatures | 0-1 | zkBroker, broker, controller | 0 |
| 58 | Envelope | 0 | controller, zkBroker | 0 |
| 59 | FetchSnapshot | 0-1 | controller | 0 |
| 60 | DescribeCluster | 0-1 | zkBroker, broker, controller | 0 |
| 61 | DescribeProducers | 0 | zkBroker, broker | 0 |
| 62 | BrokerRegistration | 0-4 | controller | 0 |
| 63 | BrokerHeartbeat | 0-1 | controller | 0 |
| 64 | UnregisterBroker | 0 | broker, controller | 0 |
| 65 | DescribeTransactions | 0 | zkBroker, broker | – |
| 66 | ListTransactions | 0-1 | zkBroker, broker | – |
| 67 | AllocateProducerIds | 0 | zkBroker, controller | – |
| 68 | ConsumerGroupHeartbeat | 0 | zkBroker, broker | – |
| 69 | ConsumerGroupDescribe | 0 | zkBroker, broker | – |
| 70 | ControllerRegistration | 0 | controller | – |
| 71 | GetTelemetrySubscriptions | 0 | broker | – |
| 72 | PushTelemetry | 0 | broker | – |
| 73 | AssignReplicasToDirs | 0 | controller | – |
| 74 | ListClientMetricsResources | 0 | broker | – |
| 75 | DescribeTopicPartitions | 0 | broker | – |
| 76 | ShareGroupHeartbeat | 0 | broker | – |
| 77 | ShareGroupDescribe | 0 | broker | – |
| 78 | ShareFetch | 0 | broker | – |
| 79 | ShareAcknowledge | 0 | broker | – |
| 80 | AddRaftVoter | 0 | controller, broker | – |
| 81 | RemoveRaftVoter | 0 | controller, broker | – |
| 82 | UpdateRaftVoter | 0 | controller | – |
| 83 | InitializeShareGroupState | 0 | broker | – |
| 84 | ReadShareGroupState | 0 | broker | – |
| 85 | WriteShareGroupState | 0 | broker | – |
| 86 | DeleteShareGroupState | 0 | broker | – |
| 87 | ReadShareGroupStateSummary | 0 | broker | – |

Every api the line adds is **flexible from its version 0** (all 23 new keys declare `"flexibleVersions": "0+"`),
and no existing api changes its flexible range in 3.x — the engine of the 2.x line needs no new encoding for this
line. The one type to check at the first ticket: whether any 3.x field uses a type the engine lacks (`int8`
enums, `bool` arrays, nested tagged structures are all already there; grep the JSON for `"type":` values and
compare with `BinarySchema`).

### The version bumps, minor by minor

The bumps of the *client-facing* apis (the replication and raft apis 4–7, 52–54, 56, 58, 59, 62, 63, 67, 70, 73,
80–87 are probe-only, as in the 2.x line). "Client sends" is the version the line implements at the end:

| Minor | Bumps |
|---|---|
| **3.0** | ListOffsets v6→**v7**, OffsetFetch v7→**v8**, FindCoordinator v3→**v4**; new 65, 66 (67 controller-only) |
| **3.1** | Fetch v12→**v13**, Metadata v11→**v12** |
| **3.2** | JoinGroup v7→**v9** (two versions in one release), LeaveGroup v4→**v5**, DescribeLogDirs v2→**v3** |
| **3.3** | DescribeAcls/CreateAcls/DeleteAcls v2→**v3**, DescribeLogDirs v3→**v4**, CreateDelegationToken v2→**v3**, DescribeDelegationToken v2→**v3**, UpdateFeatures v0→**v1**, DescribeQuorum v0→**v1** |
| **3.4** | broker-to-broker only (LeaderAndIsr v7, StopReplica v4, UpdateMetadata v8, BrokerRegistration v1) |
| **3.5** | Fetch v13→**v15** (two versions), ListOffsets v7→**v8**, AddPartitionsToTxn v3→**v4**; new 68 |
| **3.6** | OffsetCommit v8→**v9** |
| **3.7** | Produce v9→**v10**, Fetch v15→**v16**, OffsetFetch v8→**v9**, DescribeCluster v0→**v1**; new 69, 71, 72, 74 (70, 73 controller-only) |
| **3.8** | Produce v10→**v11**, FindCoordinator v4→**v5**, ListGroups v4→**v5**, InitProducerId v4→**v5**, AddPartitionsToTxn v4→**v5**, AddOffsetsToTxn v3→**v4**, EndTxn v3→**v4**, TxnOffsetCommit v3→**v4**, ListTransactions v0→**v1**; new 75 |
| **3.9** | Fetch v16→**v17**, ListOffsets v8→**v9**, FindCoordinator v5→**v6**, ApiVersions v3→**v4**, DescribeQuorum v1→**v2**; new 76–79, 80–82, 83–87 |

Two things the table hides and the agents must measure: a version that changes **nothing on the wire** (the KIP-219
style "the client promises to understand a new error code") versus one that adds fields, and the apis whose new
version is **only meaningful with a feature the container has to enable** (68/69 need the new group coordinator,
71/72/74 need a client-metrics plugin or answer 115 `UnsupportedEndpointType`/no subscriptions, 76–79 need
`share.version` and the early-access flag of KIP-932, 75 needs a KRaft broker).

### The error codes 105–127

| Code | Constant | Class of the Java client | Added in | Message |
|---|---|---|---|---|
| 105 | TRANSACTIONAL_ID_NOT_FOUND | TransactionalIdNotFoundException | 3.0 | The transactionalId could not be found. |
| 106 | FETCH_SESSION_TOPIC_ID_ERROR | FetchSessionTopicIdException | 3.1 | The fetch session encountered inconsistent topic ID usage. |
| 107 | INELIGIBLE_REPLICA | IneligibleReplicaException | 3.3 | The new ISR contains at least one ineligible replica. |
| 108 | NEW_LEADER_ELECTED | NewLeaderElectedException | 3.3 | The AlterPartition request successfully updated the partition state but the leader has changed. |
| 109 | OFFSET_MOVED_TO_TIERED_STORAGE | OffsetMovedToTieredStorageException | 3.5 | The requested offset is moved to tiered storage. |
| 110 | FENCED_MEMBER_EPOCH | FencedMemberEpochException | 3.5 | The member epoch is fenced by the group coordinator. |
| 111 | UNRELEASED_INSTANCE_ID | UnreleasedInstanceIdException | 3.5 | The instance ID is still used by another member in the consumer group. |
| 112 | UNSUPPORTED_ASSIGNOR | UnsupportedAssignorException | 3.5 | The assignor or its version range is not supported by the consumer group. |
| 113 | STALE_MEMBER_EPOCH | StaleMemberEpochException | 3.6 | The member epoch is stale. |
| 114 | MISMATCHED_ENDPOINT_TYPE | MismatchedEndpointTypeException | 3.7 | The request was sent to an endpoint of the wrong type. |
| 115 | UNSUPPORTED_ENDPOINT_TYPE | UnsupportedEndpointTypeException | 3.7 | This endpoint type is not supported yet. |
| 116 | UNKNOWN_CONTROLLER_ID | UnknownControllerIdException | 3.7 | This controller ID is not known. |
| 117 | UNKNOWN_SUBSCRIPTION_ID | UnknownSubscriptionIdException | 3.7 | Client sent a push telemetry request with an invalid or outdated subscription ID. |
| 118 | TELEMETRY_TOO_LARGE | TelemetryTooLargeException | 3.7 | Client sent a push telemetry request larger than the maximum size the broker will accept. |
| 119 | INVALID_REGISTRATION | InvalidRegistrationException | 3.7 | The controller has considered the broker registration to be invalid. |
| 120 | TRANSACTION_ABORTABLE | TransactionAbortableException | 3.8 | The server encountered an error with the transaction. The client can abort the transaction to continue using this transactional ID. |
| 121 | INVALID_RECORD_STATE | InvalidRecordStateException | 3.9 | The record state is invalid. |
| 122 | SHARE_SESSION_NOT_FOUND | ShareSessionNotFoundException | 3.9 | The share session was not found. |
| 123 | INVALID_SHARE_SESSION_EPOCH | InvalidShareSessionEpochException | 3.9 | The share session epoch is invalid. |
| 124 | FENCED_STATE_EPOCH | FencedStateEpochException | 3.9 | The share coordinator rejected the request because the share-group state epoch did not match. |
| 125 | INVALID_VOTER_KEY | InvalidVoterKeyException | 3.9 | The voter key doesn't match the receiving replica's key. |
| 126 | DUPLICATE_VOTER | DuplicateVoterException | 3.9 | The voter is already part of the set of voters. |
| 127 | VOTER_NOT_FOUND | VoterNotFoundException | 3.9 | The voter is not part of the set of voters. |

The retriable flag of each is read off `Errors.java` @ 3.9.2 at the foundation ticket (the 2.x line read it off the
tag and off its own classes, and the two agreed); the naming rule stays rule 2 of `CLAUDE.md` — a published
identifier is never renamed for a Java rename.

### What is not on the wire

* **No new message format.** The record batch v2 of Kafka 0.11 is the format of every 3.x release; a 3.9.2 node
  answers `message.format.version` below 3.0 with a warning and writes v2 anyway (KIP-724 deprecates the older
  formats). The codecs are unchanged.
* **No lowest version raised.** Kafka **4.0** removes the versions below the 2.1 baseline of KIP-896 (Produce below
  v3, Fetch below v4, Metadata … see the 4.0.0 JSON) — that is the 4.x line's problem, and its first note: a 4.x
  broker closes the connection on every frame of the 0.x lines' versions, so the compliance replay of the inherited
  vectors becomes a *classes-only* replay there.
* **AlterIsr (56) was renamed AlterPartition** in 3.2; the class keeps the published name `AlterIsrRequest` with the
  Java name in the docblock (rule 2). ControlledShutdown, LeaderAndIsr, StopReplica and UpdateMetadata lose their
  flexible versions in 4.0 because they are removed with ZooKeeper — nothing to do in 3.x.

## What is deliberately not in this line (proposed; the owner decides)

| Feature | Arrived in | Proposal |
|---|---|---|
| The KRaft quorum apis 52–55, 58, 59, 62–64, 67, 70, 73, 80–82 | 2.7 … 3.9 | **probe only**, as in 2.x: a class and a vector where the node answers a client, no client method |
| Share groups 76–79 and their state apis 83–87 (KIP-932) | 3.9 | **out**: early access behind `unstable.api.versions.enable` / `group.share.enable`; the 4.x line implements them when they are GA (4.1) |
| Client metrics 71, 72, 74 (KIP-714) | 3.7 | **wire only**: the classes, the vectors of the answers a node without a metrics plugin gives (115 or an empty subscription), no telemetry emitter in the client |
| The new consumer protocol 68, 69 (KIP-848) | 3.5 … 3.9 | **in**, as the last wave of the line: it is the default of Kafka 4.0 and the 3.9 coordinator serves it with `group.coordinator.rebalance.protocols=classic,consumer`; a `KafkaConsumer` option `group.protocol=consumer` that drives `ConsumerGroupHeartbeat` instead of the JoinGroup/SyncGroup cycle |
| The ACL apis 29–31 | 0.11, v3 in 3.3 | **in, for the first time**, if the node runs KRaft: `authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer` with `super.users=User:ANONYMOUS;User:kafkatest` costs one line of `server.properties` and makes every ACL answer real; on ZooKeeper the same is `kafka.security.authorizer.AclAuthorizer`. The 2.x line left them out for want of an authorizer, not for want of a client |
| SASL/SCRAM login | 0.10.2 | still **out** unless the owner asks: the credential apis 50/51 are implemented, the login needs `Sasl\ScramMechanism` in the client; a KRaft node creates a SCRAM user at format time (`kafka-storage.sh format --add-scram`), so the container can offer a SCRAM listener cheaply |
| Tiered storage (ListOffsets v8's earliest-local, error 109) | 3.5 | **wire only**: the field and the code, no remote-storage plugin in the container |

## Environment recipe (from the 2.x session, updated for 3.9.2)

1. `nohup dockerd >/tmp/dockerd.log 2>&1 &` and wait for `docker info`; `tools/dev/vendor-from-source.sh` once
   (no `composer install`, no `composer update`); copy `vendor/` into every worktree with `cp -a`.
2. Clone the Kafka sources into the scratchpad once with the tags of the line:
   `git clone --depth 1 --branch 3.9.2 https://github.com/apache/kafka kafka-src-3.9.2`, then
   `git fetch --depth 1 origin tag <t>` for `3.0.2 … 3.8.1` and `4.0.0` — the JSON message specs and
   `Errors.java` at each tag are the attribution authority.
3. Build **`docker/kafka-3.9.2/`**: `eclipse-temurin:17-jre`, `kafka_2.13-3.9.2.tgz` from
   `archive.apache.org` (curl through the proxy needs the CA bundle in `docker/kafka-3.9.2/ca/`, as the 2.8.2 image
   does), the four client listeners plus a `CONTROLLER://:9096` listener, `process.roles=broker,controller`,
   `node.id=1`, `controller.quorum.voters=1@localhost:9096`, `kafka-storage.sh random-uuid` + `format` at the first
   start, `--add-scram` for the SASL user if a SCRAM listener is wanted, the two log directories, the
   `delegation.token.secret.key`, the `transaction.state.log.*=1` and `offsets.topic.replication.factor=1` of a
   one-node cluster, `group.coordinator.rebalance.protocols=classic,consumer` for the KIP-848 wave, and — if the
   owner takes the ACL proposal — `authorizer.class.name` with `super.users`. The container name is
   `kafka-3-9-2`; `docker-compose.yml` points at it and `docker/kafka-2.8.2/` is **deleted** (the image of the 2.x
   line lives on the `2.x` branch). No `ulimits:` in the compose file (the sandbox refuses it); every suite deletes
   the topics it creates, `IntegrationTestCase` already does it for `uniqueTopicName()`.
   *ZooKeeper variant:* keep the 2.8.2 `start.sh` shape with `zookeeper.connect`, `inter.broker.protocol.version=3.9`
   and `log.message.format.version` left at the default; the tools keep their `--zookeeper` flags.
4. The readiness probe of `tests/Integration/IntegrationTestCase.php` works unchanged (Metadata on a probe topic).
   A KRaft node answers Metadata with brokers **before** any topic exists, so the 0.8/0.9 note of `CLAUDE.md` does
   not apply; a fresh node still answers 5/6 for a moment after a topic is created.
5. Baseline: the whole 2.x gate must be green on the new container **before** the first ticket changes anything —
   `tools/dev/gate.sh <worktree> all` with zero skips. Expect `ApiVersionProbeTest` to be the first thing that
   fails: it pins the api table of the container, and the table of a 3.9.2 node is the one above. Expect the
   ZooKeeper-specific quirks of `docs/protocol/3.9.md` to be re-measured (DeleteTopics v5's null `error_message`,
   the DescribeLogDirs v2 "ignores the selection" note, the `broker:` default resource of KIP-226, the delegation
   token owner), each kept as "on a 2.8.2 ZooKeeper broker" with the 3.9.2 answer next to it.
6. Recreate the container between milestones (`docker compose down -v`, `up -d --wait`), announce a freeze to the
   agents first, and run the milestone gate with the broker to the coordinator alone.

## Ticket plan: one milestone per Kafka minor, chronologically

Same delivery model as the 2.x line (epic + tickets, four Opus agents in isolated worktrees, file ownership per
ticket, PRs against an integration branch merged with merge commits, the coordinator gates every PR locally and
cuts the milestone commits). The four agents of the 2.x line and their surfaces map onto 3.x as follows:

| Agent | Surface | 3.x work, in milestone order |
|---|---|---|
| **T1** | engine, foundation, the apis with a new key, the api-key table, the error codes | foundation ticket (image, renames, keys 65–87, errors 105–127, the table, the probe), 3.0 DescribeTransactions/ListTransactions, 3.3 UpdateFeatures v1 and DescribeQuorum v1, 3.7 DescribeCluster v1 + the client-metrics classes (wire only), 3.8 DescribeTopicPartitions + ListTransactions v1, 3.9 ApiVersions v4 + the raft-voter and share-group classes (probe only), the final table audit |
| **T2** | Produce, Fetch, ListOffsets, Metadata, OffsetForLeaderEpoch, the codecs, the consumer's fetch path | 3.0 ListOffsets v7, 3.1 Fetch v13 + Metadata v12 (topic ids in the request: `Cluster` keeps a name↔id map), 3.5 Fetch v14/v15 + ListOffsets v8, 3.7 Produce v10 + Fetch v16, 3.8 Produce v11, 3.9 Fetch v17 + ListOffsets v9 |
| **T3** | the group apis and the consumer's coordinator | 3.0 OffsetFetch v8 + FindCoordinator v4, 3.2 JoinGroup v8/v9 + LeaveGroup v5, 3.6 OffsetCommit v9, 3.7 OffsetFetch v9, 3.8 FindCoordinator v5 + ListGroups v5, 3.9 FindCoordinator v6 — and the **KIP-848 consumer** (68 in 3.5, 69 in 3.7) as its own wave at the end of the line |
| **T4** | admin, transactions, SASL, tokens, ACLs | 3.2 DescribeLogDirs v3, 3.3 ACL apis v3 (**implemented** if the owner takes the authorizer proposal), DescribeLogDirs v4, the token apis v3, 3.5 AddPartitionsToTxn v4, 3.8 the transaction protocol v2 of KIP-890 (InitProducerId v5, AddPartitionsToTxn v5, AddOffsetsToTxn v4, EndTxn v4, TxnOffsetCommit v4) with the 120 in `TransactionManager` |
| **T10** | docs | the final pass: the four agents' correction lists, the release notes, the README, the record |

The milestone process, unchanged from 2.x and to be repeated verbatim in the epic:

1. Every PR of a minor carries **only that minor** (the 2.x lesson of PR #135: a PR stacked on a next-minor commit
   was rebuilt, because a tag point must speak exactly its minor).
2. Merge order inside a minor: the PR that carries a shared change first (engine, base test class), then the rest;
   every agent merges the integration head before opening, bumps the vector counts of the preambles **on top of**
   the head's numbers and never rewrites the preamble.
3. After the last PR of the minor: freeze announced to the agents, container recreated, whole gate
   (`tools/dev/gate.sh <worktree> all`, zero skips), then the milestone commit `chore(3.x): Kafka 3.N complete`
   with the CHANGELOG section of the minor folded from the PRs, the api-key table and the README at the versions
   the client sends, then a docs commit that writes the tag row (`docs/handoff/main.md`), then the epic comment.
4. A vector-count preamble conflict is resolved by *adding* both sides' clauses and recounting from the JSON
   files (`php -r` over `docs/protocol/vectors/*.json`); a hex-dump seam conflict by keeping both blocks and
   checking the closing fence of the first (`DocumentationSyncTest` catches a lost fence or a lost last byte line).

## Pitfalls (the 2.x session's list — repeat them in every brief)

* **The broker is the authority, the tags are the attribution authority, memory is neither.** Two versions were
  mis-attributed from memory in the 2.x plan (the client-quota v1 is 2.8, the transaction apis' flexible version
  is v3 in 2.8) and corrected only because the agents read the JSON at the tag. Every "Kafka 3.N added" claim is
  read off `git show <tag>:clients/src/main/resources/common/message/<Api>Request.json`.
* **Run phpunit as `php -d opcache.jit=0 vendor/bin/phpunit`** in the sandbox (the JIT miscompiles the LZ4 decoder).
* **Never `pkill` php or phpunit** — it kills the other agents' gates and once killed the coordinator's shell;
  stop only your own pids. **Never restart the broker from an agent.**
* **`git stash` is shared across all worktrees of one repository** — a `stash pop` popped another agent's WIP once.
  Use a WIP commit on your own branch instead.
* **Unique prefixes cannot separate shared resources**: the cluster-wide `broker:` default config resource, the
  delegation tokens of the single SASL user, the fetch-session cache, the group reaper (an `Empty` group with no
  offsets is deleted after `offsets.retention.check.interval.ms`), the pending member of a KIP-394 first join. A
  test on such a resource asserts a **superset**, polls a read-back, or repeats the exchange a bounded number of
  times; a failure of another agent's suite under load is re-run alone once and reported, never "flaked".
* **Every suite deletes the topics it creates** — 2505 leftover topics took a log directory offline with "Too many
  open files" (fd limit 20000, `ulimits:` refused by the sandbox) and left `__consumer_offsets` leaderless.
  `IntegrationTestCase::tearDownAfterClass()` does it for every `uniqueTopicName()`; a suite that creates topics
  another way deletes them itself.
* **Clock skew of one millisecond between the PHP process and the JVM** fails an exact `LogAppendTime` bound; the
  lower bound has a two-millisecond tolerance.
* **The Metadata answer reorders partitions** while other clients create topics: a byte-exact comparison of two
  consecutive answers repeats until they agree.
* **"It is the version this client sends" goes stale at the next bump.** Write "the version Kafka 3.N added" and
  name the keep-behind class; T3's final review found nine such sentences in the 2.x document. The final pass of
  the line is a review of every api section by its owner, applied by unique text anchors.
* **The api-key table and the README are the coordinator's** at every milestone (the agents give the rows in their
  report); the CHANGELOG section of a minor is the agents' verbatim text, folded by the coordinator.
* **A merge tool that drops the tail of a hunk**: the coordinator's python resolver once dropped the sentence "Of
  the other 318 …" of the preamble and once the last byte line of a dump; T3 and `DocumentationSyncTest` caught
  them. After every resolution: `php -d opcache.jit=0 vendor/bin/phpunit --testsuite compliance` and
  `--filter DocumentationSyncTest`, and `grep -c 'Of the other'`.
* **Agents subscribe to their PRs** through the GitHub tool; the coordinator unsubscribes (it gates locally) or the
  events pile up in its queue.
* **A `Closes #n` in a multi-minor ticket closes it early**: every PR says `Part of #n`, the coordinator closes the
  ticket by hand when its last minor lands.
* **`git merge -F -` does not read stdin** in this git; write the message to a file first.
* The 1.1 and 2.8 broker specifics of `CLAUDE.md` still hold for a ZooKeeper node; a KRaft node has none of the
  `server.properties` trailing-newline or `--zookeeper` notes and gets its own "3.9 specifics" paragraph from the
  foundation ticket.

## The first ticket (foundation), as a checklist

1. `docs/handoff/main.md` → `docs/handoff/2.x.md`; this file → `docs/handoff/main.md`; `CLAUDE.md` says `2.x`
   complete and protected, `main` the 3.x line, and points at the record and the plan; `docs/CASCADE.md` already
   carries `2.x → main` (PR of the close-out of the 2.x line).
2. `docs/protocol/3.9.md` → `docs/protocol/3.9.md`, with every `@see docs/protocol/…` reference and the api-key
   table rebuilt from the ApiVersions answer of the new container (`DocumentationSyncTest` checks both); the
   vectors README and preamble keep their counts (318 inherited + the 351 of the 2.x line = 669, all of them still
   served by a 3.9.2 node, which the compliance replay proves).
3. `docker/kafka-3.9.2/` replaces `docker/kafka-2.8.2/`; `docker-compose.yml`, every fixture and every example
   name the new container; `tests/Fixture/ClientQuota` uses the quota apis; the brief template
   (`docs/handoff/AGENT_BRIEF.template.md`) loses its `--zookeeper` tools if the node is KRaft.
4. `Protocol\ApiKeys` 65–87; `KafkaException` 105–127 with one class each and the retriable set of `Errors.java`
   @ 3.9.2; `ApiVersionProbeTest` at the new table; every new exception in the code map.
5. README and CHANGELOG headers for the 3.x line ("Current milestone: none yet — the foundation"), the tag table of
   the 3.x line in the handoff with ten `_pending_` rows.
6. Baseline gate green with zero skips on the new container; the foundation commit is the first commit of the
   integration branch `claude/kafka-3-x-line-<id>` and the base every agent branches from.

## Open questions for the owner (answer them in the first message of the 3.x session)

1. **KRaft or ZooKeeper** for the container (recommendation: KRaft).
2. **ACL apis in** (with `StandardAuthorizer` on the node) or still out.
3. **KIP-848 consumer protocol in** as the last wave (recommendation: in), or the classic protocol only.
4. **Share groups out** (recommendation: out until 4.1 makes them GA).
5. **Client metrics wire-only** (recommendation: yes).
6. Whether the tags of the 3.x line are again the last patch release of each minor (`3.0.2` … `3.9.2`), created by
   the owner after the final merge into `main`, as for the 2.x line.
