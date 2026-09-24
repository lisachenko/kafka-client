Wire vectors of the Kafka 3.9.2 protocol
========================================
The 4.x line has captured **58** vectors of its own so far, on the `kafka-4-3-1` node — the 14 frames of the record
half of Kafka 4.0: the 6 of Produce v12 in `produce.json` (a plain batch, the transactional batch of the transaction
protocol v2 of KIP-890 part 2 whose partition the broker adds itself, and the version 11 pair of the same question
refused with the 120), the 4 of Metadata v13 in `metadata.json` (the top-level error code of KIP-1102) and the 4 of
ListOffsets v10 in `offsets.json` (the `timeout_ms` of KIP-1075) — and the 44 frames of the admin half of Kafka 4.0: the 20 of UpdateFeatures v2 and of the version 1 a 4.x controller answers in `update-features.json`,
the 6 of DescribeCluster v2 (KIP-1073) in `describe-cluster.json`, the 2 of DescribeQuorum v2 re-measured on the
dynamic quorum in `describe-quorum.json`, and the 16 of the two raft-voter apis of KIP-853 in the two new files
`add-raft-voter.json` and `remove-raft-voter.json`.

The 3.x line has captured **466** vectors of its own so far, on the `kafka-3-9-2` KRaft node — the 49 frames of Kafka 3.0:
the 19 of DescribeTransactions and ListTransactions in the two new files `describe-transactions.json` and
`list-transactions.json`, the 10 of ListOffsets v7 in `offsets.json`, the 12 of FindCoordinator v4 in
`group-coordinator.json` and the 8 of OffsetFetch v8 in `offset-fetch.json` — the 20 frames of Kafka 3.1, the
10 of Fetch v13 in `fetch.json` and the 10 of Metadata v12 in `metadata.json` — the 25 frames of Kafka 3.2,
the 12 of JoinGroup v8 and v9 in `join-group.json`, the 4 of LeaveGroup v5 in `leave-group.json` and the 9 of
DescribeLogDirs v3 in `describe-log-dirs.json` — and the 73 frames of Kafka 3.3, the 10 of DescribeQuorum v0 and
v1 in the new file `describe-quorum.json`, the 16 of UpdateFeatures v1 in `update-features.json`, the 30 of the
three ACL apis in the three new files `describe-acls.json`, `create-acls.json` and `delete-acls.json`, the 7 of
DescribeLogDirs v4 in `describe-log-dirs.json` and the 10 of the two delegation-token apis of KIP-373 in
`delegation-tokens.json` — and the 30 frames of Kafka 3.5, the 10 of the broker version AddPartitionsToTxn v4 in
`add-partitions-to-txn.json`, the 12 of Fetch v14 and v15 in `fetch.json` and the 8 of ListOffsets v8 in
`offsets.json` — and the 18 frames of Kafka 3.6, the OffsetCommit v9 of KIP-848 in `offset-commit.json`, with
the 69 of a group that does not exist, the 113 of a stale member epoch and the two version 8 frames that answer
the same questions with the 22 and the 35 — and the 71 frames of Kafka 3.7: the 12 of DescribeCluster v1 (the
endpoint type of KIP-919, with the 114 of a controller asked on a broker listener and the 115 of a type the api
does not define) in `describe-cluster.json`, the 26 of the three client-metrics apis of KIP-714 in the three
new files `get-telemetry-subscriptions.json`, `push-telemetry.json` and `list-client-metrics-resources.json`,
which are wire only, the 20 of OffsetFetch v9 of KIP-848 in `offset-fetch.json`, with the 113 of a stale
member epoch, the 25 of an unknown member id and the -1 a null member id with a non-negative epoch is answered,
and the 13 of the leader discovery of KIP-951: the 5 of Produce v10 in `produce.json`, one of them the
constructed answer that names the current leader of a refused partition, and the 8 of Fetch v16 in `fetch.json`,
among them the follower fetch the node really answers with the `current_leader` and the `node_endpoints` — and
the 88 frames of Kafka 3.8, the 6 of Produce v11 in `produce.json`: the accepted pair, the pair of a
transactional batch for a partition the coordinator has not verified, which is the **120** `TransactionAbortable`
of KIP-890, and the pair of the very same request at version 10, which is answered the **48** instead, and the 22 of the group apis: the 16 of ListGroups v5 in `list-groups.json`
(the group types `classic` and `consumer` of KIP-848, the types filter with its case-insensitive parse and its empty
answer for a type the enum does not know, and the version 4 pair of the same question without a type) and the 6 of
FindCoordinator v5 in `group-coordinator.json` (KIP-890: the version 4 frames with another number in their header,
because no FindCoordinator ever answers the 120 the version exists for), and the 36 of the admin surface: the 28 of
the new api DescribeTopicPartitions (key 75, KIP-966) in the new file `describe-topic-partitions.json`, with the paging
cursor, the empty ELR arrays, the 3 of an unknown topic, the 17 of an illegal name, the -1 of the empty one, the two
42 of a cursor the node refuses and the 29 of a topic the SASL user `acltest` may not describe, and the 8 of
ListTransactions v1 (the duration filter of KIP-994) in `list-transactions.json`, among them the minute-old filter
that lists an id which never began a transaction, and the 24 of the five transaction apis, whose version bumps
declare no field at all: the 4 of InitProducerId v5 in `init-producer-id.json` (with the 90 of an epoch that is
neither the current nor the last one), the 4 of AddOffsetsToTxn v4 in `add-offsets-to-txn.json` (with the 49 of an
unknown producer id), the 8 of TxnOffsetCommit in `txn-offset-commit.json` (the v4 with the 25 of a member the
group does not hold, and the v4/v3 pair of the same commit answered the **120** and the **48** — the partition
verification of KIP-890 part 1 reporting itself through the version gate), the 4 of the broker version
AddPartitionsToTxn v5 in `add-partitions-to-txn.json` (with the 120 of a `verify_only` and the top-level 31 of
`acltest`) and the 4 of EndTxn v4 in `end-txn.json` (with the 48 of an abort after a commit), and the 42 frames of
Kafka 3.9: the 14 of FindCoordinator v6 in `group-coordinator.json` (KIP-932: the version 5 frames with another
number in their header, the coordinator type **2** that is the 42 of the version gate at v4 and v5 and the 15 of a
node without a share coordinator at v6, the type 99 that stays the 42 of the error path, and the 31 of the SASL
user `acltest`, who may not ask where a share coordinator is), the 16 the same release adds to the producer and
consumer apis, the 8 of Fetch v17 in `fetch.json` (the tagged `replica_directory_id` of KIP-853, written by a
consumer, by a follower next to the `replica_state` of KIP-903, and left off the wire altogether, all three
answered the same as at version 16) and the 8 of ListOffsets v9 in `offsets.json` (the target time `-5`
`LATEST_TIERED_TIMESTAMP` of KIP-1005 on a filled and on an empty log, both the offset `-1` of a node without
remote storage, with the version 8 refusal and the ordinary `-1` pair), the 4 of ApiVersions in
`api-versions.json` (the v4 pair this client sends and the v3 pair of the same question on the same node, 19 bytes
shorter because the `kraft.version` feature of KAFKA-17011 is hidden below the version 4) and the 8 of
DescribeQuorum v2 in `describe-quorum.json` (the nodes, the directory ids and the two error messages of KIP-853:
the metadata quorum, a topic no quorum replicates, the empty topic array and the 31 of the SASL user `acltest`,
which now says `Cluster authorization failed.`) — and the 50 frames of the **KIP-848 consumer**, the last wave
of the line: the 34 of ConsumerGroupHeartbeat (key 68) in the new file `consumer-group-heartbeat.json` — the join
that creates a group and is answered every partition at once, the acknowledgement that echoes them back, the
steady state of five nulls, the subscription that changes without a rejoin, the revoke of a two-member group, the
server-side assignor `range`, the leave of the epoch -1, the static join with its instance id and its leave of the
epoch -2, and the six refusals 42, 42, 110, 25, 112 and 69 — the 14 of ConsumerGroupDescribe (key 69) in the new
file `consumer-group-describe.json`, with both assignments of every member, the authorized operations of KIP-430,
the 69 of a classic group and of a group that does not exist, the -1 the empty group id crashes the answer builder
with and the 30 of `acltest`, and the 2 of the classic DescribeGroups v5 asked for a KIP-848 group in
`describe-groups.json`, which answers the state `Dead` rather than an error — next
to the
**669** vectors of the lines
up to 2.x below, which are replayed unchanged against the classes of this line, because the 3.9.2 node still serves
every version they were captured at.

One file per api, each holding frames that a real Apache Kafka broker sent or accepted. They are the
machine-readable half of [`../3.9.md`](../3.9.md), whose "Wire vectors" section shows the same bytes as annotated
hex dumps. There are **1207** of them in **62** files: **14** are the transaction protocol v2 of Kafka 4.0 (KIP-890 part
2), captured on the `kafka-4-3-1` node of the 4.x line: the TxnOffsetCommit v4 and v5 pairs of one commit, the 120 and
the 0, in `txn-offset-commit.json` and five EndTxn v5 pairs in `end-txn.json` - **351** were captured on the `kafka-2-8-2` container of the
**2.x** line - the request and the answer of every version Kafka **2.0** added to the producer and consumer apis
(14 frames), to the admin, the transaction and the delegation-token apis (34 frames), to the ten group apis
(20 frames) and to ApiVersions (2 frames), nearly all of them KIP-219 bumps, plus what Kafka **2.1** added: the
19 frames of the leader epochs and the zstd codec in the producer and consumer apis (KIP-320, KIP-110), the 6
frames of OffsetCommit v5/v6 and OffsetFetch v5 and the 4 of DeleteTopics v3 and TxnOffsetCommit v2, and the 10
frames of **Kafka 2.2**: the ListOffsets v5 pair (KIP-207), SaslAuthenticate v1, ControlledShutdown v2 and the new
api ElectLeaders (key 43), whose `elect-leaders.json` is the one file this line added, and the 4 frames of the
KIP-394 exchange (JoinGroup v4), the 6 frames of the new api IncrementalAlterConfigs (key 44) of **Kafka 2.3**,
in the new file `incremental-alter-configs.json`, the 12 frames of what Kafka **2.3** added to the group apis
(static membership, KIP-345, and the authorized operations of KIP-430) and the 7 frames of what it added to the
producer and consumer apis (Metadata v8 of KIP-430, Fetch v11 and OffsetForLeaderEpoch v3 of KIP-392), the 3 flexible ApiVersions v3 frames of **Kafka 2.4** and the 6 frames
of its non-flexible admin half - CreateTopics v4 (KIP-464) and ElectLeaders v1 (KIP-460), the 6 frames of the batch
LeaveGroup v3 of Kafka **2.4**, the 16 frames of its **partition reassignments and first flexible admin bumps**
(KIP-455, in the two new files `alter-partition-reassignments.json` and `list-partition-reassignments.json`, plus
InitProducerId v2 and CreateDelegationToken v2), the 5 frames of its Produce v8 (KIP-467, the record errors of
a refused batch), the 22 frames of the **flexible** versions of the ten group apis (KIP-482), the Metadata v9
pair of the same release, the first flexible version of an api the consumer sends, and the 5 frames of
**OffsetDelete** (KIP-496, the new file `offset-delete.json`), the 10 frames of the **flexible** admin versions
of the same release: CreateTopics v5 - whose answer is the KIP-525 one, with the configuration of the new topic in
it - DeleteTopics v4, ElectLeaders v2, IncrementalAlterConfigs v1 and ControlledShutdown v3; and of **Kafka 2.5**
the 14 frames the group apis gained - the JoinGroup v7 and SyncGroup v5 pairs of KIP-559 with the two refusals
that show their nulls, and the six OffsetFetch v7 frames of KIP-447 around the 88 of an offset a transaction has
not committed yet, and the 21 frames of its transaction, admin and SASL half: the InitProducerId v3 of KIP-360
with its epoch bump, the TxnOffsetCommit v3 of KIP-447 with the consumer it names, CreatePartitions v2,
SaslAuthenticate v2 and the token apis v2; and of **Kafka 2.6** the 4 frames of ListGroups v4 (KIP-518), the
states filter of the request and the group state of every entry of the answer, and the 9 frames of its two
client-quota apis (KIP-546, the two new files `describe-client-quotas.json` and `alter-client-quotas.json`), and the 6 frames of its DescribeConfigs **v3** (KIP-569) and the 4 frames of its
DescribeLogDirs **v2**, and the 10 frames of the three apis **Kafka 2.7** added - the two SCRAM credential apis of
KIP-554 and UpdateFeatures of KIP-584, in the three new files `describe-user-scram-credentials.json`,
`alter-user-scram-credentials.json` and `update-features.json`, and of the same release the 17 frames of the admin and transaction half: CreateTopics v6, CreatePartitions v3 and DeleteTopics v5 of
KIP-599, and InitProducerId v4, AddPartitionsToTxn v2, AddOffsetsToTxn v2 and EndTxn v2 of KIP-588, two of them
carrying the 90 `ProducerFenced` the KIP added, and the 15 frames of **Kafka 2.8**: the two new apis
DescribeCluster (KIP-700) and DescribeProducers (KIP-664) in the new files `describe-cluster.json` and
`describe-producers.json`, and the flexible **v1** of the two client-quota apis, which is where those two finally
become compact, and the 22 frames of the admin and transaction half of the same release: the CreateTopics **v7**
pair of KIP-516 whose answer carries the id of the new topic, five DeleteTopics **v6** frames of the same KIP - a
deletion by name, one by that id and the 100 of an id no topic carries - and the first flexible version of
DescribeConfigs **v4**, AlterConfigs **v2**, AlterReplicaLogDirs **v2**, WriteTxnMarkers **v1**,
AddPartitionsToTxn **v3**, AddOffsetsToTxn **v3** and EndTxn **v3** (KIP-482) - and of the other 318,
**229** were
captured by the four lines below the 1.x one and are replayed against the classes of this line unchanged, while
**89** were captured on the 1.1.1 broker of the 1.x line. Three of the inherited vectors were **re-captured**
rather than added - `apiversions.response.v0` and `.v1`, whose whole content is the api-key table of the broker,
and `apiversions.response.v0.unsupported-version`, which gained the api row of KIP-511.

What the group apis of this line have captured so far is the KIP-219 version bump of Kafka 2.0 — OffsetCommit
v4, OffsetFetch v4, FindCoordinator v2, JoinGroup v3, Heartbeat v2, LeaveGroup v2, SyncGroup v2, DescribeGroups v2,
ListGroups v2 and DeleteGroups v1, one request/response pair each, taken from one life of the group
`t3-kip219-group` — the same bump of the four apis the producer and the consumer send, and what **Kafka 2.1**
(KIP-320, the leader epochs, and KIP-110, the zstd codec) added to those apis and to Produce:

| File | Of it captured here | What was captured on the 2.8.2 broker |
|---|---|---|
| `produce.json` | 13 of 37 | **Produce v6** (KIP-219), request and answer, plus the throttled answer of a `producer_byte_rate` quota — the frame that shows what KIP-219 changed: `ThrottleTime = 2381` in an answer that arrived after about a millisecond; **Produce v7** (KIP-110), request and answer, and the **76** a v6 is answered when its record set is compressed with zstd; **Produce v8** (KIP-467), the accepted pair, the batch a `cleanup.policy=compact` topic refuses with the record errors that name its key-less records, and the same refusal answered to a version 7 request; **Produce v9** (KIP-482), the flexible pair, whose record set is a compact byte array |
| `fetch.json` | 22 of 53 | **Fetch v8** (KIP-219), request and answer; the throttled answer, whose topics array is **empty**; the pair of a Fetch v3 against a topic with `message.downconversion.enable=false`, which is answered **35** `UNSUPPORTED_VERSION` per partition (KIP-283); **Fetch v9** and **v10** (KIP-320, KIP-110) with the `current_leader_epoch` on the wire, the **75** of an epoch above the leader's, and the three frames of the zstd rule — the **76** of a v9 against a `compression.type=zstd` topic and the same partition served to a v10; **Fetch v11** (KIP-392) with the `rack_id` of the consumer and the `preferred_read_replica` it is answered with; and the four frames of **Fetch v12** (Kafka 2.7), the first flexible version of the api - the plain pair of a consumer fetch and the pair in which the `last_fetched_epoch` of KIP-595 is really asked and answered with the tagged `diverging_epoch` |
| `offsets.json` | 8 of 22 | **ListOffsets v3** (KIP-219), the version 2 frames with another number in the header; **v4** (KIP-320), which carries a `current_leader_epoch` in the request and a `leader_epoch` behind every answered offset; and **v5** (KIP-207), the version 4 frames again — what it adds is the error code **78**, which a one-broker container can not produce; and **v6** (KIP-482), the flexible pair |
| `metadata.json` | 13 of 32 | **Metadata v6** (KIP-219), likewise; **v7** (KIP-320), whose partition entries carry the `leader_epoch` of their leader; and **v8** (KIP-430), with the two booleans that ask for the authorized operations, the bitfields they are answered with and the `Integer.MIN_VALUE` of the same answer when they are off; **v10** and **v11** (KIP-516, KIP-700), the two pairs of the topic ids - the v10 one still carrying the cluster-wide bitfield that v11 has not got |
| `offset-for-leader-epoch.json` | 9 of 13 | **OffsetForLeaderEpoch v1** (KIP-279): the `leader_epoch` the answered `end_offset` belongs to, inserted between the partition id and the offset; and **v2** (KIP-320), the version a consumer sends, with a `current_leader_epoch` in the request, a `throttle_time_ms` at the head of the answer and the **75** of a fenced epoch; and **v4** (KIP-482), the flexible pair |

What Kafka 2.1 added to the two offset apis: OffsetCommit v5 (the frame without
`retention_time`, KIP-211), OffsetCommit v6 and OffsetFetch v5 (the `committed_leader_epoch` of KIP-320). What
Kafka 2.2 added to JoinGroup: the four frames of the KIP-394 exchange — a v4 join that carries an empty member id,
the answer that refuses it with **79** `MEMBER_ID_REQUIRED` and the member id the coordinator assigned, and the
join that follows with that id. What Kafka 2.3 added: the `group_instance_id` of a static member in JoinGroup v5,
SyncGroup v3, Heartbeat v3 and OffsetCommit v7 (KIP-345), the heartbeat that is answered **82**
`FENCED_INSTANCE_ID` once a second consumer has taken that instance id over, and the DescribeGroups v3 pair whose
request asks for the authorized operations of the group and whose answer reports them (KIP-430). What Kafka **2.5**
added: the `protocol_type` and `protocol_name` of KIP-559 - in the JoinGroup **v7** answer, where an error answer
reports both as null, and on both sides of SyncGroup **v5**, where leaving them out is a **23** - and the
`require_stable` flag of OffsetFetch **v7** (KIP-447) with the **88** it is answered while a transactional offset
commit of that partition is still open. What **Kafka 2.6** added: the `states_filter` of ListGroups **v4** and the
`group_state` of every group it answers (KIP-518). What Kafka 2.4
added: the batch LeaveGroup **v3** of KIP-345 and the **flexible** version of every one of the ten apis (KIP-482) -
one request/response pair per api, with the compact strings and arrays, the tagged-field section of every
structure, the request header v2 and the response header v1, plus the plain DescribeGroups v4 whose members carry
their `group_instance_id`. What **Kafka 3.2** added, captured on the KRaft node: the `reason` of KIP-800 in
JoinGroup **v8** and in every entry of a LeaveGroup **v5** batch - as a compact string and, for a member that
names none, as the compact null - and the `skip_assignment` of KIP-814 in the JoinGroup **v9** answer, `false`
for the 79 of a first join and for the leader of a fresh generation, and **true** for the one frame it exists
for: a static instance that comes back to a `Stable` group with an empty member id and is told to keep the
assignment the group already has.

What **Kafka 2.1 and 2.2** added to the admin, transaction and SASL apis, captured with the client id `t4-vectors`
and the topics `t4-21-vectors` and `t4-22-vectors`:

| File | Of it captured here | What was captured on the 2.8.2 broker |
|---|---|---|
| `delete-topics.json` | 2 of 9 | **DeleteTopics v3** (Kafka 2.1): the frame of v2, whose version promises that the client understands the error code **73** `TopicDeletionDisabled` |
| `txn-offset-commit.json` | 2 of 6 | **TxnOffsetCommit v2** (Kafka 2.1, KIP-320): the `committed_leader_epoch` between the offset and the metadata, and the answer of the coordinator that stores it without looking at it |
| `sasl-authenticate.json` | 2 of 7 | **SaslAuthenticate v1** (Kafka 2.2, KIP-368): the PLAIN token and the answer that now ends in a `session_lifetime_ms` — **0** on a listener without `connections.max.reauth.ms` |
| `controlled-shutdown.json` | 2 of 6 | **ControlledShutdown v2** (Kafka 2.2, KIP-380): the `broker_epoch` behind the broker id, asked with the **unknown** broker id 4242 and the epoch -1 — a shutdown of the real broker id would stop the shared container |
| `elect-leaders.json` | 8 of 8, **new file** | **ElectLeaders v0** (Kafka 2.2, KIP-183, added as ElectPreferredLeaders): a named partition whose leader is already the preferred replica (**84** `ElectionNotNeeded`, a code of Kafka 2.4 that reaches a v0 client unchanged) and a partition of a topic the cluster does not have (**3**); plus the **v1** frames of KIP-460 (Kafka 2.4), whose request opens with an `election_type` byte and whose answer carries a top-level error code - a preferred and an **unclean** election of the same healthy partition, both answered 84 |
| `describe-configs.json` | 4 of 18 | **DescribeConfigs v3** (Kafka 2.6, KIP-569): one option of a topic asked with `include_documentation` - the answer ends in the `config_type` byte (7, LIST) and 329 characters of prose - and the same option without the flag, whose entry still carries the type and a null documentation |
| `create-topics.json` | 4 of 11 | **CreateTopics v3** (Kafka 2.0, KIP-219) and **v4** (Kafka 2.4, KIP-464): the v4 pair asks for the broker's own `num.partitions` and `default.replication.factor` with -1/-1 and an empty assignment, and the container answers 0 and creates three partitions with one replica each |
| `incremental-alter-configs.json` | 6 of 6, **new file** | **IncrementalAlterConfigs v0** (Kafka 2.3, KIP-339): the three operations a topic accepts in one resource (SET, APPEND and the DELETE whose value is the null string), an APPEND to an option that is not a list (**42**) and the cluster-wide default broker resource with a static option, asked with `validate_only` (**42**) |

What **Kafka 2.4** added to the admin and transaction surface, captured with the client id `kafka-client-t1-vectors`:

| File | Of it captured here | What was captured on the 2.8.2 broker |
|---|---|---|
| `alter-partition-reassignments.json` | 8 of 8, **new file** | **AlterPartitionReassignments v0** (KIP-455), the first *flexible* admin frames this package sends: a reassignment to the replica set a partition already has (the only successful one a one-broker cluster can be asked for), a cancellation with nothing in flight (**85** `NoReassignmentInProgress`, the code Kafka 2.4 added for it), a replica set naming a broker that is not alive (**39**), a topic the cluster does not have (**3**, per partition and never at the top level) and a request whose three partitions carry three different codes. The topic is `t1-reassign-vectors`; no vector ever names a **null** topic array, which would move the partitions of every other suite of the shared container |
| `list-partition-reassignments.json` | 2 of 2, **new file** | **ListPartitionReassignments v0** (KIP-455): the request for one named partition and the 14-byte answer of a cluster with nothing in flight. A one-broker cluster completes a reassignment before it answers the request that submitted it, so the empty list is the only answer it can produce; the shape of a partition in flight is documented from the sources |
| `offset-delete.json` | 5 of 5, **new file** | **OffsetDelete v0** (Kafka 2.4, KIP-496), the one api of that release that is **not** flexible: the request and the answer whose top-level error code stands *before* the throttle time, and the three refusals - **86** `GroupSubscribedToTopic` for a topic a live `consumer` group consumes, **68** `NonEmptyGroup` for a live group of another protocol type and **69** `GroupIdNotFound` for a group the coordinator does not know, the last two in 18 bytes that name no partition at all |
| `describe-client-quotas.json` | 8 of 8, **new file** | **DescribeClientQuotas v0** (Kafka 2.6, KIP-546): the filter for one named `client-id` and the answer that carries its two quotas as **`float64`s** - the only place in this protocol where that type appears - plus the two refusals, **35** for an entity type the broker does not know (with a **null** entry array, where a filter that matched nothing answers an empty one) and **-1** for a match type it does not know. The v0 frames are plain - Kafka 2.6 added the api after KIP-482 and still without the compact encoding - and the **v1** frames of Kafka 2.8 next to them are the same fields in the compact one |
| `alter-client-quotas.json` | 8 of 8, **new file** | **AlterClientQuotas v0** (Kafka 2.6, KIP-546): two quotas set on the `client-id` entity `t1-quota-vectors` and the answer that reports the entity on its own - this api has no top-level error code - a removal asked with `validate_only` (whose op still carries the eight bytes of double the broker discards) and the **42** of a quota name the broker does not know; plus the three **v1** frames of Kafka 2.8, where the api finally becomes compact |
| `describe-user-scram-credentials.json` | 3 of 3, **new file** | **DescribeUserScramCredentials v0** (Kafka 2.7, KIP-554), flexible from its v0: the request for one user and the answer that names the mechanism and the iteration count of their credential - and nothing else, because a credential is a salted password and the frame has no field for one - plus the **91** of a user who has none, which is a per-user code while the top level stays 0 |
| `alter-user-scram-credentials.json` | 5 of 5, **new file** | **AlterUserScramCredentials v0** (Kafka 2.7, KIP-554): the upsertion of a SCRAM-SHA-256 credential with 8192 iterations, whose 32-byte salted password the **client** derived with `hash_pbkdf2()` so that the password never reaches the wire, the answer that reports one result per affected *user*, and the three refusals **93** (too few iterations), **92** (the same user and mechanism twice) and **91** (deleting a credential that is not there) |
| `update-features.json` | 2 of 2, **new file** | **UpdateFeatures v0** (Kafka 2.7, KIP-584): a request to finalize a feature at the version level 1 and the answer of a ZooKeeper-backed cluster, which finalizes nothing at all - the top-level 0 with the per-feature **42**, *"Could not apply finalized feature update because the provided feature is not supported."* |
| `describe-cluster.json` | 4 of 4, **new file** | **DescribeCluster v0** (Kafka 2.8, KIP-700): the smallest request of this protocol - one boolean - with and without the acl flag, and the two answers. Without the flag `cluster_authorized_operations` is `Integer.MIN_VALUE`, the default of the specification; with it, **8096** = `CREATE \| ALTER \| DESCRIBE \| CLUSTER_ACTION \| DESCRIBE_CONFIGS \| ALTER_CONFIGS \| IDEMPOTENT_WRITE`, the whole set a broker without an authorizer allows on a CLUSTER resource |
| `describe-producers.json` | 4 of 4, **new file** | **DescribeProducers v0** (Kafka 2.8, KIP-664): the producer state of a partition an idempotent producer wrote to - the producer id, the epoch, the last sequence number and timestamp, and the `current_txn_start_offset` of **-1** that says no transaction of it is open here - the same partition while a transaction of that producer IS open, whose `coordinator_epoch` is still -1 because the marker and not the batch writes it, and an answer for two empty partitions and one the broker does not have (**3** per partition, this api has no top-level error code) |
| `init-producer-id.json` | 2 new of 10 | **InitProducerId v2** (KIP-482): the v1 body in the compact encoding - the request header v2, a compact transactional id and a tag buffer at the end of the header and of the body - for the transactional id `t1-24-vectors-tx` |
| `delegation-tokens.json` | 4 new of 26 | **CreateDelegationToken v2** (KIP-482): the flexible pair for `User:kafkatest` on the SASL_PLAINTEXT listener, the **57** of a renewer whose principal type is not `User` and the **64** of the PLAINTEXT listener, all four with compact strings and bytes. The `owner` of the answer is two *flat* fields of the specification, so it carries no tag buffer of its own - the `InlineStruct` case of the engine |

A vector is captured on the broker of the line that introduced its api version and is not re-captured while the
frame does not change: the vectors inherited from `0.8.x` were captured on a Kafka 0.8.2.2 broker, those of `0.9.x`
- Produce v1, Fetch v1, OffsetCommit v2, ControlledShutdown v1, the group membership apis and the consumer protocol
structures - on a 0.9.0.1 broker, those of `0.10.x` - message format v1, Metadata v1/v2, Produce v2, Fetch v2/v3,
Offsets v1, OffsetFetch v2, JoinGroup v1, CreateTopics v0/v1, DeleteTopics v0 and SaslHandshake v0 - on a 0.10.2.2
broker, and everything Kafka 0.11 added - the record batch v2, the throttle time of KIP-124 on fourteen apis,
Produce v3, Fetch v4/v5, DeleteRecords, DescribeConfigs/AlterConfigs v0, OffsetForLeaderEpoch and the six apis of
the transaction protocol - on the 0.11.0.3 container of the `0.11.x` line.

What the 1.x line captured on the `kafka-2-8-2` container of `docker-compose.yml`
--------------------------------------------------------------------------------

The container runs Apache Kafka **1.1.1** with four listeners (PLAINTEXT 9092, SSL 9093, SASL_PLAINTEXT 9094,
SASL_SSL 9095), `log.message.format.version = 1.1-IV0`, two log directories and a delegation-token master key. Six
of the files below are new on this line; the rest gained the frames of the versions Kafka 1.0 and 1.1 added.

| File | Of it captured here | What was captured on the 1.1.1 broker |
|---|---|---|
| `api-versions.json` | 2 new of 7, 3 **re-captured** | The **ApiVersions v2** pair (Kafka 2.0, KIP-219; the frame of v1) and the three re-captured answers. The answer of this api *is* the api-key table of the broker, so it is re-captured on every line: it now carries the **56** keys 0-51, 56, 57, 60 and 61 of a Kafka 2.8.2 broker, where the 1.1.1 capture carried 43, the 0.11 one 34 and the 0.10 one 21; the 35 answer of an unknown version carries the row `18 0 3` of KIP-511 instead of an empty array |
| `sasl-handshake.json` | 6 of 12 | **SaslHandshake v1** (KIP-152): the accepted handshake, a mechanism the broker has not enabled (33), and a second handshake on the same connection (34, with the **empty** mechanism list that 1.1 answers where 1.0.2 still filled it). The v0 frames of the `0.10.x` line are replayed unchanged, now through `SaslHandshakeRequestV0` |
| `sasl-authenticate.json` | 5 of 5, **new file** | **SaslAuthenticate v0** (key 36): the framed PLAIN token and its empty answer, a wrong password (**58** with the message of the broker) and a second `SaslAuthenticate` on an authenticated connection (**34**, which leaves the connection usable) |
| `produce.json` | 8 of 24 | **Produce v4** (the v3 body, one api version higher) and **Produce v5** with its `log_start_offset`, captured after a `DeleteRecords` so that the field is really 2; plus the two pairs of the 1.x idempotent producer — a duplicate of the batch **four batches back** (error code 0 and the base offset of the original append: the five-batch window) and the batch after a `DeleteRecords` of the whole partition (**59** `UNKNOWN_PRODUCER_ID` with `log_start_offset = 5`) |
| `fetch.json` | 10 of 29 | **Fetch v6** (the v5 frame, one api version higher) and the six frames of one **fetch session** of v7 (KIP-227): the full fetch that opens it, an incremental fetch with an empty topic array, an incremental fetch that drops a partition in `forgotten_topics_data`, and the two session errors **71** (a wrong epoch) and **70** (an unknown session id) |
| `metadata.json` | 2 of 19 | **Metadata v5** (KIP-112/113): the answer whose every partition ends with the `offline_replicas` array, empty on the one-broker container |
| `describe-log-dirs.json` | 6 of 6, **new file** | **DescribeLogDirs v0** (KIP-113): the two log directories of the image, the answer for one named partition, both shapes of the nullable topic array (the **empty** one and its 64-byte answer, and the **null** request whose answer is not stored because it carries every replica of the container), and the answer taken **while a real replica move was running**, in which the partition appears in both directories with `is_future` marking the destination |
| `alter-replica-log-dirs.json` | 6 of 6, **new file** | **AlterReplicaLogDirs v0** (KIP-113): an accepted move, the **57** of a path that is not one of `log.dirs`, and the **9** (`ReplicaNotAvailable`) of a replica the broker does not host |
| `describe-configs.json` | 8 of 14 | **DescribeConfigs v1** (KIP-226): a topic with and without `include_synonyms` (the config **source** and the synonyms that replace `is_default`), the broker resource right after a dynamic change — the source 2 with the static default behind it, and the sensitive `ssl.key.password` whose value is null in the entry and in its synonym — and the cluster-wide **default** broker resource with the source 3. The six v0 frames of the `0.11.x` line are replayed through the new `…V0` classes |
| `alter-configs.json` | 6 of 12 | **The dynamic broker configuration of KIP-226**: a dynamic option on one broker (accepted), the same option on the default resource, and a static one refused with 42 and `Cannot update these configs dynamically: Set(…)`. The two 0.11 frames of a *refused* broker resource stay as history — a 1.1 broker no longer refuses the frame at all |
| `create-partitions.json` | 8 of 8, **new file** | **CreatePartitions v0** (KIP-195): the growth of a topic, an assignment with `validate_only`, a shrink (**37**) and an unknown topic (**3**) |
| `delete-groups.json` | 6 of 6, **new file** | **DeleteGroups v0** (KIP-229): an `Empty` group deleted (0) next to a group that never existed (**69**) in one frame, and a group with a live member (**68**) |
| `delegation-tokens.json` | 14 of 14, **new file** | **The four token apis 38 to 41** (KIP-48) — the one vector file that holds *several* apis, so its `apiKey` is null and every request vector carries the key of its own api. The fourteen frames are one life of one token on the SASL_PLAINTEXT listener as `User:kafkatest`: created with a renewer and a one-hour lifetime, described, renewed, deleted; plus the **67** of a renewer that is not a `User`, the **63** of a principal that is neither owner nor renewer, the **62** of expiring a deleted token, the **66** of a token past its lifetime and the two **64**s of the PLAINTEXT listener |

The other 23 files carry no 1.x frame at all: their apis and structures are unchanged since the line that captured
them, and a 1.1.1 broker still answers every one of their versions.

That the older frames are still the current ones is not an assumption:
`tests/Integration/ApiVersionProbeTest.php` asks the 1.1.1 broker with a real **ApiVersions** request
(`api-versions.json`) which versions it serves, and sends a frame of every one of them — and one frame above every
one of them, to see the connection close.

The shape of a file
-------------------

```json
{
    "api": "metadata",
    "apiKey": 3,
    "section": "Metadata API (key 3, v0 to v13)",
    "vectors": [
        {
            "id": "metadata.request.v0.all-topics",
            "kind": "request",
            "class": "Protocol\\Kafka\\Protocol\\Request\\MetadataRequestV0",
            "version": 0,
            "source": "broker",
            "description": "…",
            "hex": "0000001200030000…",
            "fields": { "messageSize": 18, "apiKey": 3, "…": "…" }
        }
    ]
}
```

A vector of the kind `structure` is not a frame but the content of a byte array field that travels inside one -
the `Subscription` and `MemberAssignment` payloads of the consumer group protocol, which belong to no api key of
their own (`"apiKey": null`) and carry neither a `Size` field nor a header. The compliance suite replays them
through the `pack()` and `unpack()` helpers of their class instead of the framing of `AbstractProtocolMessage`.

* `hex` is the complete frame, `Size` field included, or the bare structure for a vector of the kind `structure`.
* `fields` mirrors the scheme of the class: the field names and the order are the ones of `getScheme()`, nested
  objects are nested maps, and a raw byte field - the message set of Produce and Fetch, and the member metadata and
  assignments of the group apis - is written as `{"$bytes": "<hex>"}`.
* `source` is `broker` for a captured frame and `constructed` for the few that were built by the client because a
  capture would carry unrelated state of the test cluster.
* A file whose `apiKey` is `null` either holds structures (`consumer-protocol.json`) or several apis
  (`delegation-tokens.json`); in the second case every vector names its own `apiKey`.

`tests/Compliance/ProtocolVectorTest` replays every vector - decode the frame, compare every field, encode the
message back and compare the bytes - and `tests/Compliance/DocumentationSyncTest` checks that the document and these
files still describe the same vectors:

```bash
vendor/bin/phpunit --testsuite compliance
```

To add a vector: capture the frame from a broker (`docker compose up -d` starts one), add an entry here with the
values it decodes into, and add the annotated dump to the "Wire vectors" section of the protocol document with an
`<!-- vector: <id> -->` marker in front of it. Both suites fail until the two halves agree.

A **new api** needs a new file plus one data provider and one test method in `ProtocolVectorTest`. The provider is
named after the file it reads - `offsetCommitVectors()` reads `offset-commit.json` - and returns
`VectorFile::provideFor(__FUNCTION__)`; `testEveryVectorFileIsReplayed()` derives the list of covered apis from
those providers, so nothing has to be added to a shared list and two tickets that capture vectors at the same time
do not conflict.
