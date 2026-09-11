Wire vectors of the Kafka 2.8.2 protocol
========================================
One file per api, each holding frames that a real Apache Kafka broker sent or accepted. They are the
machine-readable half of [`../2.8.md`](../2.8.md), whose "Wire vectors" section shows the same bytes as annotated
hex dumps. There are **549** of them in **41** files: **231** were captured on the `kafka-2-8-2` container of the
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
**OffsetDelete** (KIP-496, the new file `offset-delete.json`), and the 10 frames of the **flexible** admin versions
of the same release: CreateTopics v5 - whose answer is the KIP-525 one, with the configuration of the new topic in
it - DeleteTopics v4, ElectLeaders v2, IncrementalAlterConfigs v1 and ControlledShutdown v3; and of **Kafka 2.5**
the 14 frames the group apis gained - the JoinGroup v7 and SyncGroup v5 pairs of KIP-559 with the two refusals
that show their nulls, and the six OffsetFetch v7 frames of KIP-447 around the 88 of an offset a transaction has
not committed yet; and of **Kafka 2.6** the 4 frames of ListGroups v4 (KIP-518), the states filter of the request
and the group state of every entry of the answer - and of the other 318,
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
| `produce.json` | 11 of 35 | **Produce v6** (KIP-219), request and answer, plus the throttled answer of a `producer_byte_rate` quota — the frame that shows what KIP-219 changed: `ThrottleTime = 2381` in an answer that arrived after about a millisecond; **Produce v7** (KIP-110), request and answer, and the **76** a v6 is answered when its record set is compressed with zstd; **Produce v8** (KIP-467), the accepted pair, the batch a `cleanup.policy=compact` topic refuses with the record errors that name its key-less records, and the same refusal answered to a version 7 request |
| `fetch.json` | 16 of 49 | **Fetch v8** (KIP-219), request and answer; the throttled answer, whose topics array is **empty**; the pair of a Fetch v3 against a topic with `message.downconversion.enable=false`, which is answered **35** `UNSUPPORTED_VERSION` per partition (KIP-283); **Fetch v9** and **v10** (KIP-320, KIP-110) with the `current_leader_epoch` on the wire, the **75** of an epoch above the leader's, and the three frames of the zstd rule — the **76** of a v9 against a `compression.type=zstd` topic and the same partition served to a v10 |
| `offsets.json` | 6 of 20 | **ListOffsets v3** (KIP-219), the version 2 frames with another number in the header; **v4** (KIP-320), which carries a `current_leader_epoch` in the request and a `leader_epoch` behind every answered offset; and **v5** (KIP-207), the version 4 frames again — what it adds is the error code **78**, which a one-broker container can not produce |
| `metadata.json` | 9 of 28 | **Metadata v6** (KIP-219), likewise; **v7** (KIP-320), whose partition entries carry the `leader_epoch` of their leader; and **v8** (KIP-430), with the two booleans that ask for the authorized operations, the bitfields they are answered with and the `Integer.MIN_VALUE` of the same answer when they are off |
| `offset-for-leader-epoch.json` | 7 of 11 | **OffsetForLeaderEpoch v1** (KIP-279): the `leader_epoch` the answered `end_offset` belongs to, inserted between the partition id and the offset; and **v2** (KIP-320), the version a consumer sends, with a `current_leader_epoch` in the request, a `throttle_time_ms` at the head of the answer and the **75** of a fenced epoch |

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
their `group_instance_id`.

What **Kafka 2.1 and 2.2** added to the admin, transaction and SASL apis, captured with the client id `t4-vectors`
and the topics `t4-21-vectors` and `t4-22-vectors`:

| File | Of it captured here | What was captured on the 2.8.2 broker |
|---|---|---|
| `delete-topics.json` | 2 of 9 | **DeleteTopics v3** (Kafka 2.1): the frame of v2, whose version promises that the client understands the error code **73** `TopicDeletionDisabled` |
| `txn-offset-commit.json` | 2 of 6 | **TxnOffsetCommit v2** (Kafka 2.1, KIP-320): the `committed_leader_epoch` between the offset and the metadata, and the answer of the coordinator that stores it without looking at it |
| `sasl-authenticate.json` | 2 of 7 | **SaslAuthenticate v1** (Kafka 2.2, KIP-368): the PLAIN token and the answer that now ends in a `session_lifetime_ms` — **0** on a listener without `connections.max.reauth.ms` |
| `controlled-shutdown.json` | 2 of 6 | **ControlledShutdown v2** (Kafka 2.2, KIP-380): the `broker_epoch` behind the broker id, asked with the **unknown** broker id 4242 and the epoch -1 — a shutdown of the real broker id would stop the shared container |
| `elect-leaders.json` | 8 of 8, **new file** | **ElectLeaders v0** (Kafka 2.2, KIP-183, added as ElectPreferredLeaders): a named partition whose leader is already the preferred replica (**84** `ElectionNotNeeded`, a code of Kafka 2.4 that reaches a v0 client unchanged) and a partition of a topic the cluster does not have (**3**); plus the **v1** frames of KIP-460 (Kafka 2.4), whose request opens with an `election_type` byte and whose answer carries a top-level error code - a preferred and an **unclean** election of the same healthy partition, both answered 84 |
| `create-topics.json` | 4 of 11 | **CreateTopics v3** (Kafka 2.0, KIP-219) and **v4** (Kafka 2.4, KIP-464): the v4 pair asks for the broker's own `num.partitions` and `default.replication.factor` with -1/-1 and an empty assignment, and the container answers 0 and creates three partitions with one replica each |
| `incremental-alter-configs.json` | 6 of 6, **new file** | **IncrementalAlterConfigs v0** (Kafka 2.3, KIP-339): the three operations a topic accepts in one resource (SET, APPEND and the DELETE whose value is the null string), an APPEND to an option that is not a list (**42**) and the cluster-wide default broker resource with a static option, asked with `validate_only` (**42**) |

What **Kafka 2.4** added to the admin and transaction surface, captured with the client id `kafka-client-t1-vectors`:

| File | Of it captured here | What was captured on the 2.8.2 broker |
|---|---|---|
| `alter-partition-reassignments.json` | 8 of 8, **new file** | **AlterPartitionReassignments v0** (KIP-455), the first *flexible* admin frames this package sends: a reassignment to the replica set a partition already has (the only successful one a one-broker cluster can be asked for), a cancellation with nothing in flight (**85** `NoReassignmentInProgress`, the code Kafka 2.4 added for it), a replica set naming a broker that is not alive (**39**), a topic the cluster does not have (**3**, per partition and never at the top level) and a request whose three partitions carry three different codes. The topic is `t1-reassign-vectors`; no vector ever names a **null** topic array, which would move the partitions of every other suite of the shared container |
| `list-partition-reassignments.json` | 2 of 2, **new file** | **ListPartitionReassignments v0** (KIP-455): the request for one named partition and the 14-byte answer of a cluster with nothing in flight. A one-broker cluster completes a reassignment before it answers the request that submitted it, so the empty list is the only answer it can produce; the shape of a partition in flight is documented from the sources |
| `offset-delete.json` | 5 of 5, **new file** | **OffsetDelete v0** (Kafka 2.4, KIP-496), the one api of that release that is **not** flexible: the request and the answer whose top-level error code stands *before* the throttle time, and the three refusals - **86** `GroupSubscribedToTopic` for a topic a live `consumer` group consumes, **68** `NonEmptyGroup` for a live group of another protocol type and **69** `GroupIdNotFound` for a group the coordinator does not know, the last two in 18 bytes that name no partition at all |
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
    "section": "Metadata API (key 3, v0 to v9)",
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
