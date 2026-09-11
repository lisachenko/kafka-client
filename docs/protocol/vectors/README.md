Wire vectors of the Kafka 2.8.2 protocol
========================================
One file per api, each holding frames that a real Apache Kafka broker sent or accepted. They are the
machine-readable half of [`../2.8.md`](../2.8.md), whose "Wire vectors" section shows the same bytes as annotated
hex dumps. There are **406** of them in **37** files: **88** were captured on the `kafka-2-8-2` container of the
**2.x** line - the request and the answer of every version Kafka **2.0** added to the producer and consumer apis
(14 frames), to the admin, the transaction and the delegation-token apis (34 frames), to the ten group apis
(20 frames) and to ApiVersions (2 frames), nearly all of them KIP-219 bumps, the 6 frames of what Kafka
**2.1** added to OffsetCommit and OffsetFetch and the 4 of what it added to DeleteTopics and TxnOffsetCommit, and
the 8 frames of **Kafka 2.2**: SaslAuthenticate v1, ControlledShutdown v2 and the new api ElectLeaders (key 43),
whose `elect-leaders.json` is the one file this line added - and of the other 318, **229** were
captured by the four lines below the 1.x one and are replayed against the classes of this line unchanged, while
**89** were captured on the 1.1.1 broker of the 1.x line. Three of the inherited vectors were **re-captured**
rather than added - `apiversions.response.v0` and `.v1`, whose whole content is the api-key table of the broker,
and `apiversions.response.v0.unsupported-version`, which gained the api row of KIP-511.

What the group apis of this line have captured so far is the KIP-219 version bump of Kafka 2.0 — OffsetCommit
v4, OffsetFetch v4, FindCoordinator v2, JoinGroup v3, Heartbeat v2, LeaveGroup v2, SyncGroup v2, DescribeGroups v2,
ListGroups v2 and DeleteGroups v1, one request/response pair each, taken from one life of the group
`t3-kip219-group` — and the same bump of the four apis the producer and the consumer send:

| File | Of it captured here | What was captured on the 2.8.2 broker |
|---|---|---|
| `produce.json` | 3 of 27 | **Produce v6** (KIP-219), request and answer, plus the throttled answer of a `producer_byte_rate` quota — the frame that shows what KIP-219 changed: `ThrottleTime = 2381` in an answer that arrived after about a millisecond |
| `fetch.json` | 5 of 38 | **Fetch v8** (KIP-219), request and answer; the throttled answer, whose topics array is **empty**; and the pair of a Fetch v3 against a topic with `message.downconversion.enable=false`, which is answered **35** `UNSUPPORTED_VERSION` per partition (KIP-283) |
| `offsets.json` | 2 of 16 | **ListOffsets v3** (KIP-219), the version 2 frames with another number in the header |
| `metadata.json` | 2 of 21 | **Metadata v6** (KIP-219), likewise |
| `offset-for-leader-epoch.json` | 2 of 6 | **OffsetForLeaderEpoch v1** (KIP-279): the `leader_epoch` the answered `end_offset` belongs to, inserted between the partition id and the offset |

What Kafka 2.1 added to the two offset apis: OffsetCommit v5 (the frame without
`retention_time`, KIP-211), OffsetCommit v6 and OffsetFetch v5 (the `committed_leader_epoch` of KIP-320).

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
    "section": "Metadata API (key 3, v0 to v6)",
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
