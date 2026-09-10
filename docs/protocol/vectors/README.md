Wire vectors of the Kafka 0.11.0.3 protocol
===========================================
One file per api, each holding frames that a real Apache Kafka broker sent or accepted. They are the
machine-readable half of [`../1.1.md`](../1.1.md), whose "Wire vectors" section shows the same bytes as annotated
hex dumps. There are **229** of them in 30 files: the 120 the three lines below captured, which a 0.11.0.3 broker
still answers unchanged, and the **109** frames of what Kafka 0.11 added.

A vector is captured on the broker of the line that introduced its api version and is not re-captured while the
frame does not change: the vectors inherited from `0.8.x` were captured on a Kafka 0.8.2.2 broker, those of `0.9.x`
- Produce v1, Fetch v1, OffsetCommit v2, ControlledShutdown v1, the group membership apis and the consumer protocol
structures - on a 0.9.0.1 broker, those of `0.10.x` - message format v1, Metadata v1/v2, Produce v2, Fetch v2/v3,
Offsets v1, OffsetFetch v2, JoinGroup v1, CreateTopics v0/v1, DeleteTopics v0 and SaslHandshake v0 - on a 0.10.2.2
broker, and everything Kafka 0.11 adds on the 0.11.0.3 container of `docker-compose.yml`:

| File | Vectors | What was captured on the 0.11.0.3 broker |
|---|---|---|
| `message-format.json` | 13 of 20 | The **record batch v2** (magic 2): the four codecs, a `LogAppendTime` batch, a three-batch region, record **headers**, null keys and values, a transactional batch, the two real **control batches** with their COMMIT and ABORT markers, and the two down-conversions of a 0.11 log to the message formats v1 and v0 |
| `api-versions.json` | 5 of 5 | ApiVersions **v0 and v1**: the 34 keys the broker serves, in both layouts, and the error code 35 of an unknown version (the two v0 frames were re-captured here) |
| `metadata.json` | 6 of 17 | Metadata **v3 and v4**, and the v4 answer of an absent topic asked for with `allow_auto_topic_creation = false` |
| `offsets.json` | 4 of 14 | Offsets **v2** in both isolation levels |
| `offset-commit.json`, `offset-fetch.json` | 2 of 8, 2 of 15 | OffsetCommit **v3** and OffsetFetch **v3** |
| `group-coordinator.json` | 4 of 6 | GroupCoordinator **v1** for a consumer group and for a transactional id |
| `join-group.json`, `sync-group.json`, `heartbeat.json`, `leave-group.json` | 2 of 8, 2 of 6, 2 of 8, 2 of 6 | The group membership apis one version up: JoinGroup **v2**, SyncGroup/Heartbeat/LeaveGroup **v1** |
| `describe-groups.json`, `list-groups.json` | 2 of 6, 2 of 4 | DescribeGroups **v1** and ListGroups **v1** |
| `create-topics.json`, `delete-topics.json` | 2 of 7, 2 of 5 | CreateTopics **v2** and DeleteTopics **v1** |
| `offset-for-leader-epoch.json` | 4 of 4 | OffsetForLeaderEpoch **v0** (KIP-101), the epoch of a partition and of one the cluster does not host |
| `produce.json` | 8 of 16 | Produce **v3**: the request with its nullable `transactional_id` and a record batch v2, the two answers of a `CreateTime` and a `LogAppendTime` topic - which are the version 2 frame, because `PRODUCE_RESPONSE_V3` is `PRODUCE_RESPONSE_V2` - and the four frames of the idempotent producer: a numbered batch, the answer to the very same frame sent again, and an out-of-order sequence |
| `fetch.json` | 8 of 19 | Fetch **v4 and v5**: both versions at both isolation levels, which is where the `last_stable_offset = -1` and the null `aborted_transactions` of a `read_uncommitted` answer come from, plus the `read_committed` answers of a partition whose only transaction was aborted - with an empty and with a filled `aborted_transactions` array |
| `delete-records.json` | 6 of 6 | DeleteRecords **v0** (KIP-107): the low watermark it moves, an offset above the high watermark and an unknown topic |
| `describe-configs.json` | 6 of 6 | DescribeConfigs **v0** (KIP-133) of a topic and of a broker resource, and the 42 of a broker id the answering broker does not have |
| `alter-configs.json` | 6 of 6 | AlterConfigs **v0** (KIP-133): a topic that is altered, an unknown option name (40) and the broker resource a 0.11 broker refuses (42) |
| `init-producer-id.json` | 6 of 6 | InitProducerId **v0** (KIP-98) with a null transactional id, with a real one, and the 50 of a transaction timeout above the broker's maximum |
| `add-partitions-to-txn.json` | 4 of 4 | AddPartitionsToTxn **v0** (KIP-98), and the 47 a producer whose epoch was bumped away gets - reported per partition, because the api has no top-level error code |
| `add-offsets-to-txn.json` | 2 of 2 | AddOffsetsToTxn **v0**: the request that enrols the offsets of a consumer group into a transaction |
| `end-txn.json` | 6 of 6 | EndTxn **v0**: a commit, an abort, and the 48 of an abort that follows a commit of the same transactional id |
| `txn-offset-commit.json` | 2 of 2 | TxnOffsetCommit **v0**: the offsets a transaction carries to the **group** coordinator |
| `write-txn-markers.json` | 2 of 2 | WriteTxnMarkers **v0** - a broker-to-broker frame, which an unsecured broker serves to an ordinary client, and the only answer of 0.11 without a `throttle_time_ms` |

The remaining three files - `consumer-protocol.json`, `controlled-shutdown.json` and `sasl-handshake.json` - carry
no 0.11 frame at all: their apis and structures are unchanged since the line that captured them.

That the older frames are still the current ones is not an assumption:
`tests/Integration/ApiVersionProbeTest.php` asks the 0.11.0.3 broker with a real **ApiVersions** request
(`api-versions.json`) which versions it serves, and sends a frame of every one of them.

```json
{
    "api": "metadata",
    "apiKey": 3,
    "section": "Metadata API (key 3, v0 to v4)",
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
