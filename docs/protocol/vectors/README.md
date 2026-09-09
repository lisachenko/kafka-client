Wire vectors of the Kafka 0.10.2.2 protocol
===========================================

One file per api, each holding frames that a real Apache Kafka broker sent or accepted. They are the
machine-readable half of [`../0.10.2.md`](../0.10.2.md), whose "Wire vectors" section shows the same bytes as annotated
hex dumps.

A vector is captured on the broker of the line that introduced its api version and is not re-captured while the
frame does not change: the vectors inherited from `0.8.x` were captured on a Kafka 0.8.2.2 broker, those of `0.9.x`
- Produce v1, Fetch v1, OffsetCommit v2, ControlledShutdown v1, the group membership apis and the consumer protocol
structures - on a 0.9.0.1 broker, and everything Kafka 0.10 adds on the 0.10.2.2 container of `docker-compose.yml`:

| File | What was captured on the 0.10.2.2 broker |
|---|---|
| `api-versions.json` | ApiVersions v0, the 21 keys the broker serves, and the error code 35 of an unknown version |
| `message-format.json` | seven message sets of format v1 and the format v0 the broker converts an lz4 set down to |
| `metadata.json` | Metadata v1 and v2 - `rack`, `is_internal`, `controller_id`, `cluster_id` - next to the v0 frames |
| `produce.json` | Produce v2 with the `log_append_time` of a `LogAppendTime` topic |
| `fetch.json` | Fetch v2 and v3, including the request-level `max_bytes` of v3 |
| `offsets.json` | Offsets v1: by timestamp, one offset per partition, and the 43 of a message-format-v0 topic |
| `offset-fetch.json` | OffsetFetch v2: named topics, `null` (all topics), `[]` (no topic), and the group error code |
| `join-group.json` | JoinGroup v1 with the `rebalance_timeout` |
| `create-topics.json` | CreateTopics v0 and v1, including the `error_message` of v1 |
| `delete-topics.json` | DeleteTopics v0, including the answer for a topic the cluster does not have |
| `sasl-handshake.json` | SaslHandshake v0 and the two frames of the PLAIN token exchange (`kind: structure`) |

That the older frames are still the current ones is not an assumption:
`tests/Integration/ApiVersionProbeTest.php` asks the 0.10.2.2 broker with a real **ApiVersions** request
(`api-versions.json`) which versions it serves, and sends a frame of every one of them.

```json
{
    "api": "metadata",
    "apiKey": 3,
    "section": "Metadata API (key 3, v0, v1 and v2)",
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
