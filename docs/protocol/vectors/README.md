Wire vectors of the Kafka 0.8.2.2 protocol
==========================================

One file per api, each holding frames that a real Apache Kafka 0.8.2.2 broker sent or accepted. They are the
machine-readable half of [`../0.8.2.md`](../0.8.2.md), whose "Wire vectors" section shows the same bytes as annotated
hex dumps.

```json
{
    "api": "metadata",
    "apiKey": 3,
    "section": "Metadata API (key 3, v0)",
    "vectors": [
        {
            "id": "metadata.request.v0.all-topics",
            "kind": "request",
            "class": "Protocol\\Kafka\\Protocol\\Request\\MetadataRequest",
            "version": 0,
            "source": "broker",
            "description": "…",
            "hex": "0000001200030000…",
            "fields": { "messageSize": 18, "apiKey": 3, "…": "…" }
        }
    ]
}
```

* `hex` is the complete frame, `Size` field included.
* `fields` mirrors the scheme of the class: the field names and the order are the ones of `getScheme()`, nested
  objects are nested maps, and a raw byte field - only the message set of Produce and Fetch in 0.8 - is written as
  `{"$bytes": "<hex>"}`.
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
