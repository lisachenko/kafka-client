# Handoff: the `main` line (Kafka 0.11)

State at handoff: the `0.10.x` line is **complete**. Everything a Kafka 0.10.2.2 broker speaks is implemented on the
`BinarySchema` engine and verified against a real broker — 1054 unit tests, 127 compliance tests replaying the 120
wire vectors of the three lines, and 285 integration tests against a 0.10.2.2 broker over its four listeners
(`vendor/bin/phpunit` counts 1466 with the integration suite skipped). The grammar is written down in
[`docs/protocol/0.10.2.md`](../protocol/0.10.2.md), the machine-readable frames in
[`docs/protocol/vectors`](../protocol/vectors), and the behaviour a client cannot read out of the grammar in that
document's "Broker quirks and observations" section.

`main` is the **last** line of the cascade: it speaks Kafka **0.11.0.3** (the last release of the 0.11 line, on the
principle the three lines below it follow — `0.8.x` speaks 0.8.2.2, `0.9.x` 0.9.0.1, `0.10.x` 0.10.2.2). It is also
the only line whose current code was not written for the schema engine: what is on `main` today is the pre-schema
implementation with hand-written `pack()`/`unpack()` fragments, a `RecordBatch` of format v2, an `ApiKeys` that runs
to 33 and a `KafkaException` that stops at 35. By rule 5 of [`docs/CASCADE.md`](../CASCADE.md) the parts of it that
target the removed design are **dropped in the cascade merge** and re-implemented on the 0.10 design, starting from
`main`'s own classes wherever they are usable.

Read `CLAUDE.md` first (hard rules, toolchain, the Composer/phpstan sandbox workaround, Docker, the JIT caveat).

## First decision for the owner: which Kafka release the line speaks

Everything below is written for **Kafka 0.11.0.3**. That covers both raises of the 0.11 line:

* **0.11.0.0** — record batch v2 (varints, headers, delta-encoded offsets and timestamps), the idempotent and
  transactional producer with the api keys 21–28, the ACL and config apis 29–33, `throttle_time_ms` in the group
  apis, Produce v3, Fetch v4/v5, Metadata v3/v4, OffsetCommit v3, OffsetFetch v3, ListOffsets v2, error codes
  45–56.
* **0.11.0.1/0.11.0.2/0.11.0.3** — bug-fix releases; no protocol change. The api-version table of a 0.11.0.3 broker
  is the one below, and `SaslAuthenticate` (key 36) is Kafka **1.0**, i.e. still out of scope.

If the owner prefers "0.11.0.0 only", nothing in this plan changes.

## What Kafka 0.11.0.3 adds over 0.10.2.2

Verified against `clients/src/main/java/org/apache/kafka/common/protocol/{Protocol,ApiKeys,Errors}.java` at the tag
**0.11.0.3** (`git clone --depth 1 --branch 0.11.0.3 https://github.com/apache/kafka`); as always the broker of the
line has the last word, and this line can simply **ask** it — the ApiVersions api of `0.10.x` is already implemented
and `tests/Integration/ApiVersionProbeTest.php` pins the answer.

| Api | 0.10.2.2 | 0.11.0.3 | Start from |
|---|---|---|---|
| Produce (0) | v0–v2 | **v3**: the request gains a `transactional_id` nullable string before `acks`, and the record set it carries is a **record batch v2**; the response partition gains `log_start_offset` int64 | `ProduceRequest`/`ProduceResponse` of `0.10.x`, plus `main`'s `RecordBatch` |
| Fetch (1) | v0–v3 | **v4**: request gains `isolation_level` int8 after `max_bytes`, response partition gains `last_stable_offset` int64 and `aborted_transactions [producer_id first_offset]`. **v5**: both gain `log_start_offset` int64 | `main`'s `FetchRequest` (v5, `READ_COMMITTED`/`READ_UNCOMMITTED`) and `Data/FetchResponseAbortedTransaction`, which already exist |
| Offsets (2) | v0, v1 | **v2**: request gains `isolation_level` int8 after `replica_id`, response gains a top-level `throttle_time_ms` | `OffsetsRequest`/`OffsetsResponse` of `0.10.x` |
| Metadata (3) | v0–v2 | **v3**: response gains a leading `throttle_time_ms` int32. **v4**: request gains `allow_auto_topic_creation` boolean | `MetadataRequest`/`MetadataResponse` of `0.10.x` |
| OffsetCommit (8) | v0–v2 | **v3**: response gains a leading `throttle_time_ms` | `0.10.x` |
| OffsetFetch (9) | v0–v2 | **v3**: response gains a leading `throttle_time_ms` | `0.10.x` |
| GroupCoordinator (10) | v0 | **v1**: request gains `coordinator_type` int8 (0 = group, 1 = transaction), response gains `throttle_time_ms` and an `error_message` nullable string. The api is called **FindCoordinator** from 0.11 on — `main`'s identifiers decide the class name | `GroupCoordinatorRequest`/`Response` of `0.10.x` |
| JoinGroup (11), Heartbeat (12), LeaveGroup (13), SyncGroup (14), DescribeGroups (15), ListGroups (16) | v0/v1 | **+1 version each**, all of them adding nothing but the leading `throttle_time_ms` of the response (JoinGroup v2, Heartbeat v1, LeaveGroup v1, SyncGroup v1, DescribeGroups v1, ListGroups v1) | `0.10.x`; one `…V0`/`…V1` subclass per api, the pattern of this repository |
| CreateTopics (19) | v0, v1 | **v2**: response gains a leading `throttle_time_ms` | `0.10.x` |
| DeleteTopics (20) | v0 | **v1**: response gains a leading `throttle_time_ms` | `0.10.x` |
| **DeleteRecords (21)** | – | new: `[topic [partition offset]] timeout` → `throttle_time_ms [topic [partition low_watermark error_code]]`, truncates a log below an offset | nothing; `ApiKeys::DELETE_RECORDS` exists on `main` |
| **InitProducerId (22)** | – | new: `transactional_id transaction_timeout_ms` → `throttle_time_ms error_code producer_id producer_epoch` — the first request of an idempotent producer | nothing |
| **OffsetForLeaderEpoch (23)** | – | new, broker→broker (replica fetcher), needed only for completeness | nothing |
| **AddPartitionsToTxn (24), AddOffsetsToTxn (25), EndTxn (26), WriteTxnMarkers (27), TxnOffsetCommit (28)** | – | new: the transactional producer. 27 is broker→broker | nothing |
| **DescribeAcls (29), CreateAcls (30), DeleteAcls (31)** | – | new: the ACL apis, only useful against a broker with an `authorizer.class.name` | nothing |
| **DescribeConfigs (32), AlterConfigs (33)** | – | new: read and change broker/topic configuration through the protocol instead of ZooKeeper | nothing |
| Message format | v0, v1 | **record batch v2** (magic 2), see below | `main`'s `Common\Record\{RecordBatch,Record,Header}` and `Common\Utils\ByteUtils` |
| Error codes | −1 … 44 | **45** `OUT_OF_ORDER_SEQUENCE_NUMBER`, **46** `DUPLICATE_SEQUENCE_NUMBER`, **47** `INVALID_PRODUCER_EPOCH`, **48** `INVALID_TXN_STATE`, **49** `INVALID_PRODUCER_ID_MAPPING`, **50** `INVALID_TRANSACTION_TIMEOUT`, **51** `CONCURRENT_TRANSACTIONS`, **52** `TRANSACTION_COORDINATOR_FENCED`, **53** `TRANSACTIONAL_ID_AUTHORIZATION_FAILED`, **54** `SECURITY_DISABLED`, **55** `OPERATION_NOT_ATTEMPTED`, (**56** `KAFKA_STORAGE_ERROR` is 1.0) | `Common/Errors/*` of `0.10.x`; `main`'s `KafkaException` stops at 35 and has to be extended, not copied |
| Producer | at most once, retries | **idempotent** (`enable.idempotence`, producer id + epoch + sequence per partition) and **transactional** (`transactional.id`, `initTransactions()`, `beginTransaction()`, `sendOffsetsToTransaction()`, `commitTransaction()`, `abortTransaction()`) | `main`'s `KafkaProducer` has none of it |
| Consumer | – | `isolation.level = read_committed` skips aborted records with the help of the `aborted_transactions` of a Fetch v4 answer, and stops at the `last_stable_offset` instead of the high water mark | `main`'s `KafkaConsumer` has none of it |

### Record batch v2 is the big one

`main` already carries `Common\Record\RecordBatch`, `Common\Record\Header` and `Common\Utils\ByteUtils` (varint and
zigzag), which is the right place to start, but the batch has to be rebuilt on the schema engine and next to the
`MessageSet`/`Message` of the lines below, because a **0.11 broker still reads and writes the older formats** and a
topic keeps whatever `message.format.version` says:

```
RecordBatch => firstOffset length partitionLeaderEpoch magic(2) crc attributes lastOffsetDelta
               firstTimestamp maxTimestamp producerId producerEpoch firstSequence [Record]
  Record     => length(varint) attributes(int8) timestampDelta(varint) offsetDelta(varint)
                keyLength(varint) key valueLength(varint) value [Header]
  Header     => keyLength(varint) key valueLength(varint) value
```

* the CRC is **CRC-32C** (Castagnoli), not the CRC-32 of the message formats v0/v1, and it covers the batch from
  the attributes on — PHP's `hash('crc32c', …)` has it since 7.4;
* `attributes` carry the codec (bits 0–2), the timestamp type (bit 3), `isTransactional` (bit 4) and `isControl`
  (bit 5); a **control batch** (an abort or commit marker) is never handed to an application;
* offsets and timestamps inside a batch are **deltas** of the batch header's values, which is what makes the schema
  engine's per-field reading insufficient on its own: the record entries need a small reader of their own, as the
  `MessageSet` of the lower lines has;
* the engine needs `TYPE_VARINT` and `TYPE_ZIGZAG` (or one varint type with a zigzag flag) plus a nullable
  varint-prefixed byte array, and `BinarySchema::getObjectTypeSize()` has to size them — that is the first ticket
  of the line, before any api.

## What the cascade `0.10.x → main` drops and re-implements

Rule 4 of `docs/CASCADE.md`: the lower line wins the shared part, the higher line's version-specific additions come
back on top as fields and constants. Concretely, on a `cascade/0.10.x-into-main` branch created from `main`:

**Close the automatic pull request first.** `.github/workflows/cascade.yml` opened **PR #48** (`0.10.x` → `main`)
and it conflicts in almost every file, because the two trees share nothing but their history. Close it with a note
that it is superseded, create `cascade/0.10.x-into-main` from `main`, `git merge origin/0.10.x` into it and resolve
there; the workflow will then track that branch's PR.

**Keep from `0.10.x`** (the whole tree, in practice): the schema engine and its two fixes (signed `int8`, a null
byte array as `-1`), the framing, `Client`, `AdminClient`, `KafkaProducer`, `KafkaConsumer` with the group
membership, the assignors, the message formats v0/v1 with gzip/snappy/lz4, SSL and SASL/PLAIN, the configuration
classes, every test suite, the wire vectors, the protocol document, the tooling and the Docker broker.

**Drop from `main`** — all of it re-implemented as the tickets below, and listed in the merge commit:

* every request/response class with `$header = null;` instead of `parent::getScheme() + [...]` — 14 of them
  (`ProduceRequest`, `FetchRequest`, `MetadataRequest`, `OffsetsRequest`, `OffsetCommitRequest`,
  `OffsetFetchRequest`, `GroupCoordinatorRequest`, `JoinGroupRequest`, `SyncGroupRequest`, `HeartbeatRequest`,
  `LeaveGroupRequest`, `DescribeGroupsRequest`, `ControlledShutdownRequest`, `SaslHandshakeRequest`). A request that
  declares `$header = null` writes **no** request header at all; the version-specific *fields* of those classes are
  what the tickets keep;
* `Common\Record\RecordBatch` and `Common\Record\Record` as they stand (untyped properties, no `MessageSet` next to
  them, no magic-byte dispatch) — the batch comes back on the 0.10 `Message`/`MessageSet` design;
* `KafkaException` and `Common/Errors/*` of `main`, which stop at 35 and lack the codes 36–44 the 0.10 line needs —
  the 0.10 classes win and the codes 45–55 are added to them;
* `ApiKeys` of `main` keeps its constants 21–33 (they are correct for 0.11) but is otherwise the 0.10 file;
* `main`'s `KafkaConsumer`/`KafkaProducer`/`AdminClient`, which are a subset of the 0.10 ones (no group
  coordination, no assignors, no admin topic apis) with a few 0.11 fields — keep the 0.10 classes and re-add the
  0.11 fields per ticket;
* `main`'s six test files, which the 0.10 suites cover many times over. Do not lose
  `tests/Common/Record/HeaderTest.php`: it is the only existing test of the record headers of 0.11.

**Never lower** anything: the versions of the table above are the protocol of this line, and a 0.11 broker really
does serve them.

## Environment recipe

1. **Docker**: `nohup dockerd >/tmp/dockerd.log 2>&1 &` if `docker info` fails. Copy `docker/kafka-0.10.2.2/` to
   `docker/kafka-0.11.0.3/`, bump `KAFKA_VERSION`, `SCALA_VERSION` (2.11 or 2.12 for 0.11) and `KAFKA_DIST`, and
   point `docker-compose.yml` at it. The tarball comes from `archive.apache.org`, which is reachable; old Docker
   Hub images with v1 manifests are not pullable.
2. **The proxy-CA trick**: the sandbox intercepts TLS, so the `curl` of the build fails unless the proxy CA is
   trusted. `docker/kafka-<version>/ca/` is copied into `/usr/local/share/ca-certificates/extra/` and
   `update-ca-certificates` runs before the download; drop `/root/.ccr/ca-bundle.crt` in there when the build
   reports certificate errors. Keep the mechanism in the new image.
3. **Listeners**: carry over all four — `PLAINTEXT://0.0.0.0:9092`, `SSL://0.0.0.0:9093`,
   `SASL_PLAINTEXT://0.0.0.0:9094`, `SASL_SSL://0.0.0.0:9095` — with the checked-in `ssl/broker.crt`/`broker.key`
   and the `jaas.conf` of the 0.10 image. `KAFKA_SSL_BOOTSTRAP_SERVERS`, `KAFKA_SASL_BOOTSTRAP_SERVERS` and
   `KAFKA_SASL_SSL_BOOTSTRAP_SERVERS` are already wired into `IntegrationTestCase`.
4. **Broker settings** the suite depends on: `auto.create.topics.enable=true`, `num.partitions=3`,
   `offsets.topic.replication.factor=1`, `offsets.topic.num.partitions=5`, `group.min.session.timeout.ms=1000`,
   `group.max.session.timeout.ms=60000`, `delete.topic.enable=true`. For 0.11 add
   `transaction.state.log.replication.factor=1` and `transaction.state.log.min.isr=1` — without them the
   `__transaction_state` topic cannot be created on a one-broker cluster and every transactional test fails with 15.
   A topic with `message.format.version=0.10.0` (and one with `0.9.0`) is what proves the down-conversion paths,
   which a 0.11 broker has two of.
5. **Dependencies**: `tools/dev/vendor-from-source.sh` once per session, then `cp -a vendor/` into every agent
   worktree. Never `composer update` in the sandbox.
6. **In-container tools**: `kafka-topics.sh --zookeeper localhost:2181`, `kafka-console-producer.sh`,
   `kafka-console-consumer.sh --new-consumer --bootstrap-server localhost:9092 --consumer.config <file>`,
   `kafka-consumer-groups.sh --new-consumer`, `kafka-configs.sh`, and
   `kafka-run-class.sh kafka.tools.DumpLogSegments --files /tmp/kafka-logs/<topic>-0/00000000000000000000.log --print-data-log --deep-iteration`,
   which prints a record batch v2 with its producer id, epoch, sequence and headers — the fastest way to see what
   the format really looks like. `docker logs <container>` shows why the broker closed a connection.

## Ticket plan (waves of up to four agents in isolated worktrees)

Wave 1 — foundation
- **T1 broker, surface, docs**: `docker/kafka-0.11.0.3/`, the protocol document renamed to
  `docs/protocol/0.11.0.md` with every `@see` updated, `ApiKeys` 21–33, error codes 45–55, the ApiVersions table of
  the new broker pinned by the probe, README/CHANGELOG headers.
- **T2 record batch v2**: `TYPE_VARINT`/zigzag in the engine, `RecordBatch`, `Record` with headers, CRC-32C, the
  control batches, and the two down-conversions a 0.11 broker performs. The largest single piece of the line.

Wave 2 — the versioned apis (all of them mechanical once the batch is there)
- **T3 throttle time everywhere**: GroupCoordinator/FindCoordinator v1, JoinGroup v2, Heartbeat v1, LeaveGroup v1,
  SyncGroup v1, DescribeGroups v1, ListGroups v1, OffsetCommit v3, OffsetFetch v3, CreateTopics v2, DeleteTopics
  v1, Metadata v3/v4, Offsets v2 — one `…V<n>` subclass per api and the throttle time on the response.
- **T4 Produce v3 / Fetch v4 and v5**: `transactional_id`, `log_start_offset`, `isolation_level`,
  `last_stable_offset` and the aborted transactions.
- **T5 DeleteRecords (21) and the config apis (32, 33)** with the `AdminClient` methods.
- **T6 the ACL apis (29–31)**, only if the owner wants them; they need a broker with an authorizer.

Wave 3 — the producer semantics
- **T7 idempotent producer**: InitProducerId (22), the producer id/epoch/sequence bookkeeping per partition,
  `enable.idempotence`, and what a client does with 45/46/47.
- **T8 transactional producer**: AddPartitionsToTxn (24), AddOffsetsToTxn (25), EndTxn (26), TxnOffsetCommit (28),
  the transaction coordinator lookup (FindCoordinator v1 with `coordinator_type = 1`) and the consumer's
  `isolation.level = read_committed`.
- **T9 docs, compliance, README matrix, CHANGELOG, examples, and the handoff** — the closing ticket of every line.
  There is no line above `main`, so its "handoff" is the release notes of the package instead.

Every ticket: byte-exact vectors from the real broker, integration tests with unique topic/group names, the whole
gate green before the PR, and a PR against the integration branch with "Closes #n".

## Pitfalls of the 0.10.x session (do not rediscover them)

* **The shared broker fills up.** Every agent runs the *whole* integration suite before its PR and each run creates
  about a hundred topics; after two waves the container held 4000 of them. An all-topics Metadata request against
  4000 topics takes longer than a one-second session timeout, which is what broke `ConsumerGroupTest` once.
  Recreate the broker between waves (`docker compose down -v && docker compose up -d --wait`) and write every test
  helper so that it bootstraps with a **named** topic instead of asking for the whole cluster.
* **A 0.10/0.11 broker closes the socket on a frame it cannot parse** — an unknown api key, a version it does not
  serve, or a body that does not match the schema of a version it does. The client sees the end of the stream, not
  a timeout, and `docker logs <container>` names the reason. Only ApiVersions answers an unknown version (with the
  error code 35), and ControlledShutdown ignores its version altogether.
* **An empty group stays `Empty`**, with its committed offsets, instead of being dropped; `Dead` now really means
  "this coordinator has never heard of the group".
* **A 0.10.2.2 broker answers Metadata with one broker even on a topic-less cluster**, where 0.8/0.9 answered zero
  brokers. An empty broker array still means "not ready, retry", so the readiness probe stays.
* **The consumer defaults changed with 0.10.1** and the 0.11 ones are the same: `session.timeout.ms` 10000,
  `request.timeout.ms` 305000, `max.poll.interval.ms` 300000. `request.timeout.ms` has to exceed both other values,
  because a JoinGroup blocks the connection for a whole rebalance — and two members of one group need two PHP
  processes (`tests/Fixture/consumer-group-member.php` + `ConsumerGroupMemberProcess`).
* **The recurring merge conflicts of parallel tickets were exactly four places**, all of them "keep both":
  the 0.10.2.2 bullet of the "Wire vectors" preamble, the end of the "Wire vectors" section, the `use` list of
  `src/Kafka/Client.php` and the provider list of `tests/Compliance/ProtocolVectorTest.php`. The `### <vector id>`
  blocks, the vector files and the api sections themselves never conflicted — that part of the design works, keep
  it.
* **`private` properties break versioned subclasses**: a `…V0` class that reuses its parent's scheme needs
  `protected` (or public) properties. Write new request classes with `protected readonly` promoted properties.
* **The sandbox's PHP 8.5 CLI runs the tracing JIT by default and it miscompiles the pure-PHP LZ4 decoder** once
  its functions get hot: `Lz4Test` and the lz4 message-format vectors then fail with an order-dependent
  `CorruptMessageException`, while 0 of 200 round trips fail with `php -d opcache.jit=0`. Run the suite as
  `php -d opcache.jit=0 vendor/bin/phpunit` there; CI on PHP 8.4 is unaffected. Expect the same for the CRC-32C and
  varint code of the record batch — measure before believing a failure is a defect of the code.
* **Nested branch names** (`main/foo`) are fine on `main`, but keep the `t<n>-<slug>` convention of the other lines
  so that the tooling stays uniform; `Closes #n` **does** auto-close on `main`, unlike on the protocol branches.

## Open questions for the owner

1. **Which Kafka release the line speaks** — 0.11.0.3 is assumed above.
2. **How far the transactional support should go.** The idempotent producer is a bounded piece of work; the
   transactional one needs a coordinator lookup, a state machine, `sendOffsetsToTransaction()` and a consumer that
   filters aborted records. It could also be declared out of scope, leaving `main` with the wire formats but
   without the semantics.
3. **The ACL apis (29–31)** need a broker with an `authorizer.class.name`; implement them against a configured
   container, or from the specification alone and mark them unverified?
4. **What happens to `main`'s public API where it differs from the lower lines** (`protected const VERSION`, the
   untyped `RecordBatch` properties, `Consumer\Subscription` without the assignors). The cascade rules say the
   lower line wins, which changes `main`'s API — the owner should confirm that this is intended before the merge,
   because it is the API the package publishes.
5. **Whether `main` keeps the `0.8.x`/`0.9.x`/`0.10.x` protocol documents and vectors** or only its own. The
   compliance suite replays every one of the 120 vectors of the lower lines against the classes of this line, which
   is the strongest regression test the repository has — dropping them would lose it.
