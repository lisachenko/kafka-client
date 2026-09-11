# The 2.x line (Kafka 2.0 to 2.8, verified against 2.8.2) — the plan

**State: in development on `main`.** This file is the plan the 2.x line is built from; when the line is complete it
becomes its release record, with this plan kept below the release notes, exactly as `docs/handoff/1.x.md` and
`docs/handoff/0.11.x.md` were written. The line is built on the integration branch `claude/kafka-2-0-line-gr07ns`
and merged into `main` as one pull request at the end.

State at handoff: the **1.x line is complete** — 1737 unit tests, 325 compliance tests replaying the **318** wire
vectors of the five lines and 549 integration tests against a 1.1.1 container over its four listeners, without a
skip; its record is [`docs/handoff/1.x.md`](1.x.md), its grammar was `docs/protocol/1.1.md` (renamed to
[`docs/protocol/2.8.md`](../protocol/2.8.md) on this line). The finished 1.x tree was branched off as **`1.x`** at
`2ee4866` (the merge of PR #109), PR #110 wired the branch into the cascade and CI workflows and PR #111 cascaded
it into `main`, so `main` starts this line identical to `1.x` plus the foundation commit described below.

Read `CLAUDE.md` first (hard rules, toolchain, the Composer/phpstan sandbox workaround, Docker, the JIT caveat).

## The decision of the line: one branch for the whole 2.x major

The lines up to 0.11 were **minor** lines (0.8.x, 0.9.x, 0.10.x, 0.11.x), each speaking the last release of one
Kafka minor. From 1.x on the lines are **major** lines: `1.x` speaks 1.1.1 and with it everything 1.0 and 1.1 added,
and this line — `main`, to be branched off as `2.x` when it is complete — speaks **Kafka 2.8.2**, the last release of
the 2.x major, and with it everything 2.0, 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7 and 2.8 added. The original handoff of
this line (`docs/handoff/2.0.x.md`, written by the 1.x session) planned a 2.0.1 line with the option of 2.1/2.2; the
owner chose the whole major instead, and this file supersedes it.

One broker is enough to verify the whole major: a 2.8.2 broker still serves **every** version 2.0 to 2.7 added, so
every version bump of the line is captured on the same container, and the api-key table of the document is its
literal ApiVersions answer. Where a version has to be attributed to the minor release that added it, the tags
`2.0.1`, `2.1.1`, `2.2.2`, `2.3.1`, `2.4.1`, `2.5.1`, `2.6.3`, `2.7.2` and `2.8.2` are the last releases of every
2.x minor and are what "@ 2.4.1" means in this repository (all of them exist in the Apache repository).

**The schema authority changed once more.** From Kafka 2.4 the Java client generates its request and response
classes from JSON specifications, and by 2.8.2 every api has one:
`clients/src/main/resources/common/message/<Api>{Request,Response}.json` with `validVersions`, `flexibleVersions`,
and per field `versions`, `nullableVersions`, `taggedVersions`/`tag`, `default` and `entityType`, plus a `// Version
N …` comment per version that names the KIP. That file is what a ticket reads; `ApiKeys.java` (the api table),
`Errors.java` (the error codes) and `core/src/main/scala/kafka/server/KafkaApis.scala` (what the broker does with a
request, and which error a version gets — `if (version < N) OLD else NEW` is a pattern of this line) stay next to it.
As always the container has the last word.

## What Kafka 2.x adds over 1.1.1

**Four things change the engine or the runtime, everything else is versions and new apis.**

| What | KIP | Kafka | Where it lands |
|---|---|---|---|
| **Flexible versions**: compact strings, bytes and arrays (an unsigned varint `length + 1`, 0 for null), **tagged fields** (an unsigned varint count, then `tag`/`size`/`value` triples) at the end of every structure, the **request header v2** (`api_key api_version correlation_id client_id TAG_BUFFER`; `client_id` stays a plain nullable STRING) and the **response header v1** (`correlation_id TAG_BUFFER`). A version is flexible from the `flexibleVersions` of its JSON on; the ApiVersions **response is always sent with the response header v0**, tagged fields or not, so that a client can read the 35 of an older broker | KIP-482 | 2.4 | `BinarySchema`, `Stream`, `AbstractRequest`, `AbstractResponse` (T1); every flexible version afterwards (wave 2) |
| **Leader epochs**: `current_leader_epoch` in Fetch v9, ListOffsets v4, OffsetsForLeaderEpoch v2, OffsetCommit v6, and the `leader_epoch` of Metadata v7, ListOffsets v4, OffsetFetch v5, OffsetsForLeaderEpoch v1; the codes **74** `FENCED_LEADER_EPOCH` and **75** `UNKNOWN_LEADER_EPOCH`; the consumer detects log truncation after a leader change instead of reading past it | KIP-320 (+ KIP-279 for OffsetsForLeaderEpoch v1) | 2.1 (2.0) | the wire in T2/T3, the consumer in T5 |
| **zstd**: the compression codec 4 of the record batch attributes; Produce v7 and Fetch v10 promise that the client understands it, a lower Fetch of a zstd partition is **76** `UNSUPPORTED_COMPRESSION_TYPE` (the broker does not down-convert zstd) | KIP-110 | 2.1 | `Common\Record\*` and `Compression` (T2): through `ext-zstd` when it is loaded, refused with a clear exception otherwise — there is no pure-PHP zstd |
| **KIP-219 throttling**: from the versions 2.0 bumped on, a throttled broker answers **first** and mutes the channel afterwards; a client that sends those versions has to honour `throttle_time_ms` itself. This client sleeps the throttle time before its next request to that broker, as the Java client does, with an option to switch it off; measured against a client quota | KIP-219 | 2.0 | `Client` (T2) |

**22 new api keys** (43 to 64), of which a ZooKeeper-backed broker serves **14**: ElectLeaders (43, KIP-183/460),
IncrementalAlterConfigs (44, KIP-339), AlterPartitionReassignments and ListPartitionReassignments (45/46, KIP-455),
OffsetDelete (47, KIP-496), DescribeClientQuotas and AlterClientQuotas (48/49, KIP-546), DescribeUserScramCredentials
and AlterUserScramCredentials (50/51, KIP-554), AlterIsr (56, KIP-497, broker-to-controller), UpdateFeatures (57,
KIP-584), DescribeCluster (60, KIP-700) and DescribeProducers (61, KIP-664). The raft apis (52-55, KIP-595), Envelope
(58), FetchSnapshot (59) and the broker registration apis (62-64) are the KRaft controller's and never appear in the
ApiVersions answer of this container; their constants exist in `ApiKeys` so that every frame of 2.8.2 can be named,
nothing else of them is implemented. **33 error codes** (72 to 104), all in `KafkaException` since the foundation
(one class each, retriable as in the Java client); which of them the container can produce is what the tickets find
out.

**No new message format**: the record batch v2 (magic 2) is unchanged from 0.11 to 2.8; `log.message.format.version`
of the container is `2.8-IV1`, which is still magic 2. **No new SASL mechanism in this line** (OAUTHBEARER of
KIP-255 and the SCRAM login stay out, decision 4 below).

### The ApiVersions answer of a 2.8.2 broker

The literal answer of the container (`docker/kafka-2.8.2`, probed with a raw ApiVersions v0 frame on 2026-09-11):
`error_code = 0`, **56 apis**: the keys 0 to 51, 56, 57, 60 and 61, exactly the `zkBroker` listener set of the
JSON specifications. The v1 answer is the same table plus `throttle_time_ms = 0`; the v3 answer (flexible request,
compact array, tagged fields) carries the same 56 rows and a **response header v0**. This table is the contract of
the line; `tests/Integration/ApiVersionProbeTest.php` sends a real frame of every one of the 56 keys and one frame
above every one of them, and the "API keys" section of `docs/protocol/2.8.md` is this answer.

| Key | Api | 1.1.1 | 2.8.2 | Flexible from | Added by (last release of the minor that added it) |
|---|---|---|---|---|---|
| 0 | Produce | 0-5 | **0-9** | 9 | v6 2.0, v7 2.1 (zstd), v8 2.4 (KIP-467 record errors), v9 2.8 |
| 1 | Fetch | 0-7 | **0-12** | 12 | v8 2.0, v9-v10 2.1 (KIP-320, zstd), v11 2.3 (KIP-392 rack id / preferred read replica), v12 2.7 (flexible, cluster id tagged field) |
| 2 | ListOffsets | 0-2 | **0-6** | 6 | v3 2.0, v4 2.1 (KIP-320), v5 2.2 (KIP-207, error 78), v6 2.8 |
| 3 | Metadata | 0-5 | **0-11** | 9 | v6 2.0, v7 2.1 (leader epoch), v8 2.3 (KIP-430 authorized operations), v9 2.4 (flexible), v10 2.8 (KIP-516 topic ids), v11 2.8 (no cluster authorized operations) |
| 4 | LeaderAndIsr | 0-1 | 0-5 | 4 | broker-to-broker, not implemented (probed) |
| 5 | StopReplica | 0 | 0-3 | 2 | broker-to-broker, not implemented (probed) |
| 6 | UpdateMetadata | 0-4 | 0-7 | 6 | broker-to-broker, not implemented (probed) |
| 7 | ControlledShutdown | 0-1 | **0-3** | 3 | v2 2.2 (KIP-380 broker epoch), v3 2.4 |
| 8 | OffsetCommit | 0-3 | **0-8** | 8 | v4 2.0, v5 2.1 (retention time removed), v6 2.1 (leader epoch), v7 2.3 (KIP-345 group instance id), v8 2.4 |
| 9 | OffsetFetch | 0-3 | **0-7** | 6 | v4 2.0, v5 2.1 (leader epoch), v6 2.4, v7 2.5 (KIP-447 require stable) |
| 10 | FindCoordinator | 0-1 | **0-3** | 3 | v2 2.0, v3 2.4 |
| 11 | JoinGroup | 0-2 | **0-7** | 6 | v3 2.0, v4 2.2 (KIP-394, error 79), v5 2.3 (KIP-345), v6 2.4, v7 2.5 (KIP-559 protocol type/name in the answer) |
| 12 | Heartbeat | 0-1 | **0-4** | 4 | v2 2.0, v3 2.3 (KIP-345), v4 2.4 |
| 13 | LeaveGroup | 0-1 | **0-4** | 4 | v2 2.0, v3 2.4 (KIP-345 batch of members), v4 2.4 |
| 14 | SyncGroup | 0-1 | **0-5** | 4 | v2 2.0, v3 2.3 (KIP-345), v4 2.4, v5 2.5 (KIP-559) |
| 15 | DescribeGroups | 0-1 | **0-5** | 5 | v2 2.0, v3 2.3 (KIP-430), v4 2.4 (group instance id), v5 2.4 |
| 16 | ListGroups | 0-1 | **0-4** | 3 | v2 2.0, v3 2.4, v4 2.6 (KIP-518 states filter) |
| 17 | SaslHandshake | 0-1 | 0-1 | – | unchanged |
| 18 | ApiVersions | 0-1 | **0-3** | 3 | v2 2.0, v3 2.4 (KIP-511 client software name/version, KIP-584 features as tagged fields) |
| 19 | CreateTopics | 0-2 | **0-7** | 5 | v3 2.0, v4 2.4 (KIP-464 optional partitions/replication factor), v5 2.4 (flexible, KIP-525 configs in the answer), v6 2.7 (KIP-599, error 89), v7 2.8 (topic id in the answer) |
| 20 | DeleteTopics | 0-1 | **0-6** | 4 | v2 2.0, v3 2.1, v4 2.4, v5 2.7 (KIP-599, error message), v6 2.8 (KIP-516 topic ids) |
| 21 | DeleteRecords | 0 | **0-2** | 2 | v1 2.0, v2 2.6 |
| 22 | InitProducerId | 0 | **0-4** | 2 | v1 2.0, v2 2.4, v3 2.5 (KIP-360 producer id/epoch), v4 2.7 (KIP-588, error 90) |
| 23 | OffsetForLeaderEpoch | 0 | **0-4** | 4 | v1 2.0 (KIP-279 leader epoch in the answer), v2 2.1 (KIP-320 current leader epoch), v3 2.3 (replica id), v4 2.8 |
| 24 | AddPartitionsToTxn | 0 | **0-3** | 3 | v1 2.0, v2 2.7 (error 90), v3 2.8 |
| 25 | AddOffsetsToTxn | 0 | **0-3** | 3 | v1 2.0, v2 2.7 (error 90), v3 2.8 |
| 26 | EndTxn | 0 | **0-3** | 3 | v1 2.0, v2 2.7 (error 90), v3 2.8 |
| 27 | WriteTxnMarkers | 0 | **0-1** | 1 | v1 2.8 (flexible); broker-to-broker, class and vectors only |
| 28 | TxnOffsetCommit | 0 | **0-3** | 3 | v1 2.0, v2 2.1 (committed leader epoch), v3 2.5 (KIP-447 member id, group instance id, generation) |
| 29-31 | DescribeAcls, CreateAcls, DeleteAcls | 0 | 0-2 | 2 | v1 2.0 (KIP-290 resource pattern type), v2 2.5 — **out** unless the optional T11 lands (needs an authorizer) |
| 32 | DescribeConfigs | 0-1 | **0-4** | 4 | v2 2.0, v3 2.6 (KIP-226 documentation, KIP-569 config type), v4 2.8 |
| 33 | AlterConfigs | 0 | **0-2** | 2 | v1 2.0, v2 2.8 |
| 34 | AlterReplicaLogDirs | 0 | **0-2** | 2 | v1 2.0, v2 2.8 |
| 35 | DescribeLogDirs | 0 | **0-2** | 2 | v1 2.0, v2 2.6 |
| 36 | SaslAuthenticate | 0 | **0-2** | 2 | v1 2.2 (KIP-368 session lifetime), v2 2.5 |
| 37 | CreatePartitions | 0 | **0-3** | 2 | v1 2.0, v2 2.5, v3 2.7 (KIP-599, error 89) |
| 38-41 | the four delegation token apis | 0 | **0-2** | 2 | v1 2.0; v2 2.4 for CreateDelegationToken (38), 2.5 for Renew/Expire/DescribeDelegationToken (39-41) — verified by T1 against the tags |
| 42 | DeleteGroups | 0 | **0-2** | 2 | v1 2.0, v2 2.4 |
| 43 | ElectLeaders | – | **0-2** | 2 | v0 2.2 (ElectPreferredLeaders, KIP-183), v1 2.4 (KIP-460 election type), v2 2.4 |
| 44 | IncrementalAlterConfigs | – | **0-1** | 1 | v0 2.3 (KIP-339), v1 2.4 |
| 45 | AlterPartitionReassignments | – | **0** | 0 | 2.4 (KIP-455) |
| 46 | ListPartitionReassignments | – | **0** | 0 | 2.4 (KIP-455) |
| 47 | OffsetDelete | – | **0** | never | 2.4 (KIP-496) — the one new api without a flexible version |
| 48 | DescribeClientQuotas | – | **0-1** | 1 | v0 2.6 (KIP-546), v1 2.8 |
| 49 | AlterClientQuotas | – | **0-1** | 1 | v0 2.6 (KIP-546), v1 2.8 |
| 50 | DescribeUserScramCredentials | – | **0** | 0 | 2.7 (KIP-554) |
| 51 | AlterUserScramCredentials | – | **0** | 0 | 2.7 (KIP-554) |
| 56 | AlterIsr | – | 0 | 0 | 2.7 (KIP-497), broker-to-controller, probed only |
| 57 | UpdateFeatures | – | **0** | 0 | 2.7 (KIP-584) |
| 60 | DescribeCluster | – | **0** | 0 | 2.8 (KIP-700) |
| 61 | DescribeProducers | – | **0** | 0 | 2.8 (KIP-664) |

The attribution column was derived from the JSON specifications at every tag (the 2.0.1 and 2.1.1 trees predate the
JSON for most apis; their `schemaVersions()` arrays were counted instead) and is a guide, not a source: **every
ticket verifies its rows against the tags and the container before writing them down**, and the document says
"Kafka 2.4" only where a ticket checked it.

### What is deliberately not in this line

* **The KRaft mode and its apis** (52-55, 58, 59, 62-64): the container is ZooKeeper-backed, as every Kafka 2.x
  production cluster was, and a ZooKeeper-backed broker does not serve them.
* **The replication apis** LeaderAndIsr (4), StopReplica (5), UpdateMetadata (6) and AlterIsr (56): only a
  controller sends them; the probe still sends one frame of each to see the answer (11 or 77) and the version above
  the table close the connection.
* **The ACL apis 29-31**: they do nothing without an `authorizer.class.name` and every vector comes from a real
  broker. T11 (optional) adds a second, authorizer-enabled container for them; it never runs on the shared broker.
* **SASL/SCRAM, SASL/OAUTHBEARER, GSSAPI** (decision 4): the login stays PLAIN; a delegation token can be issued,
  renewed, expired and described but not used, and a SCRAM credential can be created and described through 50/51
  but not logged in with.
* **A pure-PHP zstd codec**: `ext-zstd` when loaded, `UnsupportedCompressionTypeException` otherwise.
* **Cooperative rebalancing** (KIP-429) and the consumer protocol v1/v2 subscription (owned partitions, generation)
  of KIP-429/KIP-792: the embedded consumer protocol changes are Kafka 2.4+, but they are a client-library feature
  on top of the wire, not a broker contract; the `ConsumerProtocolSubscription`/`Assignment` JSON of 2.8.2 stays
  documented as the ceiling. The assignors of the 1.x line (range, round-robin) stay.
* **KIP-500 broker-side features** (dynamic quorum, `UpdateFeatures` beyond describing what the container answers).

## Environment recipe

* **The broker image**: `docker/kafka-2.8.2/` (`eclipse-temurin:11-jre`, `kafka_2.13-2.8.2.tgz` from
  archive.apache.org, the four listeners PLAINTEXT 9092 / SSL 9093 / SASL_PLAINTEXT 9094 / SASL_SSL 9095, the SSL
  material and the `jaas.conf` of the 1.1.1 image, two log directories, `delegation.token.secret.key` (the 2.x name;
  `delegation.token.master.key` is a deprecated alias), `transaction.state.log.*=1`, `inter.broker.protocol.version`
  and `log.message.format.version` `2.8-IV1`), the container **`kafka-2-8-2`** of `docker-compose.yml`. The temurin
  image ships `curl` and `openssl`, so the Dockerfile runs no `apt-get` (the sandbox proxy stalled the package lists
  for good). Behind the sandbox proxy: `cp /root/.ccr/ca-bundle.crt docker/kafka-2.8.2/ca/proxy-ca.crt` before the
  build. `docker compose down -v` and `docker compose up -d --wait` as two commands, between the waves.
* **Every stale container name was repointed in the foundation** (`kafka-1-1-1`, `docker/kafka-1.1.1`) — grep for
  them again before the final gate; a stale name makes a test skip.
* **Sources**: `git clone --depth 1 --branch 2.8.2 https://github.com/apache/kafka <scratch>/kafka-src-2.8.2`, then
  `git fetch --depth 1 origin tag 2.x.y` for the eight other tags; `git show 2.4.1:<path>` attributes a version.
* **`tools/dev/vendor-from-source.sh`** once per session, `cp -a vendor` into every worktree; phpunit always with
  `-d opcache.jit=0`; `tools/dev/gate.sh <worktree>` is the whole gate.
* `nohup dockerd >/tmp/dockerd.log 2>&1 &` if `docker info` fails; find it with `ps -C dockerd -o pid=` (a grep for
  the name matches the shell that runs it) — the daemon died three times in the 1.x session.

## Ticket plan: one milestone per Kafka minor, chronologically

**The owner's rule for this line: build it step by step, 2.0, 2.1, 2.2 … 2.8, so that every minor is a commit of
the 2.x branch that can be tagged.** The integration branch is therefore a chronological sequence of nine
milestones. Each minor is a wave: the four surface agents (T1 protocol/table, T2 producer/consumer apis, T3 group
apis, T4 admin/transaction/SASL apis) each deliver **one PR per minor** limited to what that minor added on their
surface (the same agent keeps its ticket and its context across the minors; the tickets #113-#116 stay open until
2.8 is done), the coordinator merges the PRs of the minor with merge commits, runs the whole gate against the 2.8.2
broker, and closes the minor with a **milestone commit** `chore(2.x): Kafka 2.N complete` that reconciles the
CHANGELOG (`### Kafka 2.N` subsection), the "What 2.x adds" section of the document, the api-key table's "implemented"
column and the tag table below. **That milestone commit is the commit to tag** (the tag names follow the last
release of the minor, as every line of this repository speaks the last release of its version: `2.0.1`, `2.1.1`,
`2.2.2`, `2.3.1`, `2.4.1`, `2.5.1`, `2.6.3`, `2.7.2`, `2.8.2`). The tags are created by the owner on `main` after
the final PR is merged (with a merge commit, so that the milestone commits are in `main`'s history).

Two things are deliberately **not** chronological: the **foundation** (`443c067`) declares the api keys 43-64 and
the error codes 72-104 of 2.8.2 up front, so that the constant ranges are frozen for every wave (a tag 2.0.1
therefore carries the constants of the whole major and implements the wire of 2.0 — the document says so), and the
**broker** is 2.8.2 at every milestone (it serves every version 2.0 to 2.7 added; the api-key table is its answer
with an "implemented on this line" column that grows with the milestones).

| Milestone (tag) | T1 — protocol, table, engine | T2 — Produce, Fetch, ListOffsets, Metadata, OffsetForLeaderEpoch, records | T3 — the group apis, the consumer's membership | T4 — admin, transactions, SASL, control |
|---|---|---|---|---|
| **2.0** (`2.0.1`) | ApiVersions v2; the 56-key table and the probe; "What is not in 2.8.2"; the document preamble | Produce v6, Fetch v8, ListOffsets v3, Metadata v6 (KIP-219 bumps); OffsetForLeaderEpoch v1 (KIP-279); **the KIP-219 wait** in `Client`; **KIP-283** (43) and the re-measured down-conversion section | OffsetCommit v4, OffsetFetch v4, FindCoordinator v2, JoinGroup v3, Heartbeat v2, LeaveGroup v2, SyncGroup v2, DescribeGroups v2, ListGroups v2, DeleteGroups v1 | CreateTopics v3, DeleteTopics v2, DeleteRecords v1, DescribeConfigs v2, AlterConfigs v1, AlterReplicaLogDirs v1, DescribeLogDirs v1, CreatePartitions v1, the token apis v1, InitProducerId v1, AddPartitionsToTxn/AddOffsetsToTxn/EndTxn v1, TxnOffsetCommit v1 |
| **2.1** (`2.1.1`) | – (builds the 2.4 engine in the background) | Fetch v9 (KIP-320 `current_leader_epoch`, 74/75) and v10 (zstd), ListOffsets v4, Metadata v7 (`leader_epoch`), OffsetForLeaderEpoch v2; **the zstd codec** (KIP-110, 76); **KIP-320 in the consumer** (epoch tracking, validation, truncation detection, `LogTruncationException`) | OffsetCommit v5 (retention time removed) and v6 (`committed_leader_epoch`), OffsetFetch v5 | DeleteTopics v3 (73), TxnOffsetCommit v2 (`committed_leader_epoch`) |
| **2.2** (`2.2.2`) | – | ListOffsets v5 (KIP-207, 78) | JoinGroup v4 (KIP-394, the 79 rejoin in the consumer) | SaslAuthenticate v1 (KIP-368 session lifetime), ControlledShutdown v2 (KIP-380, 77), **ElectLeaders (43) v0** (KIP-183, 80), `group.max.size` 81 documented |
| **2.3** (`2.3.1`) | – | Fetch v11 (KIP-392 rack id / preferred read replica), Metadata v8 (KIP-430 authorized operations), OffsetForLeaderEpoch v3 (replica id) | JoinGroup v5, SyncGroup v3, Heartbeat v3, OffsetCommit v7 (KIP-345 **static membership**, 82), DescribeGroups v3 (KIP-430) | **IncrementalAlterConfigs (44) v0** (KIP-339) |
| **2.4** (`2.4.1`) — T1 first, then the others | **The flexible-version engine** (KIP-482: compact types, unsigned varints, tagged fields, header v2/v1, the contract) and ApiVersions v3 (KIP-511); then, on a stacked PR, **AlterPartitionReassignments (45)** and **ListPartitionReassignments (46)** (KIP-455, 85), InitProducerId v2 and CreateDelegationToken v2 (flexible), and **OffsetDelete (47)** (KIP-496, 86) with `deleteConsumerGroupOffsets()` | Produce v8 (KIP-467, 87), Metadata v9 (flexible) | OffsetCommit v8, OffsetFetch v6, FindCoordinator v3, JoinGroup v6, Heartbeat v4, LeaveGroup v3 (KIP-345 batch) and v4, SyncGroup v4, DescribeGroups v4 and v5, ListGroups v3, DeleteGroups v2 (flexible); `removeMembersFromConsumerGroup()` | CreateTopics v4 (KIP-464) and v5 (flexible, KIP-525 configs), DeleteTopics v4, ElectLeaders v1 (KIP-460, 83/84) and v2, IncrementalAlterConfigs v1, ControlledShutdown v3 |
| **2.5** (`2.5.1`) | – | – | JoinGroup v7, SyncGroup v5 (KIP-559), OffsetFetch v7 (KIP-447 `require_stable`, 88; the consumer's `read_committed` fetch of offsets) | InitProducerId v3 (**KIP-360** epoch bump in the producer), TxnOffsetCommit v3 (**KIP-447** group metadata; `sendOffsetsToTransaction()` with `ConsumerGroupMetadata`), CreatePartitions v2, SaslAuthenticate v2, Renew/Expire/DescribeDelegationToken v2 |
| **2.6** (`2.6.3`) | **DescribeClientQuotas (48)** and **AlterClientQuotas (49)** v0 and v1 (KIP-546; `TYPE_FLOAT64`) | DeleteRecords v2 | ListGroups v4 (KIP-518 states filter) | DescribeConfigs v3 (KIP-569 type, documentation), DescribeLogDirs v2 |
| **2.7** (`2.7.2`) | **DescribeUserScramCredentials (50)** and **AlterUserScramCredentials (51)** (KIP-554, 91-93), **UpdateFeatures (57)** (KIP-584, 95/96; `describeFeatures()` from ApiVersions v3) | Fetch v12 (flexible; `last_fetched_epoch`, the diverging-epoch tagged fields) | – | CreateTopics v6, DeleteTopics v5, CreatePartitions v3 (**KIP-599**, 89, the controller mutation quota), InitProducerId v4 and AddPartitionsToTxn/AddOffsetsToTxn/EndTxn v2 (KIP-588, **90 vs 47**), AlterIsr (56) probed |
| **2.8** (`2.8.2`) | **DescribeCluster (60)** (KIP-700), **DescribeProducers (61)** (KIP-664); the final consistency pass of the table and the probe | Produce v9 (flexible), ListOffsets v6, Metadata v10 (KIP-516 **topic ids**) and v11, OffsetForLeaderEpoch v4 | – | CreateTopics v7 (topic id), DeleteTopics v6 (by topic id, 100), DescribeConfigs v4, AlterConfigs v2, AlterReplicaLogDirs v2, AddPartitionsToTxn/AddOffsetsToTxn/EndTxn v3, WriteTxnMarkers v1 |
| **close** | T10 (docs, README matrix, CHANGELOG, examples, the release notes of the line, the handoff for 3.x) runs after 2.8; T11 (ACLs on an authorizer container) only if the owner asks | | | |

**Frozen across the line**: the public signatures of `Client`, `KafkaConsumer`, `KafkaProducer` and `AdminClient`
(additions only), the ranges of `ApiKeys` and `KafkaException` (foundation), the engine files (T1 only; nobody adds a
flexible version before T1's 2.4 engine is merged), the section headings another surface references (a heading that
names versions changes with its owner's minor, together with the `section` field of the vector file and every `@see`).

**Tag points** (filled in as the milestones land; the commit is the `chore(2.x): Kafka 2.N complete` milestone on the
integration branch, which is in `main`'s history after the final merge):

| Tag | Kafka | Milestone commit | Merged PRs |
|---|---|---|---|
| `2.0.1` | 2.0 | `2afcb2f` (`chore(2.x): Kafka 2.0 complete`) | #117 (T4), #118 (T3), #119 (T1), #122 (T2) |
| `2.1.1` | 2.1 | `72d1bb3` (`chore(2.x): Kafka 2.1 complete`) | #121 (T3), #120 (T4), #128 (T2) |
| `2.2.2` | 2.2 | _pending_ | |
| `2.3.1` | 2.3 | _pending_ | |
| `2.4.1` | 2.4 | _pending_ | |
| `2.5.1` | 2.5 | _pending_ | |
| `2.6.3` | 2.6 | _pending_ | |
| `2.7.2` | 2.7 | _pending_ | |
| `2.8.2` | 2.8 | _pending_ | |

Every PR: its own `t<n>-<slug>` branch off the integration branch (the same branch continues across the minors,
merged with the integration branch before each push), a PR against it titled `[2.x] T<n> (Kafka 2.N): …` with
`Part of #<ticket>`, commits `feat(2.N): …`, the report of `docs/handoff/AGENT_BRIEF.template.md`, its own doc
sections with the headings at the minor's range, its `### <vector id>` blocks at the end of "Wire vectors", and the
api-key table/README/CHANGELOG lines it wants written in its report (the coordinator writes them into the milestone).

## Pitfalls (the 1.x session's list, plus what the foundation found)

* **A stale container name makes tests skip, not fail.** Zero skips is the state of the integration suite, locally
  and on CI; a skip is a regression. The foundation repointed every name; check again before the final gate.
* **A 2.x broker closes the socket for a version above its table, for every api, and for a body it cannot parse.**
  Only ApiVersions answers an unknown version (with 35, in the v0 layout). A flexible request with a wrong compact
  length or a missing tagged-field count is dropped the same way — `docker logs kafka-2-8-2` says which frame.
* **`throttle_time_ms` is the LAST field of the four delegation-token answers (38-41)** and absent from
  SaslAuthenticate (36); every other api carries it first. The flexible versions of the token apis keep it last.
* **A version bump with an identical schema still needs its own class and its own vector.**
* **The broker is the authority on the error message, and the messages move between releases.** Never assert on a
  message this repository has not measured on its own container; expect the 1.x messages to have moved again.
* **`if (version < N) OLD_ERROR else NEW_ERROR` is a pattern of this line**: 47 becomes 90 for the transaction apis
  at v2 (or v4 for InitProducerId), 6 becomes 56 (1.x), 3 becomes 100 for topic ids. Read `KafkaApis.scala` for the
  api before asserting a code.
* **A shared container is shared.** Unique topic, group and transactional-id prefixes per test class; never restart
  or recreate the broker from a subagent; never delete a topic the test did not create; a DescribeLogDirs with a
  `null` topic array answers every replica of everybody's tests.
* **Timing-sensitive tests must not depend on the disk** (`LogDirsApiTest` throttles the replica mover); **timestamps
  come from the clock**, never from a fixed date.
* **Merge seams between two tickets' vector dumps can lose a closing code fence** — run the compliance suite after
  every merge of the integration branch, not only before the PR.
* **The tracing JIT of the sandbox's PHP 8.5 CLI miscompiles hot pure-PHP byte loops** (lz4, CRC-32C, varints — and
  now the unsigned varints of the compact types). Always `php -d opcache.jit=0 vendor/bin/phpunit`.
* **Agents subscribe to their own PR despite the brief.** Say it twice, and unsubscribe after reading a PR.
* **`pkill -f <pattern>` and a grep for `dockerd` kill or match the shell that runs them.** Run `docker compose down`
  and `up` as separate commands; stop a background build by its pid.
* **`offsets.retention.minutes` is 10080 on 2.x** (KIP-186): a test that relies on an expired offset sets the option
  down, and the `Empty`-group cleanup of the 1.x line behaves differently on the default configuration.
* **`--new-consumer` is gone from the console tools** (KIP-176): it is an error on this image, not a no-op;
  `kafka-consumer-groups.sh` needs `--bootstrap-server`.

## Decisions taken at the start of the line (the owner's)

1. **The line speaks Kafka 2.8.2 and covers every 2.x minor** (above). Skipping nothing, one broker.
2. **KIP-219: the client waits.** Sending Produce v6 / Fetch v8 / the bumped versions promises to honour
   `throttle_time_ms`; `Client` sleeps the throttle time before its next request to that broker, as the Java client
   does, with an option to switch it off. Measured against a client quota in T2. The only decision of the line that
   changes runtime behaviour rather than a version number.
3. **ACLs only on a second container and only if there is capacity after wave 2** (T11), never on the shared broker.
4. **SASL/OAUTHBEARER out, SCRAM login out**: the delegation-token limitation of 1.x stays.
5. **The 0.8/0.9/0.10/0.11/1.1 protocol vectors stay on `main`**: the compliance suite replays all 318 against this
   line's classes, and a 2.8.2 broker still speaks every one of them.

## Open questions for the owner

1. **The compression codec for zstd** needs `ext-zstd`; should `composer.json` suggest it (like `ext-openssl`)?
   The plan says yes, as a `suggest`.
2. **Topic ids** (KIP-516, 2.8): Metadata v10 answers a topic id per topic, DeleteTopics v6 accepts one. Should
   `Cluster`/`TopicMetadata` expose them as a `Uuid` value object (the Java name)? The plan says yes, in T5/T7.
3. **The consumer's `client.rack`** (KIP-392, Fetch v11): a one-broker container cannot observe a preferred read
   replica; the wire is implemented and the behaviour documented from the sources. Acceptable?
