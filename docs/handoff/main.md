# The 4.x line: Kafka 4.x on a KRaft node — the plan

**State: not started.** `main` is the 4.x line and, until the foundation ticket of the line lands, speaks exactly
what the 3.x line delivered — Kafka **3.9.2** and the KIP-848 consumer protocol, on the 3.9.2 KRaft node — because
`main` and `3.x` were identical when the owner branched `3.x` off at the end of the 3.x line. The line is built on
`main` through an integration branch, as the 2.x and 3.x lines were, and merged into `main` as one pull request when
it is complete; its release notes are then written above this plan, so that the file becomes the record of the line,
as [`docs/handoff/3.x.md`](3.x.md) is for the 3.x line and [`docs/handoff/2.x.md`](2.x.md) for the 2.x line.

This plan was written at the end of the 3.x line by its coordinator. **Everything it says about Kafka 4.x is a
starting hypothesis to be verified at the release tags and against the node** — the 3.x plan was corrected in
several places that way (EndTxn did not grow in 3.9; Kafka 3.4 added nothing a client sends; the codes 121–127 have
no frame that produces them on a static quorum), and the pitfalls list of the 3.x record says why: the broker is the
authority, the tags are the attribution authority, memory is neither.

## What is known about Kafka 4.x, to verify first

* **KRaft only.** Kafka 4.0 removed ZooKeeper; the node of the line is a KRaft combined node like `docker/kafka-3.9.2`
  (`process.roles=broker,controller`), formatted once at the first start. The broker needs Java 17 (the 3.9.2 image
  already uses `eclipse-temurin:17-jre`).
* **The last 4.x release available when the line starts is the broker of the line**, as 3.9.2 was for 3.x and 2.8.2
  for 2.x: read `git ls-remote --tags https://github.com/apache/kafka` first, pick the last patch release of the
  highest 4.x minor, and take its tarball from `https://archive.apache.org/dist/kafka/<version>/kafka_2.13-<version>.tgz`.
  One node serves every version the earlier 4.x minors added, so one container verifies the whole line.
* **KIP-896 (Kafka 4.0) removed old protocol versions from the broker.** A 4.x node no longer serves the lowest
  versions of many apis (the ones a pre-2.1 client sent); the ApiVersions answer reports a `min_version` above 0
  for those keys. For this package that means: the wire vectors of the lines below still replay in the compliance
  suite (a replay needs no broker) and their classes stay (rule 2: a published identifier is never removed), but
  every integration test that sends a removed version, and `ApiVersionProbeTest`'s "one frame at the minimum
  version of every key", moves to what the node serves. The exact minimum per key is read off the `validVersions`
  of every `*Request.json` at the tag — never off memory — and written into the api-key table.
* **The KIP-848 consumer protocol is GA in 4.0**; the classic protocol stays. Which `group.protocol` the Java
  consumer defaults to at the release of the line is a fact to verify; this package keeps `classic` as its default
  unless the owner decides otherwise.
* **KIP-890 part 2 (transactions v2) is on by default from 4.0** (`transaction.version` finalized above 0 on a
  fresh 4.x cluster): the node then answers the 120 of an abortable transaction the way the 3.8 wave declared but
  could not observe on 3.9.2, `AddPartitionsToTxn` is no longer sent by a v2 client (the broker adds the partitions
  on Produce), and EndTxn v5 (Kafka 4.0) carries the new semantics — the whole surface of the 3.x T4 ticket is
  re-measured on a node that finally has the feature.
* **KIP-853 (reconfigurable quorum) is GA in 4.0**: a cluster formatted with `kraft.version` 1 (`kafka-storage.sh
  format --standalone` or `--initial-controllers`) accepts AddRaftVoter (80), RemoveRaftVoter (81) and
  UpdateRaftVoter (82), and the codes 125–127 become reachable; DescribeQuorum v2 then reports real directory ids.
  Whether 80–82 get admin methods is an owner's decision (the 3.x line probed them only).
* **KIP-932 share groups**: early access in 4.0, preview in 4.1; the keys 76–79 and 83–87 the 3.x line left out. In
  or out is an owner's decision — check whether the release of the line still hides them behind
  `unstable.api.versions.enable` or serves them on a client listener, and whether the Java client of the release
  ships a `KafkaShareConsumer`.
* **KIP-1071 streams groups** (StreamsGroupHeartbeat, StreamsGroupDescribe and friends, Kafka 4.1 early access) are a
  Kafka Streams surface; probe only, as the controller apis were.
* **New api keys above 87 and new error codes above 127** exist in 4.0 and 4.1 (share groups, streams groups, the
  raft voter set, client re-bootstrap of KIP-1102, eligible leader replicas of KIP-966 in DescribeTopicPartitions);
  the exact tables are `ApiKeys.java` and `Errors.java` at the tag, read by the foundation ticket, which declares
  every key and every code with one exception class each as the 3.x foundation did for 65–87 and 105–127.
* **Version bumps a client sends**, to derive from the diff of every `*Request.json` between the 3.9.2 tag and the
  tag of the line: at least Fetch, Produce, Metadata, ListOffsets, OffsetCommit, OffsetFetch, EndTxn, InitProducerId,
  DescribeTopicPartitions, ConsumerGroupHeartbeat/Describe and ApiVersions moved in 4.0 or 4.1 — the plan of the
  minors below is filled in from that diff, one row per minor, as the 3.x plan's table was.

## Decisions to take at the start of the line (the owner's)

1. The Kafka release of the line (the last 4.x patch release at the time), KRaft combined node, four listeners,
   the `StandardAuthorizer` and the SASL users of the 3.x node kept.
2. Share groups (76–79, 83–87): in as a `KafkaShareConsumer`-like surface, wire only, or out.
3. The raft-voter apis 80–82: admin methods on a `kraft.version` 1 cluster, or probe only as in 3.x.
4. Transactions v2 (KIP-890 part 2): the client's transactional producer moves to the v2 protocol (no
   AddPartitionsToTxn, EndTxn v5), with the v1 path kept for a 3.x node, or stays on v1.
5. The default `group.protocol` of `KafkaConsumer` (recommendation: `classic` stays the default; `consumer` is the
   opt-in the 3.x line added).
6. What the removed versions of KIP-896 mean for the published api: classes and vectors stay, the client sends the
   highest version the node serves as before, and a version the node refuses is answered by the node's 35 (verify
   whether a 4.x node answers 35 or closes the connection for a version below its minimum).
7. Streams groups (KIP-1071) and client metrics beyond wire-only: out.
8. Tags: one per minor at the milestone commit `chore(4.x): Kafka 4.N complete`, named after the last patch release
   of the minor, created by the owner after the final merge into `main` — the tags of the repository are the
   record; the handoff files carry no tag or commit tables.

## Environment recipe (what the 3.x session ran on)

* The remote sandbox: `tools/dev/vendor-from-source.sh` once per session (Composer cannot reach GitHub there),
  `cp -a` of `vendor/` into every worktree, phpunit as `php -d opcache.jit=0 vendor/bin/phpunit`, the Docker daemon
  started by hand (`nohup dockerd >/tmp/dockerd.log 2>&1 &`), the image built from `docker/kafka-<version>/` with
  the tarball from archive.apache.org behind the proxy CA in `docker/kafka-<version>/ca/`.
* The node is shared by the coordinator's gate runs and up to four agents: unique topic, group and transactional-id
  prefixes per test class, every suite deletes the topics and groups it creates (a KIP-848 member leaves with the
  epoch -1 before its group is deleted), nobody restarts or recreates the node but the coordinator, and only between
  milestones with no agent on it (`docker compose down -v && docker compose up -d --wait`).
* The gate of every PR and of every milestone: `tools/dev/gate.sh <worktree> all` — php -l, php-cs-fixer, phpstan,
  unit + compliance, the whole integration suite on the four listeners with zero skips and zero failures — run by
  the coordinator in a detached worktree of the PR branch merged with the current head, before the merge.
* CI (GitHub Actions, PHP 8.4, its own node) runs the same suite on every push of the integration branch; a red
  there is diagnosed from the job log before anything else (the reds of the 3.x line were the replay lag of a slow
  runner, and the probes were hardened to wait for what the node has applied, not for what its metadata cache
  announces).

## How the work is organised (the 3.x process, to repeat)

* One epic for the line, four tickets by surface (T1 the engine, the api-key table, the error codes and the apis
  with a new key; T2 Produce, Fetch, ListOffsets, Metadata and the consumer's fetch path; T3 the group apis and the
  consumer's coordinator; T4 the admin, transaction, SASL, token and ACL apis) that carry the line minor by minor,
  and one ticket for the final documentation pass. The tickets of the 3.x line are closed with it; the 4.x line
  opens its own.
* **The foundation ticket first**, by the coordinator: the node image and `docker-compose.yml` (the 3.9.2 image is
  deleted), every fixture and example repointed, `docs/protocol/3.9.md` renamed to the version of the line with
  every `@see` reference and the api-key table rebuilt from the ApiVersions answer of the node
  (`DocumentationSyncTest` checks both), the new api keys and error codes with one exception class each,
  `ApiVersionProbeTest` at the new table, `ApiVersionsRequest::CLIENT_SOFTWARE_VERSION` at the line's version, the
  README and CHANGELOG heads, the baseline gate measured on the new node — and every failure of the inherited suite
  on it listed with an owner, as the re-baseline wave T0 of the 3.x line did.
* Then **one Kafka minor at a time**, 4.0 upwards: a wave of up to four Opus subagents in isolated worktrees, each
  with a brief (`docs/handoff/AGENT_BRIEF.template.md` filled with the session's paths, plus the per-agent scratchpad
  subdirectory and the one-line `@see` rule of the 3.x briefs), file ownership per ticket, shared files
  (`Client.php`, `AdminClient.php`) append-only. Every PR carries only its minor, merges the integration head before
  it opens, bumps the vector-count preambles on top of the head's numbers, and is gated by the coordinator before
  the merge with a merge commit; the ticket is commented, never closed, until the line ends.
* After the last PR of a minor: node recreated, whole gate, the milestone commit `chore(4.x): Kafka 4.N complete`
  (the CHANGELOG section of the minor folded from the PRs' reports, the README rows and the KIP table, the intro of
  the document and the vector counts verified against the JSON files), then the epic comment with the gate numbers.
* The last wave of the line is the one the owner names (for 3.x it was the KIP-848 consumer); then the final
  documentation pass (this file's release notes above the plan, the CHANGELOG head, the README, the document intro,
  the `DocumentationSyncTest` fixes found on the way), the pull request into `main`, the branch `4.x` and the tags by
  the owner, and a small close-out PR that points `main` at the next line.

## Pitfalls (the 3.x session's list — repeat them in every brief)

* **The broker is the authority, the tags are the attribution authority, memory is neither.** Every "Kafka 4.N
  added" claim is read off `git show <tag>:clients/src/main/resources/common/message/<Api>Request.json`.
* **A KRaft node publishes a leader before it serves it**: a Metadata answer names the leader of a fresh partition
  while the replica manager still answers 5, 6 or 9; `__consumer_offsets` answers 14, 15 and 16 while it loads;
  quotas and configs replay with a lag on a slow runner. The probes of `tests/Integration/IntegrationTestCase.php`
  and `tests/Fixture` wait for a serving leader, the group coordinator and a read-back — keep using them, and never
  assert right after a write on a KRaft node.
* **A KRaft node writes the empty string where the specification says null** (error messages, the
  `subscribed_topic_regex` of a KIP-848 member) and empty arrays where it could say null; a 3.x line's "-1 with the
  code 0" (the last tiered offset without remote storage) may change on a node with a feature the 3.9.2 one lacked.
* **The features gate more than the versions**: on 3.9.2 `transaction.version` did not exist and `kraft.version` was
  0, which made the 120 depend on the api version alone and the raft-voter apis refuse everything. A 4.x node
  formatted with its default features behaves differently — record the finalized features of the node
  (`AdminClient::describeFeatures()`) in the document before measuring anything else.
* **Run phpunit as `php -d opcache.jit=0 vendor/bin/phpunit`** in the sandbox (the JIT miscompiles the LZ4 decoder).
* **Never `pkill` php or phpunit**, never restart the node from an agent, never `git stash` (shared across
  worktrees), never force-push a shared branch, never `&` a whole `&&` chain (the coordinator once backgrounded a
  push and a gate start in one line).
* **Fill every placeholder of a merge message before merging**, verify the PR head after a gate (an agent may have
  pushed a docs-only amendment: diff the gated commit against the pushed one — docs only merge, code re-gate), and
  read `.github` job logs with `return_content=false` and a `curl` of the URL.
* **The scratchpad is shared**: every agent keeps its scripts and captures under a subdirectory named after itself;
  two agents of the 3.8 wave overwrote each other's files in the root.
* **A `@see … 3.9.md, sections "…"` reference may wrap over two docblock lines** — `DocumentationSyncTest` reads
  the continuation lines since the final pass of the 3.x line, so a renamed heading fails the gate wherever it is
  referenced; rename with `grep -rn` over `src tests examples`, not by memory.
