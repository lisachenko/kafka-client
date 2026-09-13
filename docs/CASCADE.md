# Cascade merges between protocol lines

The client is developed one Kafka protocol version at a time, lowest first, and each finished line
is merged upwards into the next one:

    0.8.x (Kafka 0.8.2.2) → 0.9.x (Kafka 0.9.0.1) → 0.10.x (Kafka 0.10.2.2) → 0.11.x (Kafka 0.11.0.3) → 1.x (Kafka 1.1.1) → main (the 2.x line, Kafka 2.8.2, in development)

From the 1.x line on, a line is a Kafka **major** version and covers every minor release inside it: `1.x` speaks
1.1.1 (everything 1.0 and 1.1 added), `main` is the **2.x line** and speaks 2.8.2 (everything 2.0 to 2.8 added);
its handoff is [`docs/handoff/main.md`](handoff/main.md).

## Rules

1. **Direction.** Changes flow upwards only. A fix needed on a lower line is made there and cascaded;
   nothing is cherry-picked downwards.
2. **Trigger.** Every push to `0.8.x`, `0.9.x`, `0.10.x`, `0.11.x` or `1.x` makes `.github/workflows/cascade.yml`
   open (or keep) a pull request into the next line. A push that the next line already contains
   opens nothing. When a finished line is branched off `main` under its own numeric name, that branch
   has to be added to the workflow's trigger list and to the chain above, as `0.11.x` and `1.x` were.
3. **Conflict resolution** happens on a branch named `cascade/<from>-into-<to>` created from `<to>`,
   with `git merge origin/<from>`; the PR is opened from that branch and the automatic PR is closed.
   Never rebase, amend or force-push a protocol branch.
4. **Who wins.** For a structure that exists on both lines, the lower line wins the shared part
   (engine, framing, DTO and client code, tests, tooling), and the higher line's version-specific
   additions are re-applied on top *as fields and constants*: extra scheme fields guarded by
   `static::VERSION`, higher `VERSION` constants, added api keys and error codes, new request classes.
   Wire formats of the higher line are never lowered; identifiers come from `main`.
5. **Superseded code.** Higher-line code written against a design the lower line replaced (e.g. the
   pre-schema `pack()`/`unpack()` classes) is dropped in the merge and re-implemented on the lower
   line's design, starting from `main`'s classes. Record what was dropped in the merge commit and
   in `docs/handoff/<to>.md`.
6. **Gate.** The cascade PR must pass the full gate of the target line (cs, phpstan, unit,
   compliance, integration against the target line's broker) before it is merged with a merge commit.
7. **Docs.** After the merge, `docs/protocol/<version>.md`, `README.md`, `CHANGELOG.md`, the Docker
   broker and `docker-compose.yml` are updated to the target line's Kafka version as the first ticket
   of that line. The document is *renamed* with the line and every `@see docs/protocol/<version>.md,
   section "…"` moves with it — `Compliance\DocumentationSyncTest` checks both the file name and the
   section headings, so a rename that misses a reference fails the gate.

## History

| From  | Into  | Result |
|-------|-------|--------|
| 0.8.x | 0.9.x | merged tree identical to 0.8.x; the pre-schema 0.9 deltas (group membership apis, Produce/Fetch v1 throttle time, Metadata v1, OffsetCommit v1 fields, first AdminClient) are re-implemented as the 0.9.x tickets — see `docs/handoff/0.9.x.md` |
| 0.9.x | 0.10.x | merged tree identical to 0.9.x (plus the `ext-openssl` suggestion of `composer.json`); the pre-schema 0.10 deltas (commits `5b6dd06..77993bc`: Metadata v1/v2, Fetch v2/v3, Offsets v1, JoinGroup v1, OffsetFetch v2, ApiVersions, SaslHandshake, message format with timestamps, four error classes) are dropped by rule 5 and re-implemented as the 0.10.x tickets — see `docs/handoff/0.10.x.md` |
| 0.10.x | main | merged tree identical to 0.10.x plus the api-key constants 21–33 of `ApiKeys` (Kafka 0.11); the pre-schema 0.11 code of `main` (the 14 requests with `$header = null`, `Common\Record\{RecordBatch,Header}`, `Common\Utils\ByteUtils`, `Data\FetchResponseAbortedTransaction`, the `KafkaException` that stopped at 35, its six tests) is dropped by rule 5 and re-implemented on the 0.10 design as the tickets of the 0.11 line — see `docs/handoff/0.11.x.md`; the automatic PR #48 was closed in favour of `cascade/0.10.x-into-main`. **The line is complete**: it speaks Kafka 0.11.0.3 (epic #70, tickets #71-#78, PR #87), and was branched off `main` as **`0.11.x`** at that commit (`64d767c`) so that `main` can continue as the 1.x line — `docs/handoff/0.11.x.md` carries the release notes of the line with the original plan below them |
| 0.11.x | main | **done** (PR #89, merge `14daf6a`) — `main` and `0.11.x` were identical at the branch point (`64d767c`), so the merge carried the configuration of the `0.11.x` line and the handoff of the 1.x line (`docs/handoff/1.x.md`) and nothing else. `main` is now the **1.x line** and speaks Kafka **1.1.1** (epic #90): the broker `docker/kafka-1.1.1`, the document `docs/protocol/2.8.md`, the api keys 34–42 and the error codes 56–71 came with its first ticket. Later fixes on `0.11.x` keep arriving as cascade PRs and are merged into the integration branch of the line before its final PR. **The line is complete**: it speaks Kafka 1.1.1 (epic #90, tickets #91-#99), and `docs/handoff/1.x.md` carries the release notes of the line with the original plan below them. The next line is Kafka **2.0.1** — what it adds api by api, its ticket plan and the pitfalls of the 1.x session are in [`docs/handoff/main.md`](handoff/main.md); `main` is branched off as **`1.x`** at the completing commit, as `0.11.x` was |
| 1.x | main | **done** (PR #111) — `1.x` was branched off `main` at `2ee4866`, the merge of PR #109 that completed the 1.x line (epic #90), so the two were identical at the branch point; PR #110 wired `1.x` into the cascade and CI workflows and PR #111 cascaded it. `main` is now the **2.x line** and speaks Kafka **2.8.2** — one line for the whole 2.x major, 2.0 to 2.8: the broker `docker/kafka-2.8.2`, the document `docs/protocol/2.8.md`, the api keys 43–64 and the error codes 72–104 came with its foundation. Every fix that lands on `1.x` keeps arriving through a cascade PR and is merged into the integration branch of the line before its final PR (`docs/handoff/main.md`) |
