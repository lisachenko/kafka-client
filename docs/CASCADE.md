# Cascade merges between protocol lines

The client is developed one Kafka protocol version at a time, lowest first, and each finished line
is merged upwards into the next one:

    0.8.x (Kafka 0.8.2.2) → 0.9.x (Kafka 0.9.0.1) → 0.10.x (Kafka 0.10.x) → main (Kafka 0.11)

## Rules

1. **Direction.** Changes flow upwards only. A fix needed on a lower line is made there and cascaded;
   nothing is cherry-picked downwards.
2. **Trigger.** Every push to `0.8.x`, `0.9.x` or `0.10.x` makes `.github/workflows/cascade.yml`
   open (or keep) a pull request into the next line. A push that the next line already contains
   opens nothing.
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
   of that line.

## History

| From  | Into  | Result |
|-------|-------|--------|
| 0.8.x | 0.9.x | merged tree identical to 0.8.x; the pre-schema 0.9 deltas (group membership apis, Produce/Fetch v1 throttle time, Metadata v1, OffsetCommit v1 fields, first AdminClient) are re-implemented as the 0.9.x tickets — see `docs/handoff/0.9.x.md` |
| 0.9.x | 0.10.x | merged tree identical to 0.9.x (plus the `ext-openssl` suggestion of `composer.json`); the pre-schema 0.10 deltas (commits `5b6dd06..77993bc`: Metadata v1/v2, Fetch v2/v3, Offsets v1, JoinGroup v1, OffsetFetch v2, ApiVersions, SaslHandshake, message format with timestamps, four error classes) are dropped by rule 5 and re-implemented as the 0.10.x tickets — see `docs/handoff/0.10.x.md` |
