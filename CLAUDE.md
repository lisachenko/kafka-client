# Working on lisachenko/kafka-client

Pure-PHP Apache Kafka client. Each Kafka protocol line lives on its own branch and is developed
lowest-first, then cascade-merged upwards: `0.8.x` (Kafka 0.8.2.2, **complete**) → `0.9.x`
(Kafka 0.9.0.1, **complete**) → `0.10.x` (Kafka 0.10.2.2, **complete**) → `main`
(Kafka 0.11.0.3, **complete**). The cascade ends at `main`: there is no line above it, so
`docs/handoff/main.md` carries the **release notes** of the 0.11 line with the plan it was built from
below them. See `docs/CASCADE.md` and, for a line, `docs/handoff/<branch>.md`.

## Hard rules (owner's decisions)

1. **Wire structures are strictly those of the branch's Kafka version** (fields, api versions,
   error codes, message format). Never carry a later field/version into a lower branch.
2. **Identifiers follow `main`** wherever the concept exists there: class, constant, field and
   method names (`GroupCoordinatorRequest`, `MessageTooLargeException`, `baseOffset`,
   `highWaterMarkOffset`, `ProduceRequestTopic`…). Historical names go in docblocks.
3. **Every protocol message is a `getScheme()` declaration** on `Protocol\Kafka\Protocol\BinarySchema`
   (`BinarySchemaInterface`). No hand-written `pack()`/`unpack()` in request/response/DTO classes.
   Requests declare `parent::getScheme() + [...]` — never `$header = null;` (a defect on `main`).
4. **Start from `main`'s implementation** of a class (`git show origin/main:<path>`) and strip what the
   branch's Kafka version lacks, instead of writing it anew. Backport version-independent pieces
   verbatim.
5. **The broker is the final authority.** Verify against a real broker of the branch's version
   (Docker, see below); spec sources in order: broker behaviour, Kafka sources at the release tag,
   the protocol wiki. Record broker quirks in `docs/protocol/<version>.md`.
6. Tests are spec tests: byte-exact hex vectors (also replayed by `tests/Compliance`) plus
   integration tests against the real broker, skipped when `KAFKA_BOOTSTRAP_SERVERS` is unset.

The `main` of the rules 2, 3 and 4 is the **pre-schema `main`**, i.e. the branch as it stood before the
cascade merge of `0.10.x` (`git show 94f896a:<path>`): its identifiers are the ones this package publishes,
and its `$header = null` requests were the defect the schema engine replaced. Since the 0.11 line landed,
`main` itself is the finished, schema-based implementation of Kafka 0.11.0.3.

## Toolchain and quality gate

- PHP 8.4 is the target (`composer.json`, CI). The sandbox may have a newer CLI — write 8.4 code.
- Gate before every push: `vendor/bin/php-cs-fixer check`, `php vendor/bin/phpstan analyse --memory-limit=512M`,
  `vendor/bin/phpunit` (unit + compliance), and
  `KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 vendor/bin/phpunit --testsuite integration` (whole suite).
  `composer check` runs the first three. `find src tests examples -name '*.php' -print0 | xargs -0 -n1 php -l`.
- phpstan runs as a phar at `vendor/bin/phpstan`; call it with `php vendor/bin/phpstan ...`.
- **In the sandbox, run phpunit as `php -d opcache.jit=0 vendor/bin/phpunit`.** Its PHP 8.5 CLI enables the tracing
  JIT by default and the JIT miscompiles the pure-PHP LZ4 decoder once its functions get hot, which shows up as an
  order-dependent `CorruptMessageException` in `Lz4Test` and in the lz4 message-format vectors (0 of 200 round trips
  fail with the JIT off, most of them fail with it on). CI runs PHP 8.4 without opcache in the CLI and is
  unaffected, so this is a sandbox workaround, not a code defect. Suspect the same for any other hot pure-PHP byte
  loop — the CRC-32C and the varints of the record batch v2 are the candidates — and measure with the JIT off
  before believing that a failure is a defect of the code.

### Installing dependencies in a sandbox that blocks GitHub downloads (Claude Code remote sessions)

`composer install` fails there with "Could not authenticate against github.com" because
api.github.com / codeload.github.com are blocked by the egress proxy, while git clones of public
repositories work. Do **not** try to point Composer at third-party mirrors (blocked by policy) or
change global Composer/git configuration. Instead run once per session:

    tools/dev/vendor-from-source.sh          # ~10 min: installs from git sources, adds phpstan.phar

and copy the resulting `vendor/` into every other worktree with `cp -a` (the autoloader is
relative). The script strips the packages' `.git` directories — a vendor tree with them is ~2.4 GB
and several worktrees fill the disk. `composer.lock` already lists everything; never run
`composer update` in that sandbox.

### Kafka broker for integration tests

- `docker compose up -d --wait` starts the broker of this branch's Kafka version
  (`docker/kafka-<version>/`, ZooKeeper bundled, advertised as 127.0.0.1:9092). On `main` that is
  **`docker/kafka-0.11.0.3`**, the container `kafka-0-11-0-3`, with the four listeners PLAINTEXT 9092,
  SSL 9093, SASL_PLAINTEXT 9094 and SASL_SSL 9095, and it is the only broker image this branch carries.
- In the remote sandbox the Docker daemon may not be running: `nohup dockerd >/tmp/dockerd.log 2>&1 &`
  and wait for `docker info` to answer. Old Docker Hub images with v1 manifests cannot be pulled;
  build the image from `docker/` (the Kafka tarball comes from archive.apache.org, which is reachable).
  Behind the sandbox proxy the build's `curl` needs the proxy CA: drop it into `docker/kafka-<version>/ca/`,
  which the Dockerfile copies to `/usr/local/share/ca-certificates/extra/` before `update-ca-certificates`.
- A 0.8/0.9 broker answers Metadata with **zero brokers** until a topic exists; use the readiness probe
  in `tests/Integration/IntegrationTestCase.php`. Fresh topics transiently answer 5/6 — helpers must retry.
- Several agents share one broker: unique topic/group/transactional-id names per test class; never restart it from
  a subagent.
- **0.11 specifics.** The Apache repository has **no `0.11.0.3` tag** — `0.11.0.3-rc0` is the commit the release was
  built from and is what "@ 0.11.0.3" means in this repository. The `server.properties` shipped with 0.11 ends
  **without a trailing newline**, so `start.sh` appends one before it writes the settings of this repository into
  the file. A one-broker cluster additionally needs `transaction.state.log.replication.factor=1` and
  `transaction.state.log.min.isr=1`, or `__transaction_state` cannot be created and every transactional request ends
  in the error code 15.
- Useful in-container tools: `docker exec <container> /opt/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --list`,
  `kafka-console-producer.sh`/`kafka-console-consumer.sh`, `kafka-run-class.sh kafka.tools.DumpLogSegments`.
  From 0.9 on, the console consumer joins a *group* only with `--new-consumer --bootstrap-server host:port` and
  a `--consumer.config <file>` carrying `group.id` (and, if it matters, `partition.assignment.strategy`).

## Branching and delivery

- Feature branches: `t<n>-<slug>` off the protocol branch (a nested `0.9.x/<slug>` ref cannot coexist
  with the branch `0.9.x`). PRs target the protocol branch, are merged with a merge commit, and close
  their ticket with "Closes #n" in the body (the auto-close only works for `main`, so close the issue
  by hand after merging).
- Conventional commits. No force-pushes on shared branches.
- Cascade: after a line is complete, merge it upwards on a `cascade/<from>-into-<to>` branch (rules in
  `docs/CASCADE.md`); `.github/workflows/cascade.yml` opens the PR automatically on pushes.

## How the work is organised (multi-agent)

The coordinator plans one Kafka version at a time as GitHub issues (an epic + tickets in waves),
launches up to four Opus subagents in isolated git worktrees, reviews and merges their PRs. Agents
read a brief (`docs/handoff/AGENT_BRIEF.template.md`, filled with the session's paths) and their
ticket. File ownership per ticket avoids conflicts; shared contracts (e.g. `Client`'s public method
signatures) are frozen for a wave. Every PR is gated locally by the coordinator (cs, phpstan, unit,
compliance, integration against the broker) and on CI before merging.
