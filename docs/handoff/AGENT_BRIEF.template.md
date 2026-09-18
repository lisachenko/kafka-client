# Environment brief for kafka-client agents (template — fill the <…> parts per session)

## Repository / branches
- You are in an isolated git worktree of https://github.com/lisachenko/kafka-client on the wrong branch.
- Start: `git fetch origin <line> && git checkout -b t<n>-<slug> origin/<line>` (a nested `<line>/…` ref is impossible).
- Push: `git push -u origin t<n>-<slug>`; open the PR with the GitHub MCP tool against `<line>`, title `[<line>] T<n>: …`,
  body: what/why, spec references, how it was tested against the broker, `Closes #<issue>`, then the attribution
  lines the coordinator gives you. Commit trailers as given by the coordinator. Conventional commits, no force-push.

## PHP tooling (no GitHub archive downloads in this sandbox — do NOT run composer install/update/require)
- Target PHP 8.4 (CI); the local CLI may be newer — no newer-only syntax.
- Dependencies: `cp -a <SCRATCH>/vendor ./vendor` (prebuilt by `tools/dev/vendor-from-source.sh`; phpstan is `vendor/bin/phpstan`, a phar).
- Gate: `vendor/bin/php-cs-fixer check`, `php vendor/bin/phpstan analyse --memory-limit=512M`, `vendor/bin/phpunit`,
  `KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 vendor/bin/phpunit --testsuite integration` (whole suite), `php -l` on changed files.
- Do not change global git/composer configuration; do not stop/restart the shared broker.

## Kafka broker (shared by all agents)
- Container `<name>` = Kafka <version>, PLAINTEXT 127.0.0.1:9092, SSL 127.0.0.1:9093 (CA/cert:
  `docker/kafka-<version>/ssl/broker.crt`, `KAFKA_SSL_BOOTSTRAP_SERVERS` in the integration suite),
  SASL_PLAINTEXT 9094, SASL_SSL 9095 (PLAIN: kafkatest/kafkatest-secret, admin/admin-secret, and acltest/acltest-secret
  — the one principal that is not a super user of the authorizer), a KRaft node without ZooKeeper (the lines up to
  2.x had ZooKeeper on 2181), auto-create topics on, 3 partitions, `offsets.topic.num.partitions=5`,
  `group.min.session.timeout.ms=1000`, `group.max.session.timeout.ms=60000`.
- Unique topic/group prefix per test class (`t<n>-<what>-<random>`). Readiness: use `IntegrationTestCase` (probe topic);
  fresh topics answer 5/6 for a moment — helpers retry those codes only. Other agents' tests run at the same time:
  never assume the broker has no other topics or groups, never delete a topic you did not create, never restart it.
- In-container tools (every one of them takes `--bootstrap-server localhost:9092`; there is no `--zookeeper`):
  `docker exec <name> /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list`,
  `kafka-console-producer.sh --bootstrap-server localhost:9092 --topic X [--compression-codec gzip|snappy|lz4|zstd]`,
  `kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic X --from-beginning --consumer.config <file>`
  (a Java group member; the properties file carries `group.id`, `partition.assignment.strategy` and, for the
  KIP-848 protocol, `group.protocol=consumer`), `kafka-consumer-groups.sh --bootstrap-server localhost:9092
  --list|--describe --group G`, `kafka-configs.sh --bootstrap-server localhost:9092 ...` (quotas, broker and topic
  configs; `tests/Fixture/ClientQuota` sets client quotas through the quota apis instead), `kafka-acls.sh
  --bootstrap-server localhost:9092 --list`, `kafka-delegation-tokens.sh`, `kafka-metadata-quorum.sh
  --bootstrap-server localhost:9092 describe --status`, `kafka-run-class.sh kafka.tools.DumpLogSegments --files
  /tmp/kafka-logs/X-0/00000000000000000000.log --print-data-log` (a partition lands in `/tmp/kafka-logs` or
  `/tmp/kafka-logs-2`).
- `docker logs <name>` is the answer to "why did my request never come back": an api key or version the broker
  cannot parse shows up there (`Processor got uncaught exception`) and is dropped without a response.
- Building the image behind the sandbox proxy: drop the proxy CA bundle into `docker/kafka-<version>/ca/`, which the
  Dockerfile copies into `/usr/local/share/ca-certificates/extra/` before `update-ca-certificates` (see its README).

## Specification references (the broker is the final authority)
- Kafka sources at the release tag: `<SCRATCH>/kafka-src-<version>` (`core/src/main/scala/kafka/api`, `clients/src/main/java/org/apache/kafka/common/protocol`).
- `docs/protocol/<version>.md` on the branch; the protocol wiki text; kafka-python 1.x as the Python analogue.
- Plan / working agreement: the epic issue of the line.

## Mandatory design rules (see CLAUDE.md)
- Declarative schemas (`BinarySchemaInterface::getScheme()` + `BinarySchema`), `parent::getScheme() + [...]`, never `$header = null;`.
- Only the types/fields/versions of the branch's Kafka version; identifiers from `main`; start from `main`'s class.
- File ownership per ticket (listed in the ticket); shared contracts frozen for the wave; keep the whole suite green.
- Finish with a report: PR URL, public API, what was verified against the broker, deviations.
