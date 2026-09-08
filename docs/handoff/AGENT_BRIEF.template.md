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
- Container `<name>` = Kafka <version>, broker 127.0.0.1:9092, ZooKeeper 127.0.0.1:2181, auto-create topics on, 3 partitions.
- Unique topic/group prefix per test class (`t<n>-<what>-<random>`). Readiness: use `IntegrationTestCase` (probe topic);
  fresh topics answer 5/6 for a moment — helpers retry those codes only.
- In-container tools: `docker exec <name> /opt/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --list`,
  `kafka-console-producer.sh --broker-list localhost:9092 --topic X [--compression-codec gzip|snappy]`,
  `kafka-console-consumer.sh --zookeeper localhost:2181 --topic X --from-beginning --max-messages N`,
  `kafka-run-class.sh kafka.tools.DumpLogSegments --files /tmp/kafka-logs/X-0/00000000000000000000.log --print-data-log`.

## Specification references (the broker is the final authority)
- Kafka sources at the release tag: `<SCRATCH>/kafka-src-<version>` (`core/src/main/scala/kafka/api`, `clients/src/main/java/org/apache/kafka/common/protocol`).
- `docs/protocol/<version>.md` on the branch; the protocol wiki text; kafka-python 1.x as the Python analogue.
- Plan / working agreement: the epic issue of the line.

## Mandatory design rules (see CLAUDE.md)
- Declarative schemas (`BinarySchemaInterface::getScheme()` + `BinarySchema`), `parent::getScheme() + [...]`, never `$header = null;`.
- Only the types/fields/versions of the branch's Kafka version; identifiers from `main`; start from `main`'s class.
- File ownership per ticket (listed in the ticket); shared contracts frozen for the wave; keep the whole suite green.
- Finish with a report: PR URL, public API, what was verified against the broker, deviations.
