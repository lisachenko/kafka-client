<?php

/*
 * This file is part of the lisachenko/kafka-client package.
 *
 * (c) Alexander Lisachenko <lisachenko.it@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Protocol\Kafka\Tests\Fixture;

use function sprintf;

/**
 * Sets and removes the client quotas of a Kafka 0.10.2.2 broker that runs in a Docker container.
 *
 * Quotas are the only way to make a broker answer with a non-zero `ThrottleTime`, and Kafka 0.9 has no api to
 * configure them: they live in ZooKeeper under `/config/clients/<client id>` and are written with the
 * `kafka-configs.sh` tool of the distribution, which the broker picks up through a watch within milliseconds
 * (`kafka/server/ConfigHandler.scala` @ 0.10.2.2). This fixture therefore shells out into the container of
 * `docker-compose.yml`:
 *
 * <code>
 *   $quota = ClientQuota::forClientId('my-test-client');
 *   $quota->set(['producer_byte_rate' => 1024]);
 *   try {
 *       // ... produce until the broker starts delaying the answers
 *   } finally {
 *       $quota->remove();
 *   }
 * </code>
 *
 * A quota that is left behind slows every later request of that client id down for an hour, so {@see self::set()}
 * must always be paired with a {@see self::remove()} in a `finally` block. The client id has to be unique for the
 * test that sets the quota: the broker of this suite is shared, and a quota is enforced for whoever sends that id.
 *
 * {@see self::isSupported()} tells whether the tool can be reached at all, so that a suite which runs against a
 * broker outside of Docker skips those tests instead of failing them.
 *
 * @see docs/protocol/1.1.md, section "Quotas and throttle time"
 */
final class ClientQuota
{
    /**
     * Name of the environment variable that overrides the container the broker runs in
     */
    public const string CONTAINER_ENV = 'KAFKA_CONTAINER';

    /**
     * Container of `docker-compose.yml`, used when the environment variable is not set
     */
    private const string DEFAULT_CONTAINER = 'kafka-1-1-1';

    /**
     * Path of the configuration tool inside the container
     */
    private const string CONFIGS_TOOL = '/opt/kafka/bin/kafka-configs.sh';

    /**
     * ZooKeeper connection string inside the container
     */
    private const string ZOOKEEPER = 'localhost:2181';

    /**
     * Names of the configuration entries that are currently set for the client id
     *
     * @var list<string>
     */
    private array $applied = [];

    private function __construct(
        private readonly string $clientId,
        private readonly string $container
    ) {}

    /**
     * Builds the quota handle of a client id
     */
    public static function forClientId(string $clientId): self
    {
        return new self($clientId, self::container());
    }

    /**
     * Tells whether the quotas of the broker under test can be changed from here
     *
     * That needs a `docker` binary and a running container that holds the Kafka distribution; a broker that is not
     * started by `docker-compose.yml` - or a machine without Docker - has neither.
     */
    public static function isSupported(): bool
    {
        return self::run(sprintf(
            'docker exec %s test -x %s',
            escapeshellarg(self::container()),
            escapeshellarg(self::CONFIGS_TOOL)
        )) !== null;
    }

    /**
     * Sets the given quota entries for the client id, replacing whatever was set before
     *
     * @param array<string, int> $configuration `producer_byte_rate` and `consumer_byte_rate` in bytes per second
     */
    public function set(array $configuration): void
    {
        $entries = [];
        foreach ($configuration as $name => $value) {
            $entries[] = "{$name}={$value}";
        }

        $output = self::run($this->command('--add-config', implode(',', $entries)));
        if ($output === null) {
            throw new \RuntimeException("Can not set the quota {$this->clientId}: " . implode(',', $entries));
        }

        $this->applied = array_merge($this->applied, array_keys($configuration));
    }

    /**
     * Removes every quota entry this fixture has set, so that the shared broker is left as it was found
     *
     * The call is idempotent and never throws: it also runs on the failure path of a test, where an exception of
     * its own would hide the failure that got the test there.
     */
    public function remove(): void
    {
        if ($this->applied === []) {
            return;
        }

        self::run($this->command('--delete-config', implode(',', array_unique($this->applied))));

        $this->applied = [];
    }

    /**
     * Returns what the broker has stored for this client id, for the message of a failed assertion
     */
    public function describe(): string
    {
        return self::run(sprintf(
            'docker exec %s %s --zookeeper %s --describe --entity-type clients --entity-name %s',
            escapeshellarg($this->container),
            escapeshellarg(self::CONFIGS_TOOL),
            escapeshellarg(self::ZOOKEEPER),
            escapeshellarg($this->clientId)
        )) ?? 'the quotas of the broker can not be read';
    }

    /**
     * Builds an `--alter` command line of the configuration tool
     */
    private function command(string $option, string $value): string
    {
        return sprintf(
            'docker exec %s %s --zookeeper %s --alter %s %s --entity-type clients --entity-name %s',
            escapeshellarg($this->container),
            escapeshellarg(self::CONFIGS_TOOL),
            escapeshellarg(self::ZOOKEEPER),
            $option,
            escapeshellarg($value),
            escapeshellarg($this->clientId)
        );
    }

    /**
     * Returns the container the broker runs in
     */
    private static function container(): string
    {
        $configured = getenv(self::CONTAINER_ENV);

        return $configured === false || trim($configured) === '' ? self::DEFAULT_CONTAINER : trim($configured);
    }

    /**
     * Runs a command and returns its output, or null when it failed
     */
    private static function run(string $command): ?string
    {
        $output   = [];
        $exitCode = 0;
        exec($command . ' 2>&1', $output, $exitCode);

        return $exitCode === 0 ? implode("\n", $output) : null;
    }
}
