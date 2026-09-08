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

namespace Protocol\Kafka\Tests\Integration;

use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\IO\SocketStream;

/**
 * Base class for the tests that talk to a real Kafka 0.8.2.2 broker.
 *
 * The whole suite is skipped unless the KAFKA_BOOTSTRAP_SERVERS environment variable points at a running broker,
 * e.g. the one started by `docker compose up`:
 *
 *   KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 vendor/bin/phpunit --testsuite integration
 */
abstract class IntegrationTestCase extends TestCase
{
    /**
     * Name of the environment variable that holds a comma-separated list of `host:port` pairs
     */
    public const string BOOTSTRAP_SERVERS_ENV = 'KAFKA_BOOTSTRAP_SERVERS';

    protected function setUp(): void
    {
        if (self::bootstrapServers() === []) {
            $this->markTestSkipped(
                self::BOOTSTRAP_SERVERS_ENV . ' is not set, skipping the tests against a live Kafka broker'
            );
        }
    }

    /**
     * Returns the configured list of bootstrap servers as `host:port` strings
     *
     * @return list<string>
     */
    final protected static function bootstrapServers(): array
    {
        $value = getenv(self::BOOTSTRAP_SERVERS_ENV);
        if ($value === false || trim($value) === '') {
            return [];
        }

        return array_values(array_filter(array_map(trim(...), explode(',', $value))));
    }

    /**
     * Returns the first configured bootstrap server as `host:port`
     */
    final protected static function firstBootstrapServer(): string
    {
        $servers = self::bootstrapServers();

        return $servers[0];
    }

    /**
     * Opens a socket stream to the first bootstrap server
     *
     * @param array<string, mixed> $configuration Extra client configuration options
     */
    final protected function connect(array $configuration = []): SocketStream
    {
        return new SocketStream(
            'tcp://' . self::firstBootstrapServer(),
            $configuration + [
                ClientConfig::SEND_BUFFER_BYTES    => 131072,
                ClientConfig::RECEIVE_BUFFER_BYTES => 32768,
                ClientConfig::REQUEST_TIMEOUT_MS   => 10000,
            ],
            5.0
        );
    }

    /**
     * Builds a topic name that is unique for this test run, so that concurrent suites do not collide
     */
    final protected static function uniqueTopicName(string $prefix): string
    {
        return $prefix . '-' . bin2hex(random_bytes(6));
    }
}
