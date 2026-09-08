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
use Protocol\Kafka\Tests\Fixture\BrokerRecord;
use Protocol\Kafka\Tests\Fixture\ClusterReadinessProbe;

/**
 * Base class for the tests that talk to a real Kafka 0.9.0.1 broker.
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

    /**
     * Name of the environment variable that holds the `host:port` of the SSL listener of the same broker
     *
     * The broker of `docker/kafka-0.9.0.1` binds `PLAINTEXT://0.0.0.0:9092` and `SSL://0.0.0.0:9093`, so the SSL
     * endpoint is derived from the default rather than configured separately; the variable only exists for a broker
     * that publishes its SSL listener somewhere else. The tests that need it are still skipped together with the
     * rest of the suite, i.e. when KAFKA_BOOTSTRAP_SERVERS is unset.
     *
     * @see \Protocol\Kafka\Tests\Integration\SslTransportTest
     */
    public const string SSL_BOOTSTRAP_SERVERS_ENV = 'KAFKA_SSL_BOOTSTRAP_SERVERS';

    /**
     * The SSL listener of the bundled broker, used when SSL_BOOTSTRAP_SERVERS_ENV is not set
     */
    private const string DEFAULT_SSL_BOOTSTRAP_SERVER = '127.0.0.1:9093';

    /**
     * How long to wait for a booting broker to publish its metadata, in seconds
     */
    private const float CLUSTER_TIMEOUT = 60.0;

    /**
     * Brokers of the cluster, resolved once for the whole test run
     *
     * @var array<int, BrokerRecord>|null
     */
    private static ?array $clusterBrokers = null;

    public static function setUpBeforeClass(): void
    {
        if (self::bootstrapServers() === [] || self::$clusterBrokers !== null) {
            return;
        }

        // A broker that has just booted answers with an empty broker array, which is "not ready", not "no brokers"
        self::$clusterBrokers = new ClusterReadinessProbe(
            static fn(): SocketStream => new SocketStream(
                'tcp://' . self::firstBootstrapServer(),
                [ClientConfig::REQUEST_TIMEOUT_MS => 5000],
                5.0
            ),
            self::CLUSTER_TIMEOUT
        )->awaitBrokers();
    }

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
        return self::bootstrapServers()[0];
    }

    /**
     * Returns the brokers that the cluster advertised once it was ready
     *
     * @return array<int, BrokerRecord>
     */
    final protected static function clusterBrokers(): array
    {
        return self::$clusterBrokers ?? [];
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
     * Returns the `host:port` of the SSL listener of the broker under test
     */
    final protected static function sslBootstrapServer(): string
    {
        $value = getenv(self::SSL_BOOTSTRAP_SERVERS_ENV);
        if ($value === false || trim($value) === '') {
            return self::DEFAULT_SSL_BOOTSTRAP_SERVER;
        }

        return trim(explode(',', $value)[0]);
    }

    /**
     * Returns the certificate the broker presents on its SSL listener, to be used as the trust anchor of a client
     *
     * It is the self-signed certificate that `docker/kafka-0.9.0.1` puts into the keystore of the broker
     * (CN=localhost, with `localhost` and `127.0.0.1` as subject alternative names).
     */
    final protected static function brokerCertificateFile(): string
    {
        return dirname(__DIR__, 2) . '/docker/kafka-0.9.0.1/ssl/broker.crt';
    }

    /**
     * Builds a topic name that is unique for this test run, so that concurrent suites do not collide
     */
    final protected static function uniqueTopicName(string $prefix): string
    {
        return $prefix . '-' . bin2hex(random_bytes(6));
    }
}
