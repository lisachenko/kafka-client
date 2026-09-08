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

use PHPUnit\Framework\Attributes\CoversClass;
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\AllBrokersNotAvailableException;
use Protocol\Kafka\Common\Errors\NetworkException;
use Protocol\Kafka\Common\Errors\TopicPartitionRequestException;
use Protocol\Kafka\Common\FetchedPartition;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Network\ConnectionFactory;
use Protocol\Kafka\Network\RetryPolicy;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\Request\MetadataResponse;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * Drives the network client against a real Kafka 0.8.2.2 broker.
 *
 * The broker is the authority on everything the client does over the wire: it keeps a connection open between two
 * requests, it echoes the correlation id of each request back, and it is the only thing that can tell the client
 * that its cached metadata points at the wrong leader.
 *
 * @see docs/protocol/0.9.0.md
 */
#[CoversClass(Client::class)]
#[CoversClass(Cluster::class)]
#[CoversClass(ConnectionFactory::class)]
#[CoversClass(RetryPolicy::class)]
#[CoversClass(Node::class)]
#[CoversClass(FetchedPartition::class)]
final class NetworkClientTest extends IntegrationTestCase
{
    /**
     * Client id that identifies the requests of this test in the logs of the broker
     */
    private const string CLIENT_ID = 'kafka-client-t7';

    /**
     * How long the broker may take to acknowledge a produce request, in milliseconds
     */
    private const int PRODUCE_TIMEOUT_MS = 5000;

    /**
     * Topic of the current test, created and given a leader by {@see NetworkClientTest::setUp()}
     */
    private string $topic;

    /**
     * Cache files that were written by a test and have to be removed again
     *
     * @var list<string>
     */
    private array $cacheFiles = [];

    protected function setUp(): void
    {
        parent::setUp();

        // The connections are cached per process, a test must not inherit the ones of another test class
        ConnectionFactory::closeAll();

        $this->topic = self::uniqueTopicName('t7-network');
        new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, self::CLIENT_ID)
            ->awaitTopicWithLeaders($this->topic);
    }

    protected function tearDown(): void
    {
        ConnectionFactory::closeAll();
        foreach ($this->cacheFiles as $cacheFile) {
            @unlink($cacheFile);
        }
        $this->cacheFiles = [];
    }

    public function testTheConnectionToABrokerIsKeptOpenBetweenRequests(): void
    {
        $configuration = $this->configuration();
        $cluster       = Cluster::bootstrap($configuration, $this->topic);
        $client        = new Client($cluster, $configuration);
        $leader        = $cluster->leaderFor($this->topic, 0);

        $client->produce([$this->topic => [0 => [new Record('first request')]]]);
        $connection = $leader->getConnection($configuration);
        self::assertInstanceOf(SocketStream::class, $connection);
        $localAddress = stream_socket_get_name($connection->getStreamSocket(), false);

        $client->produce([$this->topic => [0 => [new Record('second request')]]]);

        self::assertSame($connection, $leader->getConnection($configuration));
        self::assertSame(
            $localAddress,
            stream_socket_get_name($connection->getStreamSocket(), false),
            'the second request travelled over the very same TCP connection'
        );
    }

    public function testAConnectionThatSatIdleForTooLongIsOpenedAgain(): void
    {
        $configuration = $this->configuration([ClientConfig::CONNECTIONS_MAX_IDLE_MS => 50]);
        $cluster       = Cluster::bootstrap($configuration, $this->topic);
        $client        = new Client($cluster, $configuration);
        $leader        = $cluster->leaderFor($this->topic, 0);

        $client->produce([$this->topic => [0 => [new Record('before the idle period')]]]);
        $firstAddress = stream_socket_get_name($leader->getConnection($configuration)->getStreamSocket(), false);

        // A broker closes a connection that has been idle for connections.max.idle.ms on its side as well
        usleep(150000);
        $result = $client->produce([$this->topic => [0 => [new Record('after the idle period')]]]);

        self::assertSame(0, $result[$this->topic][0]->errorCode);
        self::assertNotSame(
            $firstAddress,
            stream_socket_get_name($leader->getConnection($configuration)->getStreamSocket(), false),
            'the idle connection was replaced by a fresh one'
        );
    }

    public function testEveryPartitionLeaderIsServedAndTheRecordsComeBack(): void
    {
        $configuration = $this->configuration();
        $cluster       = Cluster::bootstrap($configuration, $this->topic);
        $client        = new Client($cluster, $configuration);

        $produced = $client->produce([
            $this->topic => [
                0 => [new Record('partition zero')],
                1 => [new Record('partition one')],
                2 => [new Record('partition two', 'with a key')],
            ],
        ]);

        // The broker does not promise any order for the partitions of its answer
        $acknowledged = $produced[$this->topic];
        ksort($acknowledged);

        self::assertSame([0, 1, 2], array_keys($acknowledged));
        foreach ($acknowledged as $partitionInfo) {
            self::assertSame(0, $partitionInfo->errorCode);
            self::assertSame(0, $partitionInfo->baseOffset, 'the topic of this test is brand new');
        }

        $fetched = $client->fetch([$this->topic => [0 => 0, 1 => 0, 2 => 0]], 2000);

        self::assertSame('partition zero', $fetched[$this->topic][0][0]->value);
        self::assertSame('partition one', $fetched[$this->topic][1][0]->value);
        self::assertSame('partition two', $fetched[$this->topic][2][0]->value);
        self::assertSame('with a key', $fetched[$this->topic][2][0]->key);
    }

    public function testAFetchReportsTheHighWaterMarkOfEveryPartition(): void
    {
        $configuration = $this->configuration();
        $cluster       = Cluster::bootstrap($configuration, $this->topic);
        $client        = new Client($cluster, $configuration);

        $client->produce([$this->topic => [0 => [new Record('one'), new Record('two')]]]);

        $partition = $client->fetchPartitions([$this->topic => [0 => 0]], 2000)[$this->topic][0];

        self::assertInstanceOf(FetchedPartition::class, $partition);
        self::assertSame(0, $partition->errorCode);
        self::assertSame(2, $partition->highWaterMarkOffset, 'the log of this partition ends behind both records');
        self::assertSame(2, $partition->getNextOffset());
        self::assertFalse($partition->isSingleMessageTooLarge());
        self::assertSame(['one', 'two'], array_column($partition->getRecords(), 'value'));
    }

    public function testTheOffsetsOfEveryPartitionAreListed(): void
    {
        $configuration = $this->configuration();
        $cluster       = Cluster::bootstrap($configuration, $this->topic);
        $client        = new Client($cluster, $configuration);

        $client->produce([$this->topic => [1 => [new Record('a'), new Record('b'), new Record('c')]]]);

        $offsets = $client->fetchTopicPartitionOffsets([$this->topic => [0 => -1, 1 => -1, 2 => -1]]);

        self::assertSame(0, $offsets[$this->topic][0]);
        self::assertSame(3, $offsets[$this->topic][1]);
        self::assertSame(0, $offsets[$this->topic][2]);
    }

    public function testAStaleLeaderInTheMetadataCacheIsRecoveredByARetry(): void
    {
        $configuration = $this->staleMetadataConfiguration([ClientConfig::RETRIES => 2]);
        $cluster       = Cluster::bootstrap($configuration);

        // The cached metadata sends the client to a port nothing listens on, exactly what a leader that moved to
        // another broker looks like from here
        self::assertNotSame(9092, $cluster->leaderFor($this->topic, 0)->port);

        $result = new Client($cluster, $configuration)
            ->produce([$this->topic => [0 => [new Record('found the real leader')]]]);

        self::assertSame(0, $result[$this->topic][0]->errorCode);
        self::assertSame(0, $result[$this->topic][0]->baseOffset);
        self::assertSame(
            9092,
            $cluster->leaderFor($this->topic, 0)->port,
            'the retry refreshed the metadata and the cluster knows the real leader again'
        );
    }

    public function testAStaleLeaderIsReportedWhenNoRetryIsAllowed(): void
    {
        $configuration = $this->staleMetadataConfiguration([ClientConfig::RETRIES => 0]);
        $cluster       = Cluster::bootstrap($configuration);
        $client        = new Client($cluster, $configuration);

        try {
            $client->produce([$this->topic => [0 => [new Record('never delivered')]]]);
            self::fail('An unreachable leader is expected to be reported when retries are switched off');
        } catch (TopicPartitionRequestException $exception) {
            self::assertSame([], $exception->getPartialResult());
            self::assertInstanceOf(NetworkException::class, $exception->getExceptions()[$this->topic][0]);
        }
    }

    public function testABootstrapWithoutAReachableBrokerGivesUpAfterItsTimeout(): void
    {
        $configuration = $this->configuration([
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://127.0.0.1:' . self::closedPort()],
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 200,
            ClientConfig::RETRY_BACKOFF_MS          => 20,
        ]);

        $startedAt = microtime(true);
        try {
            Cluster::bootstrap($configuration);
            self::fail('A cluster without a reachable broker is expected to fail the bootstrap');
        } catch (AllBrokersNotAvailableException $exception) {
            self::assertGreaterThan(1, $exception->getContext()['attempts']);
        }
        self::assertGreaterThanOrEqual(0.2, microtime(true) - $startedAt);
    }

    public function testTheBootstrapAsksForTheTopicItIsGoingToWorkWith(): void
    {
        // A Metadata request that names a topic is what breaks the deadlock of a cluster without a single topic:
        // auto.create.topics.enable makes the broker create it, and the controller then publishes the metadata
        $freshTopic = self::uniqueTopicName('t7-bootstrap');

        $cluster = Cluster::bootstrap($this->configuration(), $freshTopic);

        self::assertNotSame([], $cluster->nodes());
        self::assertContains($freshTopic, $cluster->topics());
    }

    /**
     * Builds a configuration whose metadata cache points the leader of the topic at a port nothing listens on
     *
     * @param array<string, mixed> $overrides
     *
     * @return array<string, mixed>
     */
    private function staleMetadataConfiguration(array $overrides): array
    {
        $cacheFile          = sys_get_temp_dir() . '/t7-stale-leader-' . bin2hex(random_bytes(6)) . '.php';
        $this->cacheFiles[] = $cacheFile;

        $configuration = $this->configuration($overrides + [
            ClientConfig::METADATA_CACHE_FILE => $cacheFile,
            ClientConfig::METADATA_MAX_AGE_MS => 300000,
        ]);

        // Let a healthy bootstrap write the cache, then move every broker of it to a port nothing listens on
        Cluster::bootstrap($configuration, $this->topic);

        /** @var array{int, MetadataResponse} $cached */
        $cached     = include $cacheFile;
        $closedPort = self::closedPort();
        foreach ($cached[1]->brokers as $broker) {
            $broker->port = $closedPort;
        }
        file_put_contents($cacheFile, '<?php return ' . var_export($cached, true) . ';');
        if (function_exists('opcache_invalidate')) {
            opcache_invalidate($cacheFile, true);
        }

        return $configuration;
    }

    /**
     * Returns a local port that nothing listens on
     */
    private static function closedPort(): int
    {
        $server = stream_socket_server('tcp://127.0.0.1:0', $errorNumber, $errorString);
        if ($server === false) {
            self::markTestSkipped("Can not reserve a local TCP port: {$errorString}");
        }
        $address = (string) stream_socket_get_name($server, false);
        fclose($server);

        return (int) substr($address, (int) strrpos($address, ':') + 1);
    }

    /**
     * @param array<string, mixed> $overrides
     *
     * @return array<string, mixed>
     */
    private function configuration(array $overrides = []): array
    {
        return $overrides + [
            ClientConfig::BOOTSTRAP_SERVERS  => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID          => self::CLIENT_ID,
            ClientConfig::REQUEST_TIMEOUT_MS => 10000,
            ClientConfig::RETRY_BACKOFF_MS   => 200,

            ProducerConfig::ACKS       => 1,
            ProducerConfig::TIMEOUT_MS => self::PRODUCE_TIMEOUT_MS,

            ConsumerConfig::FETCH_MAX_WAIT_MS         => 2000,
            ConsumerConfig::FETCH_MIN_BYTES           => 1,
            ConsumerConfig::MAX_PARTITION_FETCH_BYTES => 65536,
        ] + ProducerConfig::getDefaultConfiguration();
    }
}
