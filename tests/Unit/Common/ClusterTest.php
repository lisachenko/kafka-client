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

namespace Protocol\Kafka\Tests\Unit\Common;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\AllBrokersNotAvailableException;
use Protocol\Kafka\Common\Errors\CorrelationIdMismatchException;
use Protocol\Kafka\Common\Errors\InvalidTopicException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\LeaderNotAvailableException;
use Protocol\Kafka\Common\Errors\UnknownErrorException;
use Protocol\Kafka\Common\Errors\UnknownTopicOrPartitionException;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Tests\Fixture\BrokerConnection;
use Protocol\Kafka\Tests\Fixture\ResponseFrame;
use Protocol\Kafka\Tests\Fixture\ScriptedConnections;

/**
 * Tests the cluster metadata against a scripted broker.
 *
 * @see docs/protocol/2.8.md, sections "Metadata API (key 3, v0 to v7)" and "Cluster readiness"
 */
#[CoversClass(Cluster::class)]
#[CoversClass(AllBrokersNotAvailableException::class)]
final class ClusterTest extends TestCase
{
    private const string BOOTSTRAP_ADDRESS = 'tcp://kafka-1:9092';

    /**
     * `metadata.max.age.ms` of the tests that let the metadata expire.
     *
     * The tests do not wait for it to pass: they move the fetch time into the past instead. A budget of a few
     * milliseconds made them fail whenever the runner was slow between two calls (e.g. under Xdebug coverage in CI).
     */
    private const int MAX_AGE_MS = 300000;

    private ScriptedConnections $broker;

    protected function setUp(): void
    {
        $this->broker = new ScriptedConnections();
    }

    protected function tearDown(): void
    {
        ScriptedConnections::uninstall();
    }

    public function testTheClusterIsBuiltFromTheMetadataOfTheFirstBrokerThatAnswers(): void
    {
        $this->script(new BrokerConnection($this->clusterMetadata()));

        $cluster = Cluster::bootstrap($this->configuration());

        self::assertSame([0, 1], array_keys($cluster->nodes()));
        self::assertSame(['orders'], $cluster->topics());
        self::assertSame([0, 1, 2], array_keys($cluster->partitionsForTopic('orders')));
        self::assertSame(1, $cluster->leaderFor('orders', 1)->nodeId);
        self::assertSame('kafka-2', $cluster->leaderFor('orders', 1)->host);
        self::assertSame(2, $cluster->partition('orders', 2)->partitionId);
        self::assertSame($cluster->partitionsForTopic('orders'), $cluster->availablePartitionsForTopic('orders'));
    }

    public function testTheClusterKnowsItsIdAndItsController(): void
    {
        $this->script(new BrokerConnection($this->clusterMetadata()));

        $cluster = Cluster::bootstrap($this->configuration());

        self::assertSame(ResponseFrame::CLUSTER_ID, $cluster->clusterId());
        self::assertSame(0, $cluster->controller()?->nodeId, 'the ControllerId of the answer names the controller');
        self::assertSame($cluster->nodeById(0), $cluster->controller());
    }

    public function testAClusterWithoutAnElectedControllerHasNoControllerNode(): void
    {
        // -1 is what a broker answers while the cluster is electing a controller
        $this->script(
            new BrokerConnection(
                ResponseFrame::metadata(0, [[0, 'kafka-1', 9092]], ['orders' => [0 => 0]], controllerId: -1)
            )
        );

        $cluster = Cluster::bootstrap($this->configuration());

        self::assertNull($cluster->controller());
    }

    public function testAControllerThatIsNotAmongTheBrokersIsNoController(): void
    {
        // The controller went down between the metadata of the controller and the alive-broker set of the answer
        $this->script(
            new BrokerConnection(
                ResponseFrame::metadata(0, [[0, 'kafka-1', 9092]], ['orders' => [0 => 0]], controllerId: 7)
            ),
            new BrokerConnection(
                ResponseFrame::metadata(0, [[0, 'kafka-1', 9092]], ['orders' => [0 => 0]], controllerId: 7)
            )
        );

        $cluster = Cluster::bootstrap($this->configuration());

        self::assertNull($cluster->controller());
    }

    public function testTheInternalTopicsAreListedUnlessTheConfigurationExcludesThem(): void
    {
        $metadata = ResponseFrame::metadata(
            0,
            [[0, 'kafka-1', 9092]],
            ['orders' => [0 => 0], '__consumer_offsets' => [0 => 0]],
            internalTopics: ['__consumer_offsets']
        );
        $this->script(new BrokerConnection($metadata), new BrokerConnection($metadata));

        $cluster = Cluster::bootstrap($this->configuration());

        self::assertSame(['orders', '__consumer_offsets'], $cluster->topics(), 'every topic of the answer');
        self::assertSame(['orders'], $cluster->topics(true), 'without the ones Kafka keeps for itself');

        $excluding = Cluster::bootstrap($this->configuration(['exclude.internal.topics' => true]));

        self::assertSame(['orders'], $excluding->topics(), 'the configuration is the default of the argument');
        self::assertSame(['orders', '__consumer_offsets'], $excluding->topics(false));
    }

    public function testAnEmptyBrokerListIsRetriedUntilTheClusterIsReady(): void
    {
        // A broker that has just booted answers with an empty broker array, that is "not ready", not "no brokers"
        $this->script(
            new BrokerConnection(ResponseFrame::metadata(0, [])),
            new BrokerConnection(ResponseFrame::metadata(0, [])),
            new BrokerConnection($this->clusterMetadata())
        );

        $cluster = Cluster::bootstrap($this->configuration([
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 5000,
            ClientConfig::RETRY_BACKOFF_MS          => 1,
        ]));

        self::assertSame([0, 1], array_keys($cluster->nodes()));
        self::assertSame(
            3,
            $this->broker->getConnectionCount(self::BOOTSTRAP_ADDRESS),
            'each attempt asks the bootstrap server again'
        );
    }

    public function testABrokerThatNeverPublishesItsMetadataFailsTheBootstrap(): void
    {
        $emptyAnswers = array_map(
            static fn(): BrokerConnection => new BrokerConnection(ResponseFrame::metadata(0, [])),
            range(1, 50)
        );
        $this->script(...$emptyAnswers);

        $startedAt = microtime(true);
        try {
            Cluster::bootstrap($this->configuration([
                ClientConfig::METADATA_FETCH_TIMEOUT_MS => 150,
                ClientConfig::RETRY_BACKOFF_MS          => 20,
            ]));
            self::fail('A cluster without a single broker is expected to fail the bootstrap');
        } catch (AllBrokersNotAvailableException $exception) {
            $context = $exception->getContext();

            self::assertSame(KafkaException::BROKER_NOT_AVAILABLE, $exception->getCode());
            self::assertSame(150, $context['timeoutMs']);
            self::assertGreaterThan(1, $context['attempts'], 'the empty answer is retried, not reported at once');
            self::assertStringContainsString('did not advertise any broker', implode(' ', $context['cause']));
        }
        self::assertGreaterThanOrEqual(0.15, microtime(true) - $startedAt, 'the whole fetch timeout is used');
    }

    public function testAKnownTopicBreaksTheDeadlockOfATopicLessCluster(): void
    {
        // A Metadata request with an empty topic list keeps answering with zero brokers on a cluster that does not
        // host a single topic; naming a topic makes the broker create it and the controller publish the metadata
        $connection = new BrokerConnection($this->clusterMetadata());
        $this->script($connection);

        Cluster::bootstrap($this->configuration(), 'orders');

        self::assertStringContainsString('orders', $connection->getReceivedFrames()[0]);
    }

    public function testTheBootstrapMovesOnToTheNextServerWhenOneIsUnreachable(): void
    {
        // Only the second address is scripted, the first one refuses the connection
        $this->broker->on('tcp://kafka-2:9092', new BrokerConnection($this->clusterMetadata()))->install();

        $cluster = Cluster::bootstrap($this->configuration([
            ClientConfig::BOOTSTRAP_SERVERS => [self::BOOTSTRAP_ADDRESS, 'tcp://kafka-2:9092'],
        ]));

        self::assertSame([0, 1], array_keys($cluster->nodes()));
    }

    public function testAnAnswerOfAnotherRequestIsNotAcceptedAsMetadata(): void
    {
        $this->script(
            new BrokerConnection($this->clusterMetadata()),
            new BrokerConnection(ResponseFrame::metadata(999, [[0, 'kafka-1', 9092]]))->withoutCorrelationIdEcho()
        );

        $cluster = Cluster::bootstrap($this->configuration());

        $this->expectException(CorrelationIdMismatchException::class);
        $cluster->reload();
    }

    public function testAnUnknownNodeIsReportedAsNull(): void
    {
        $this->script(
            new BrokerConnection($this->clusterMetadata()),
            new BrokerConnection($this->clusterMetadata())
        );

        $cluster = Cluster::bootstrap($this->configuration());

        self::assertInstanceOf(Node::class, $cluster->nodeById(1));
        self::assertNull($cluster->nodeById(42), 'the metadata is refreshed once and the node still does not exist');
    }

    public function testAnUnknownTopicPartitionIsReported(): void
    {
        $this->script(
            new BrokerConnection($this->clusterMetadata()),
            new BrokerConnection($this->clusterMetadata())
        );

        $cluster = Cluster::bootstrap($this->configuration());

        $this->expectException(UnknownTopicOrPartitionException::class);
        $cluster->leaderFor('orders', 7);
    }

    public function testAnUnknownTopicIsLookedUpAgainBeforeItIsRejected(): void
    {
        $this->script(
            new BrokerConnection($this->clusterMetadata()),
            new BrokerConnection($this->clusterMetadata())
        );

        $cluster = Cluster::bootstrap($this->configuration());

        try {
            $cluster->partitionsForTopic('payments');
            self::fail('An unknown topic is expected to be rejected');
        } catch (InvalidTopicException $exception) {
            self::assertSame('payments', $exception->getContext()['topic']);
        }
        self::assertSame(2, $this->broker->getConnectionCount(self::BOOTSTRAP_ADDRESS));
    }

    public function testAPartitionWithoutALeaderReportsTheErrorOfTheBroker(): void
    {
        // Error code 5 while the controller is electing the leader of a partition that was just created
        $metadata = ResponseFrame::metadata(
            0,
            [[0, 'kafka-1', 9092]],
            ['orders' => [0 => -1]],
            [],
            ['orders' => [0 => KafkaException::LEADER_NOT_AVAILABLE]]
        );
        $this->script(new BrokerConnection($metadata));

        $cluster = Cluster::bootstrap($this->configuration());

        $this->expectException(LeaderNotAvailableException::class);
        $cluster->leaderFor('orders', 0);
    }

    public function testALeaderThatIsNotPartOfTheClusterIsReported(): void
    {
        $metadata = ResponseFrame::metadata(0, [[0, 'kafka-1', 9092]], ['orders' => [0 => 9]]);
        $this->script(
            new BrokerConnection($metadata),
            new BrokerConnection($metadata)
        );

        $cluster = Cluster::bootstrap($this->configuration());

        $this->expectException(UnknownErrorException::class);
        $cluster->leaderFor('orders', 0);
    }

    public function testATopicErrorCodeIsReported(): void
    {
        $metadata = ResponseFrame::metadata(
            0,
            [[0, 'kafka-1', 9092]],
            ['orders' => []],
            ['orders' => KafkaException::INVALID_TOPIC_EXCEPTION]
        );
        $this->script(new BrokerConnection($metadata));

        $cluster = Cluster::bootstrap($this->configuration());

        $this->expectException(InvalidTopicException::class);
        $cluster->partitionsForTopic('orders');
    }

    public function testTheMetadataIsRefreshedOnceItIsOlderThanItsMaximumAge(): void
    {
        $this->script(
            new BrokerConnection($this->clusterMetadata()),
            new BrokerConnection(ResponseFrame::metadata(0, [[0, 'kafka-1', 9092]], ['payments' => [0 => 0]]))
        );

        $cluster = Cluster::bootstrap($this->configuration([ClientConfig::METADATA_MAX_AGE_MS => self::MAX_AGE_MS]));

        self::assertSame(['orders'], $cluster->topics());
        self::backdateMetadata($cluster, self::MAX_AGE_MS + 1);

        self::assertSame(['payments'], $cluster->topics(), 'new topics and brokers are only found by asking again');
        self::assertSame(2, $this->broker->getConnectionCount(self::BOOTSTRAP_ADDRESS));
    }

    public function testFreshMetadataIsNotFetchedAgain(): void
    {
        $this->script(new BrokerConnection($this->clusterMetadata()));

        $cluster = Cluster::bootstrap($this->configuration([ClientConfig::METADATA_MAX_AGE_MS => self::MAX_AGE_MS]));

        $cluster->topics();
        $cluster->nodes();
        $cluster->partitionsForTopic('orders');

        self::assertSame(1, $this->broker->getConnectionCount(self::BOOTSTRAP_ADDRESS));
        self::assertLessThan(1000, $cluster->getMetadataAgeMs());
    }

    public function testAnExpiredCacheFileIsIgnored(): void
    {
        $this->script(
            new BrokerConnection($this->clusterMetadata()),
            new BrokerConnection($this->clusterMetadata())
        );

        $cacheFile     = sys_get_temp_dir() . '/t7-cluster-cache-' . bin2hex(random_bytes(6)) . '.php';
        $configuration = $this->configuration([
            ClientConfig::METADATA_CACHE_FILE => $cacheFile,
            ClientConfig::METADATA_MAX_AGE_MS => self::MAX_AGE_MS,
        ]);

        try {
            Cluster::bootstrap($configuration);
            self::assertFileExists($cacheFile, 'a successful metadata fetch fills the cache');

            self::backdateCacheFile($cacheFile, self::MAX_AGE_MS + 1);
            $reloaded = Cluster::bootstrap($configuration);

            self::assertSame(['orders'], $reloaded->topics());
            self::assertSame(
                2,
                $this->broker->getConnectionCount(self::BOOTSTRAP_ADDRESS),
                'a cache entry that is older than metadata.max.age.ms is not used'
            );
        } finally {
            @unlink($cacheFile);
        }
    }

    public function testTheLeaderEpochOfEveryPartitionOfAMetadataAnswerIsRemembered(): void
    {
        $this->script(new BrokerConnection(ResponseFrame::metadata(
            0,
            [[0, 'kafka-1', 9092], [1, 'kafka-2', 9093]],
            ['orders' => [0 => 0, 1 => 1, 2 => 0]],
            leaderEpochs: ['orders' => [0 => 4, 1 => 7, 2 => 0]]
        )));

        $cluster = Cluster::bootstrap($this->configuration());

        // The `leader_epoch` of Metadata v7 (KIP-320) is what a consumer sends back as the `current_leader_epoch`
        self::assertSame(4, $cluster->lastSeenLeaderEpoch('orders', 0));
        self::assertSame(7, $cluster->lastSeenLeaderEpoch('orders', 1));
        self::assertSame(0, $cluster->lastSeenLeaderEpoch('orders', 2));
        self::assertNull($cluster->lastSeenLeaderEpoch('orders', 3), 'a partition nobody answered has no epoch');
        self::assertNull($cluster->lastSeenLeaderEpoch('other', 0));
        self::assertSame(4, $cluster->partition('orders', 0)->leaderEpoch);
    }

    public function testAnEpochIsRememberedOnlyWhileItMovesForward(): void
    {
        $this->script(new BrokerConnection($this->clusterMetadata()));
        $cluster = Cluster::bootstrap($this->configuration());

        self::assertTrue($cluster->updateLastSeenEpochIfNewer('orders', 0, 3), 'the first epoch is always news');
        self::assertFalse($cluster->updateLastSeenEpochIfNewer('orders', 0, 3), 'the same epoch again is not');
        self::assertFalse($cluster->updateLastSeenEpochIfNewer('orders', 0, 2), 'and a lower one never is');
        self::assertSame(3, $cluster->lastSeenLeaderEpoch('orders', 0));
        self::assertTrue($cluster->updateLastSeenEpochIfNewer('orders', 0, 4));
        self::assertSame(4, $cluster->lastSeenLeaderEpoch('orders', 0));

        // -1 is "the answer did not say", which is every Metadata answer below version 7
        self::assertFalse($cluster->updateLastSeenEpochIfNewer('other', 1, -1));
        self::assertNull($cluster->lastSeenLeaderEpoch('other', 1));
    }

    public function testAPartitionOfABrokerThatIsBehindTheControllerIsNotApplied(): void
    {
        // The first answer names the broker 1 as the leader of the partition 1 in the epoch 7; the second one is
        // the answer of a broker that has not caught up: the old leader, and an epoch below the one already seen
        $this->script(
            new BrokerConnection(ResponseFrame::metadata(
                0,
                [[0, 'kafka-1', 9092], [1, 'kafka-2', 9093]],
                ['orders' => [0 => 0, 1 => 1]],
                leaderEpochs: ['orders' => [0 => 2, 1 => 7]]
            )),
            new BrokerConnection(ResponseFrame::metadata(
                0,
                [[0, 'kafka-1', 9092], [1, 'kafka-2', 9093]],
                ['orders' => [0 => 0, 1 => 0]],
                leaderEpochs: ['orders' => [0 => 3, 1 => 6]]
            ))
        );

        $cluster = Cluster::bootstrap($this->configuration());
        self::assertSame(1, $cluster->leaderFor('orders', 1)->nodeId);

        $cluster->reload();

        self::assertSame(
            1,
            $cluster->leaderFor('orders', 1)->nodeId,
            'the stale entry of a broker that is behind is dropped, not applied'
        );
        self::assertSame(7, $cluster->lastSeenLeaderEpoch('orders', 1));
        self::assertSame(
            3,
            $cluster->lastSeenLeaderEpoch('orders', 0),
            'the partitions of the same answer whose epoch did move forward are taken'
        );
    }

    /**
     * Installs the connections that the bootstrap server hands out, in order
     */
    private function script(BrokerConnection ...$connections): void
    {
        $this->broker->on(self::BOOTSTRAP_ADDRESS, ...$connections)->install();
    }

    /**
     * A cluster of two brokers with one topic of three partitions, controlled by the broker 0
     */
    private function clusterMetadata(): string
    {
        return ResponseFrame::metadata(
            0,
            [[0, 'kafka-1', 9092], [1, 'kafka-2', 9093]],
            ['orders' => [0 => 0, 1 => 1, 2 => 0]]
        );
    }

    /**
     * @param array<string, mixed> $overrides
     *
     * @return array<string, mixed>
     */
    private function configuration(array $overrides = []): array
    {
        return $overrides + [
            ClientConfig::BOOTSTRAP_SERVERS         => [self::BOOTSTRAP_ADDRESS],
            ClientConfig::CLIENT_ID                 => 't7-cluster',
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 1000,
            ClientConfig::RETRY_BACKOFF_MS          => 1,
        ];
    }
    /**
     * Moves the fetch time of the metadata that a cluster holds in memory into the past
     */
    private static function backdateMetadata(Cluster $cluster, int $byMs): void
    {
        (function (int $byMs): void {
            $this->fetchedAtMs -= $byMs;
        })->call($cluster, $byMs);
    }

    /**
     * Moves the fetch time recorded in a metadata cache file into the past
     */
    private static function backdateCacheFile(string $cacheFile, int $byMs): void
    {
        [$fetchedAtMs, $metadata] = include $cacheFile;

        file_put_contents($cacheFile, '<?php return ' . var_export([$fetchedAtMs - $byMs, $metadata], true) . ';');
        if (function_exists('opcache_invalidate')) {
            opcache_invalidate($cacheFile, true);
        }
    }
}
