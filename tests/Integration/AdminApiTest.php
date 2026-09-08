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
use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\TopicMetadata;
use Protocol\Kafka\Protocol\Request\ControlledShutdownRequest;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;

/**
 * Exercises the AdminClient against a real Kafka 0.8.2.2 broker.
 *
 * @see docs/protocol/0.9.0.md
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(ControlledShutdownRequest::class)]
final class AdminApiTest extends IntegrationTestCase
{
    /**
     * A broker id no single-node test cluster can ever have; asking for a real one would shut that broker down
     */
    private const int UNKNOWN_BROKER_ID = 4242;

    /**
     * Topic of this test class, created once: the broker is shared with the other suites, and creating a topic on a
     * 0.8 cluster means a round trip through ZooKeeper and a leader election
     */
    private static ?string $topic = null;

    private Cluster $cluster;

    private AdminClient $admin;

    protected function setUp(): void
    {
        parent::setUp();

        $this->cluster = Cluster::bootstrap($this->configuration());
        $this->admin   = new AdminClient($this->cluster, $this->configuration());
    }

    public function testFindAllBrokersReturnsTheLiveBrokersOfTheCluster(): void
    {
        $brokers = $this->admin->findAllBrokers();

        self::assertNotEmpty($brokers, 'an empty broker array means "not ready", the probe should have waited');
        foreach ($brokers as $nodeId => $broker) {
            self::assertSame($nodeId, $broker->nodeId, 'the brokers are indexed by their node id');
            self::assertNotSame('', $broker->host);
            self::assertGreaterThan(0, $broker->port);
        }
        self::assertSame(array_keys(self::clusterBrokers()), array_keys($brokers));
    }

    public function testDescribeTopicsCreatesAnUnknownTopicAndThenReportsItsPartitions(): void
    {
        $topic = self::$topic ??= self::uniqueTopicName('t10-admin');

        // auto.create.topics.enable is on, so this Metadata request is what creates the topic - there is no
        // CreateTopics api in 0.8. The first answer carries the error code 5 and no partitions at all.
        $firstAnswer = $this->admin->describeTopics([$topic])[$topic];
        self::assertSame($topic, $firstAnswer->topic);
        self::assertContains(
            $firstAnswer->topicErrorCode,
            [KafkaException::NO_ERROR, KafkaException::LEADER_NOT_AVAILABLE],
            'a freshly created topic is announced as LeaderNotAvailable until the controller elected the leaders'
        );

        $metadata = $this->awaitTopic($topic);

        self::assertSame(KafkaException::NO_ERROR, $metadata->topicErrorCode);
        self::assertCount(3, $metadata->partitions, 'the test broker runs with num.partitions=3');
        foreach ($metadata->partitions as $partitionId => $partition) {
            self::assertSame($partitionId, $partition->partitionId);
            self::assertGreaterThanOrEqual(0, $partition->leader, 'a leader was elected for the partition');
            self::assertNotEmpty($partition->replicas);
        }

        self::assertContains($topic, $this->admin->listTopics());
    }

    public function testListOffsetsReturnsTheBoundariesOfAnEmptyTopic(): void
    {
        $topic      = $this->topic();
        $partitions = array_keys($this->awaitTopic($topic)->partitions);
        $this->cluster->reload();

        $latest   = $this->admin->listOffsets([$topic => $partitions]);
        $earliest = $this->admin->listOffsets([$topic => $partitions], OffsetsRequest::EARLIEST);

        self::assertSame([$topic], array_keys($latest));
        foreach ($partitions as $partition) {
            self::assertSame([0], $latest[$topic][$partition], 'nothing was produced into the topic yet');
            self::assertSame([0], $earliest[$topic][$partition]);
        }
    }

    public function testFindCoordinatorReturnsANodeOfTheCluster(): void
    {
        $groupId = 't10-admin-group-' . bin2hex(random_bytes(6));

        $coordinator = $this->admin->findCoordinator($groupId);

        self::assertArrayHasKey($coordinator->nodeId, $this->admin->findAllBrokers());
        self::assertNotSame('', $coordinator->host);
    }

    public function testListGroupOffsetsReportsAGroupThatNeverCommittedAnything(): void
    {
        $groupId    = 't10-admin-group-' . bin2hex(random_bytes(6));
        $topic      = $this->topic();
        $partitions = array_keys($this->awaitTopic($topic)->partitions);

        $topics = $this->admin->listGroupOffsets($groupId, [$topic => $partitions]);

        self::assertSame([$topic], array_keys($topics));
        foreach ($topics[$topic]->partitions as $partition) {
            self::assertSame(-1, $partition->offset, 'an uncommitted partition comes back with the offset -1');
            self::assertSame(KafkaException::NO_ERROR, $partition->errorCode, 'version 1 reports no error for it');
        }
    }

    public function testControlledShutdownOfAnUnknownBrokerIsRefused(): void
    {
        // The controller throws BrokerNotAvailableException, but ControlledShutdownRequest.handleError() maps
        // e.getCause() - which is null - so the code -1 (Unknown) is what reaches the wire, see the protocol document
        $this->expectException(KafkaException::class);
        $this->expectExceptionCode(KafkaException::UNKNOWN);

        $this->admin->controlledShutdown(self::UNKNOWN_BROKER_ID);
    }

    /**
     * Returns the topic of this test class, created by the first test that needs it
     */
    private function topic(): string
    {
        return self::$topic ??= self::uniqueTopicName('t10-admin');
    }

    /**
     * Waits until the controller has elected the leaders of a topic and returns its metadata
     */
    private function awaitTopic(string $topic): TopicMetadata
    {
        $deadline = microtime(true) + 30.0;
        do {
            $metadata = $this->admin->describeTopics([$topic])[$topic] ?? null;
            if ($metadata !== null && $metadata->topicErrorCode === KafkaException::NO_ERROR
                && $metadata->partitions !== []) {
                return $metadata;
            }
            usleep(200000);
        } while (microtime(true) < $deadline);

        self::fail("The topic {$topic} did not become available in time");
    }

    /**
     * Client configuration pointing at the broker under test
     *
     * @return array<string, mixed>
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => 't10-admin',
            ClientConfig::REQUEST_TIMEOUT_MS        => 10000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
        ];
    }
}
