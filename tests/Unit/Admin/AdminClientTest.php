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

namespace Protocol\Kafka\Tests\Unit\Admin;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\UnknownErrorException;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\Request\ControlledShutdownRequest;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorRequest;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\MetadataResponse;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequest;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;
use Protocol\Kafka\Tests\Compliance\VectorFile;
use Protocol\Kafka\Tests\Fixture\FakeStream;
use ReflectionProperty;

/**
 * Tests the way the AdminClient maps the answers of a broker onto its return values.
 *
 * The canned answers are the documented wire vectors of `docs/protocol/vectors`, i.e. frames that a real Kafka
 * 0.8.2.2 broker sent, so this suite and the compliance suite cannot disagree about what a broker says.
 *
 * @see docs/protocol/0.8.2.md, section "Wire vectors"
 */
#[CoversClass(AdminClient::class)]
final class AdminClientTest extends TestCase
{
    /**
     * Name of the topic the wire vectors were recorded for
     */
    private const string TOPIC = 't10-vectors';

    /**
     * Configuration every test of this class uses
     *
     * @var array<string, mixed>
     */
    private const array CONFIGURATION = [ClientConfig::CLIENT_ID => 't10'];

    protected function tearDown(): void
    {
        self::resetConnections();
    }

    public function testFindAllBrokersReturnsTheBrokersOfTheMetadataResponse(): void
    {
        $connection = self::connectSingleNode(self::vector('metadata', 'metadata.response.v0.single-topic'));
        $admin      = new AdminClient(self::clusterWithOneNode(), self::CONFIGURATION);

        $brokers = $admin->findAllBrokers();

        self::assertSame([0], array_keys($brokers));
        self::assertSame('127.0.0.1', $brokers[0]->host);
        self::assertSame(9092, $brokers[0]->port);
        self::assertSame(
            bin2hex((string) new MetadataRequest([], 't10')),
            bin2hex($connection->getWrittenBytes()),
            'findAllBrokers() asks for every topic with an empty topic array'
        );
    }

    public function testListTopicsReturnsTheTopicNames(): void
    {
        self::connectSingleNode(self::vector('metadata', 'metadata.response.v0.single-topic'));
        $admin = new AdminClient(self::clusterWithOneNode(), self::CONFIGURATION);

        self::assertSame([self::TOPIC], $admin->listTopics());
    }

    public function testDescribeTopicsReturnsTheMetadataOfEachTopic(): void
    {
        $connection = self::connectSingleNode(self::vector('metadata', 'metadata.response.v0.single-topic'));
        $admin      = new AdminClient(self::clusterWithOneNode(), self::CONFIGURATION);

        $topics = $admin->describeTopics([self::TOPIC]);

        self::assertSame([self::TOPIC], array_keys($topics));
        self::assertSame(0, $topics[self::TOPIC]->topicErrorCode);
        self::assertSame([2, 1, 0], array_keys($topics[self::TOPIC]->partitions));
        self::assertSame(0, $topics[self::TOPIC]->partitions[0]->leader);
        self::assertSame(
            bin2hex((string) new MetadataRequest([self::TOPIC], 't10')),
            bin2hex($connection->getWrittenBytes())
        );
    }

    public function testListOffsetsMapsThePartitionOffsetsOfEveryTopic(): void
    {
        $connection = self::connectSingleNode(self::vector('offsets', 'offsets.response.v0.latest'));
        $admin      = new AdminClient(self::clusterWithOneNode(), self::CONFIGURATION);

        $offsets = $admin->listOffsets([self::TOPIC => [0]]);

        self::assertSame([self::TOPIC => [0 => [2]]], $offsets, 'the log end offset of the partition');
        self::assertSame(
            bin2hex((string) new OffsetsRequest([self::TOPIC => [0 => OffsetsRequest::LATEST]], 1, -1, 't10')),
            bin2hex($connection->getWrittenBytes()),
            'an ordinary client asks with the replica id -1'
        );
    }

    public function testListOffsetsThrowsThePartitionErrorOfTheBroker(): void
    {
        self::connectSingleNode(self::vector('offsets', 'offsets.response.v0.unknown-partition'));
        $admin = new AdminClient(self::clusterWithOneNode(), self::CONFIGURATION);

        $this->expectExceptionMessage('This server does not host this topic-partition');

        // The leader of the partition is looked up in the cluster metadata, so the request has to name a known one
        $admin->listOffsets([self::TOPIC => [0]]);
    }

    public function testListGroupOffsetsAsksTheCoordinatorAndReturnsTheCommittedOffsets(): void
    {
        $connection = self::connectSingleNode(
            self::vector('group-coordinator', 'groupcoordinator.response.v0')
            . self::vector('offset-fetch', 'offsetfetch.response.v1')
        );
        $admin = new AdminClient(self::clusterWithOneNode(), self::CONFIGURATION);

        $topics = $admin->listGroupOffsets('t10-vectors-group', [self::TOPIC => [0]]);

        self::assertSame([self::TOPIC], array_keys($topics));
        self::assertSame(1, $topics[self::TOPIC]->partitions[0]->offset);
        self::assertSame(0, $topics[self::TOPIC]->partitions[0]->errorCode);
        self::assertSame(
            bin2hex((string) new GroupCoordinatorRequest('t10-vectors-group', 't10'))
            . bin2hex((string) new OffsetFetchRequest('t10-vectors-group', [self::TOPIC => [0]], 't10')),
            bin2hex($connection->getWrittenBytes()),
            'the coordinator lookup comes first, the OffsetFetch v1 goes to the coordinator it named'
        );
    }

    public function testListGroupOffsetsAcceptsAPartitionThatWasNeverCommitted(): void
    {
        // Version 0 answers a partition without a committed offset with the offset -1 and the error code 3
        self::connectSingleNode(self::vector('offset-fetch', 'offsetfetch.response.v0.no-committed-offset'));
        $admin = new AdminClient(
            self::clusterWithOneNode(),
            self::CONFIGURATION + [ClientConfig::OFFSETS_STORAGE => ClientConfig::OFFSETS_STORAGE_ZOOKEEPER]
        );

        $topics = $admin->listGroupOffsets('t10-vectors-group', [self::TOPIC => [1]]);

        self::assertSame(-1, $topics[self::TOPIC]->partitions[1]->offset);
        self::assertSame(3, $topics[self::TOPIC]->partitions[1]->errorCode);
    }

    public function testFindCoordinatorResolvesTheNodeOfTheCluster(): void
    {
        self::connectSingleNode(self::vector('group-coordinator', 'groupcoordinator.response.v0'));
        $admin = new AdminClient(self::clusterWithOneNode(), self::CONFIGURATION);

        $coordinator = $admin->findCoordinator('t10-vectors-group');

        self::assertSame(0, $coordinator->nodeId);
        self::assertSame('127.0.0.1', $coordinator->host);
        self::assertSame(9092, $coordinator->port);
    }

    public function testControlledShutdownThrowsTheErrorCodeOfTheController(): void
    {
        $connection = self::connectSingleNode(
            self::vector('controlled-shutdown', 'controlledshutdown.response.v0')
        );
        $admin = new AdminClient(self::clusterWithOneNode(), self::CONFIGURATION);

        try {
            $admin->controlledShutdown(4242);
            self::fail('An unknown broker id has to be reported as an error');
        } catch (UnknownErrorException $exception) {
            self::assertStringContainsString('4242', $exception->getMessage());
        }

        self::assertSame(
            bin2hex((string) new ControlledShutdownRequest(4242)),
            bin2hex($connection->getWrittenBytes())
        );
    }

    /**
     * Returns the raw frame of a documented wire vector
     */
    private static function vector(string $api, string $id): string
    {
        foreach (VectorFile::read($api)['vectors'] as $vector) {
            if ($vector['id'] === $id) {
                return (string) hex2bin($vector['hex']);
            }
        }

        self::fail("There is no wire vector {$id} in docs/protocol/vectors/{$api}.json");
    }

    /**
     * Builds a cluster of one node, with the topics and partitions of the metadata vector
     */
    private static function clusterWithOneNode(): Cluster
    {
        $metadata = MetadataResponse::unpack(
            new StringStream(self::vector('metadata', 'metadata.response.v0.single-topic'))
        );

        $cluster = new \ReflectionClass(Cluster::class)->newInstanceWithoutConstructor();
        new ReflectionProperty($cluster, 'configuration')->setValue($cluster, self::CONFIGURATION);
        new ReflectionProperty($cluster, 'nodes')->setValue($cluster, $metadata->brokers);
        new ReflectionProperty($cluster, 'topicPartitions')->setValue($cluster, $metadata->topics);

        return $cluster;
    }

    /**
     * Makes every connection of the single node of the cluster answer with the given bytes
     */
    private static function connectSingleNode(string $response): FakeStream
    {
        $connection = new FakeStream($response);
        self::resetConnections(['127.0.0.1' => [9092 => $connection]]);

        return $connection;
    }

    /**
     * Replaces the process-wide connection cache of {@see Node}
     *
     * @param array<string, array<int, Stream>> $connections
     */
    private static function resetConnections(array $connections = []): void
    {
        new ReflectionProperty(Node::class, 'nodeConnections')->setValue(null, $connections);
    }
}
