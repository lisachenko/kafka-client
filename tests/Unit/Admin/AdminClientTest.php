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
use Protocol\Kafka\Common\Errors\AllBrokersNotAvailableException;
use Protocol\Kafka\Common\Errors\UnknownErrorException;
use Protocol\Kafka\Protocol\Request\AbstractRequest;
use Protocol\Kafka\Protocol\Request\ControlledShutdownRequest;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorRequest;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequest;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequestV0;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;
use Protocol\Kafka\Tests\Compliance\VectorFile;
use Protocol\Kafka\Tests\Fixture\BrokerConnection;
use Protocol\Kafka\Tests\Fixture\ScriptedConnections;

/**
 * Tests the way the AdminClient maps the answers of a broker onto its return values.
 *
 * The canned answers are the documented wire vectors of `docs/protocol/vectors`, i.e. frames that a real Kafka
 * 0.8.2.2 broker sent, replayed by a scripted broker connection - so this suite and the compliance suite cannot
 * disagree about what a broker says. The scripted connection echoes the correlation id of each request the way a
 * broker does, which is what the client validates the answer against.
 *
 * @see docs/protocol/0.9.0.md, section "Wire vectors"
 */
#[CoversClass(AdminClient::class)]
final class AdminClientTest extends TestCase
{
    /**
     * Name of the topic the wire vectors were recorded for
     */
    private const string TOPIC = 't10-vectors';

    /**
     * Name of the consumer group the wire vectors were recorded for
     */
    private const string GROUP = 't10-vectors-group';

    /**
     * Address the cluster is bootstrapped from
     */
    private const string BOOTSTRAP_ADDRESS = 'tcp://bootstrap:9092';

    /**
     * Address of the single broker that the metadata vector announces
     */
    private const string BROKER_ADDRESS = 'tcp://127.0.0.1:9092';

    private ScriptedConnections $brokers;

    protected function setUp(): void
    {
        $this->brokers = new ScriptedConnections();
    }

    protected function tearDown(): void
    {
        ScriptedConnections::uninstall();
    }

    public function testFindAllBrokersReturnsTheBrokersOfTheMetadataResponse(): void
    {
        $broker = $this->scriptBroker(self::vector('metadata', 'metadata.response.v0.single-topic'));
        $admin  = $this->adminClient();

        $brokers = $admin->findAllBrokers();

        self::assertSame([0], array_keys($brokers));
        self::assertSame('127.0.0.1', $brokers[0]->host);
        self::assertSame(9092, $brokers[0]->port);
        self::assertSame(
            [self::requestFrame(new MetadataRequest([], 't10', $broker->getReceivedCorrelationIds()[0]))],
            $broker->getReceivedFrames(),
            'findAllBrokers() asks for every topic with an empty topic array'
        );
    }

    public function testListTopicsReturnsTheTopicNames(): void
    {
        $this->scriptBroker(self::vector('metadata', 'metadata.response.v0.single-topic'));

        self::assertSame([self::TOPIC], $this->adminClient()->listTopics());
    }

    public function testDescribeTopicsReturnsTheMetadataOfEachTopic(): void
    {
        $broker = $this->scriptBroker(self::vector('metadata', 'metadata.response.v0.single-topic'));

        $topics = $this->adminClient()->describeTopics([self::TOPIC]);

        self::assertSame([self::TOPIC], array_keys($topics));
        self::assertSame(0, $topics[self::TOPIC]->topicErrorCode);
        self::assertSame([2, 1, 0], array_keys($topics[self::TOPIC]->partitions));
        self::assertSame(0, $topics[self::TOPIC]->partitions[0]->leader);
        self::assertSame(
            [self::requestFrame(new MetadataRequest([self::TOPIC], 't10', $broker->getReceivedCorrelationIds()[0]))],
            $broker->getReceivedFrames()
        );
    }

    public function testListOffsetsMapsThePartitionOffsetsOfEveryTopic(): void
    {
        $broker = $this->scriptBroker(self::vector('offsets', 'offsets.response.v0.latest'));

        $offsets = $this->adminClient()->listOffsets([self::TOPIC => [0]]);

        self::assertSame([self::TOPIC => [0 => [2]]], $offsets, 'the log end offset of the partition');
        self::assertSame(
            [self::requestFrame(new OffsetsRequest(
                [self::TOPIC => [0 => OffsetsRequest::LATEST]],
                1,
                -1,
                't10',
                $broker->getReceivedCorrelationIds()[0]
            ))],
            $broker->getReceivedFrames(),
            'an ordinary client asks the leader of the partition with the replica id -1'
        );
    }

    public function testListOffsetsThrowsThePartitionErrorOfTheBroker(): void
    {
        $this->scriptBroker(self::vector('offsets', 'offsets.response.v0.unknown-partition'));

        $this->expectExceptionMessage('This server does not host this topic-partition');

        // The leader of the partition is looked up in the cluster metadata, so the request has to name a known one
        $this->adminClient()->listOffsets([self::TOPIC => [0]]);
    }

    public function testListGroupOffsetsAsksTheCoordinatorAndReturnsTheCommittedOffsets(): void
    {
        $broker = $this->scriptBroker(
            self::vector('group-coordinator', 'groupcoordinator.response.v0'),
            self::vector('offset-fetch', 'offsetfetch.response.v1')
        );

        $topics = $this->adminClient()->listGroupOffsets(self::GROUP, [self::TOPIC => [0]]);

        self::assertSame([self::TOPIC], array_keys($topics));
        self::assertSame(1, $topics[self::TOPIC]->partitions[0]->offset);
        self::assertSame(0, $topics[self::TOPIC]->partitions[0]->errorCode);

        [$lookupId, $fetchId] = $broker->getReceivedCorrelationIds();
        self::assertSame(
            [
                self::requestFrame(new GroupCoordinatorRequest(self::GROUP, 't10', $lookupId)),
                self::requestFrame(new OffsetFetchRequest(self::GROUP, [self::TOPIC => [0]], 't10', $fetchId)),
            ],
            $broker->getReceivedFrames(),
            'the coordinator lookup comes first, the OffsetFetch v1 goes to the coordinator it named'
        );
        self::assertNotSame($lookupId, $fetchId, 'every request carries its own correlation id');
    }

    public function testListGroupOffsetsAcceptsAPartitionThatWasNeverCommitted(): void
    {
        // Version 0 reads from ZooKeeper: any broker answers it, and a partition without a committed offset comes
        // back with the offset -1 and the error code 3
        $broker = $this->scriptBroker(self::vector('offset-fetch', 'offsetfetch.response.v0.no-committed-offset'));
        $admin  = $this->adminClient([ClientConfig::OFFSETS_STORAGE => ClientConfig::OFFSETS_STORAGE_ZOOKEEPER]);

        $topics = $admin->listGroupOffsets(self::GROUP, [self::TOPIC => [1]]);

        self::assertSame(-1, $topics[self::TOPIC]->partitions[1]->offset);
        self::assertSame(3, $topics[self::TOPIC]->partitions[1]->errorCode);
        self::assertSame(
            [self::requestFrame(
                new OffsetFetchRequestV0(self::GROUP, [self::TOPIC => [1]], 't10', $broker->getReceivedCorrelationIds()[0])
            )],
            $broker->getReceivedFrames(),
            'the ZooKeeper-backed version 0 needs no coordinator lookup'
        );
    }

    public function testFindCoordinatorResolvesTheNodeOfTheCluster(): void
    {
        $this->scriptBroker(self::vector('group-coordinator', 'groupcoordinator.response.v0'));

        $coordinator = $this->adminClient()->findCoordinator(self::GROUP);

        self::assertSame(0, $coordinator->nodeId);
        self::assertSame('127.0.0.1', $coordinator->host);
        self::assertSame(9092, $coordinator->port);
    }

    public function testControlledShutdownThrowsTheErrorCodeOfTheController(): void
    {
        $broker = $this->scriptBroker(self::vector('controlled-shutdown', 'controlledshutdown.response.v0'));
        $admin  = $this->adminClient();

        try {
            $admin->controlledShutdown(4242);
            self::fail('An unknown broker id has to be reported as an error');
        } catch (UnknownErrorException $exception) {
            self::assertStringContainsString('4242', $exception->getMessage());
        }

        self::assertSame(
            [self::requestFrame(new ControlledShutdownRequest(4242, $broker->getReceivedCorrelationIds()[0]))],
            $broker->getReceivedFrames()
        );
    }

    public function testARequestIsTriedOnEveryBrokerBeforeItIsGivenUp(): void
    {
        // The metadata answer names one broker, and nothing is scripted for it: connecting to it fails
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection(
                self::vector('metadata', 'metadata.response.v0.single-topic')
            ))
            ->install();

        $this->expectException(AllBrokersNotAvailableException::class);

        $this->adminClient()->findAllBrokers();
    }

    /**
     * Scripts the answers of the single broker of the cluster and installs the connections
     */
    private function scriptBroker(string ...$responses): BrokerConnection
    {
        $broker = new BrokerConnection(...$responses);
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection(
                self::vector('metadata', 'metadata.response.v0.single-topic')
            ))
            ->on(self::BROKER_ADDRESS, $broker)
            ->install();

        return $broker;
    }

    /**
     * Builds an admin client on the cluster that the scripted brokers answer for
     *
     * @param array<string, mixed> $overrides
     */
    private function adminClient(array $overrides = []): AdminClient
    {
        $configuration = $overrides + [
            ClientConfig::BOOTSTRAP_SERVERS         => [self::BOOTSTRAP_ADDRESS],
            ClientConfig::CLIENT_ID                 => 't10',
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 1000,
            ClientConfig::RETRY_BACKOFF_MS          => 1,
        ];

        return new AdminClient(Cluster::bootstrap($configuration), $configuration);
    }

    /**
     * Returns the frame of a request as a scripted broker records it, i.e. without the leading Size field
     */
    private static function requestFrame(AbstractRequest $request): string
    {
        return substr((string) $request, 4);
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
}
