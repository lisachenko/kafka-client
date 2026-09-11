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
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\TopicMetadata;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\ApiVersionsResponseMetadata;
use Protocol\Kafka\Protocol\Request\ApiVersionsRequest;
use Protocol\Kafka\Protocol\Request\ApiVersionsResponse;
use Protocol\Kafka\Protocol\Request\ControlledShutdownRequest;
use Protocol\Kafka\Protocol\Request\ControlledShutdownRequestV0;
use Protocol\Kafka\Protocol\Request\ControlledShutdownResponse;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;

/**
 * Exercises the AdminClient against a real Kafka 2.8.2 broker.
 *
 * @see docs/protocol/2.8.md, section "ControlledShutdown API (key 7, v0 to v2)"
 * @see docs/protocol/2.8.md, section "ApiVersions API (key 18, v0 to v2)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(ApiVersionsRequest::class)]
#[CoversClass(ApiVersionsResponse::class)]
#[CoversClass(ApiVersionsResponseMetadata::class)]
#[CoversClass(ControlledShutdownRequest::class)]
#[CoversClass(ControlledShutdownRequestV0::class)]
#[CoversClass(ControlledShutdownResponse::class)]
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

    public function testDescribeTopicsDoesNotCreateAnUnknownTopicAnyMore(): void
    {
        // Version 4 of the Metadata api (Kafka 0.11) added `allow_auto_topic_creation`, and the AdminClient sends
        // it as FALSE: an administrator must be able to ask about a topic without bringing it into existence. The
        // answer is therefore the error code 3, and `kafka-topics.sh --list` never sees the name afterwards - where
        // every version below 4 would have created the topic here and answered 5 (LeaderNotAvailable).
        $absent = self::uniqueTopicName('t10-admin-absent');

        $answer = $this->admin->describeTopics([$absent])[$absent];

        self::assertSame($absent, $answer->topic);
        self::assertSame(KafkaException::UNKNOWN_TOPIC_OR_PARTITION, $answer->topicErrorCode);
        self::assertSame([], $answer->partitions);
        self::assertNotContains($absent, $this->admin->listTopics(), 'asking about a topic must not create it');
    }

    public function testDescribeTopicsReportsThePartitionsOfATopicThatExists(): void
    {
        $topic    = $this->topic();
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
            self::assertSame(0, $latest[$topic][$partition], 'nothing was produced into the topic yet');
            self::assertSame(0, $earliest[$topic][$partition]);
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
            self::assertSame(KafkaException::NO_ERROR, $partition->errorCode, 'the kafka storage reports no error');
        }
    }

    public function testControlledShutdownOfAnUnknownBrokerIsRefused(): void
    {
        // The controller throws BrokerNotAvailableException and ControlledShutdownRequest.handleError() @ 0.9.0.1
        // maps e.getClass(), so the code 8 reaches the wire. A 0.8.2.2 broker mapped e.getCause(), which is null for
        // a directly thrown exception, and answered -1 (Unknown) instead.
        $this->expectException(KafkaException::class);
        $this->expectExceptionCode(KafkaException::BROKER_NOT_AVAILABLE);

        $this->admin->controlledShutdown(self::UNKNOWN_BROKER_ID);
    }

    public function testTheBrokerAnnouncesEveryVersionOfControlledShutdown(): void
    {
        // A 0.9 to 0.11 broker reported `minVersion = 1` for key 7: version 0 uses a request header without a client
        // id, which the Java client of those releases could not build, so the protocol retired it. Kafka 1.0 gave
        // `RequestHeader` a schema of its own for that one frame (`CONTROLLED_SHUTDOWN_V0_SCHEMA`) and moved the api
        // to the Java schemas altogether, so a 1.1.1 broker announced **v0 and v1** again. A 2.8.2 broker serves
        // two versions more: the **v2** of KIP-380, which Kafka 2.2 added for the `broker_epoch`, and the flexible
        // **v3** of Kafka 2.4, which this line does not send yet.
        $nodes       = $this->cluster->nodes();
        $apiVersions = $this->admin->getApiVersions(reset($nodes));

        self::assertSame(0, $apiVersions[ApiKeys::CONTROLLED_SHUTDOWN]->minVersion);
        self::assertSame(3, $apiVersions[ApiKeys::CONTROLLED_SHUTDOWN]->maxVersion);
        self::assertSame(
            2,
            ControlledShutdownRequest::VERSION,
            'and the client sends the highest non-flexible one of them'
        );
    }

    public function testBothVersionsOfControlledShutdownAreStillServedByTheBroker(): void
    {
        // Both announced versions really are answered. Up to 0.11 this test proved something else: key 7 was the
        // last api a broker parsed with its Scala class, which never validated the version, so v0 was answered
        // although the table did not contain it - and so was any version above 1. On this line the api is an
        // ordinary Java-schema api and the first version above its table closes the connection like every other
        // unknown version (`ApiVersionProbeTest::testTheBrokerClosesTheConnectionForAVersionAboveTheTable`). The
        // AdminClient sends v1; v0 is kept for the 0.8/0.9 lines and their vectors.
        $stream = $this->connect();

        new ControlledShutdownRequestV0(self::UNKNOWN_BROKER_ID, 4200)->writeTo($stream);
        $versionZero = ControlledShutdownResponse::unpack($stream);

        new ControlledShutdownRequest(
            self::UNKNOWN_BROKER_ID,
            ControlledShutdownRequest::UNKNOWN_BROKER_EPOCH,
            't10-admin',
            4201
        )->writeTo($stream);
        $versionOne = ControlledShutdownResponse::unpack($stream);

        self::assertSame(4200, $versionZero->getCorrelationId());
        self::assertSame(KafkaException::BROKER_NOT_AVAILABLE, $versionZero->errorCode);
        self::assertSame([], $versionZero->remainingTopicPartitions);

        self::assertSame(4201, $versionOne->getCorrelationId());
        self::assertSame(KafkaException::BROKER_NOT_AVAILABLE, $versionOne->errorCode);
        self::assertSame([], $versionOne->remainingTopicPartitions);
    }

    /**
     * Returns the topic of this test class, created by the first test that needs it
     */
    private function topic(): string
    {
        if (self::$topic !== null) {
            return self::$topic;
        }

        $topic = self::uniqueTopicName('t10-admin');
        // The topic has to be created explicitly now: describeTopics() asks with `allow_auto_topic_creation = false`
        $this->admin->createTopics([new NewTopic($topic, 3, 1)]);

        return self::$topic = $topic;
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
