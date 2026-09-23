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
use Protocol\Kafka\Common\Errors\RetriableException;
use Protocol\Kafka\Common\TopicMetadata;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;

/**
 * Exercises the AdminClient against the Kafka 3.9.2 KRaft node of this line.
 *
 * The three tests of the ControlledShutdown api that this class carried up to the 2.x line are gone with the
 * `controlledShutdown()` method of the admin client: key 7 is `zkBroker`-only, a KRaft node does not list it on a
 * client listener and closes the connection for every version of it - see the "(3.x)" note of the section below.
 *
 * @see docs/protocol/4.3.md, section "ControlledShutdown API (key 7, v0 to v3)"
 * @see docs/protocol/4.3.md, section "Metadata API (key 3, v0 to v13)"
 */
#[CoversClass(AdminClient::class)]
final class AdminApiTest extends IntegrationTestCase
{
    /**
     * Topic of this test class, created once: the node is shared with the other suites, and creating a topic means
     * a round trip through the KRaft controller and a leader election
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

    /**
     * Removes the one topic of this class again: the container is shared and outlives the suite
     *
     * The topic is created once for the whole class, so it is deleted once as well - a topic per test would be
     * one leader election per test on a container that already carries the topics of every other suite.
     */
    public static function tearDownAfterClass(): void
    {
        if (self::$topic === null) {
            return;
        }

        $configuration = [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => 't10-admin',
            ClientConfig::REQUEST_TIMEOUT_MS        => 10000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
        ];
        new AdminClient(Cluster::bootstrap($configuration), $configuration)->deleteTopics([self::$topic]);

        self::$topic = null;
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

        $latest   = $this->listOffsetsOfALeaderThatIsThere($topic, $partitions, OffsetsRequest::LATEST);
        $earliest = $this->listOffsetsOfALeaderThatIsThere($topic, $partitions, OffsetsRequest::EARLIEST);

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
     * Lists the offsets of a freshly created topic, retrying while the node is still moving its leadership around
     *
     * A KRaft node answers a topic that the controller has just created before every broker has replayed the
     * metadata record of it, so the leader of a partition can be elsewhere for a moment and the Offsets api - which
     * only its leader serves - answers **6** `NotLeaderForPartition` (and **3** while the partition is not there at
     * all). Both are retriable and both pass within a few dozen milliseconds; the metadata of the client is
     * reloaded between the attempts, because it is the stale half of the race.
     *
     * @param list<int> $partitions Partitions of the topic to list
     *
     * @return array<string, array<int, int>> Offsets as topic => partition => offset
     */
    private function listOffsetsOfALeaderThatIsThere(string $topic, array $partitions, int $time): array
    {
        $deadline = microtime(true) + 30.0;
        do {
            try {
                return $this->admin->listOffsets([$topic => $partitions], $time);
            } catch (RetriableException $exception) {
                $last = $exception;
                usleep(200000);
                $this->cluster->reload();
            }
        } while (microtime(true) < $deadline);

        throw $last;
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
