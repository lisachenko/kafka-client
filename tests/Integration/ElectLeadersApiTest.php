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
use Protocol\Kafka\Admin\ElectionType;
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\ElectionNotNeededException;
use Protocol\Kafka\Common\Errors\InvalidTopicException;
use Protocol\Kafka\Common\Errors\UnknownTopicOrPartitionException;
use Protocol\Kafka\Common\Errors\UnsupportedVersionException;
use Protocol\Kafka\Common\TopicPartition;
use Protocol\Kafka\Protocol\Data\ElectLeadersRequestTopicPartitions;
use Protocol\Kafka\Protocol\Data\ElectLeadersResponsePartitionResult;
use Protocol\Kafka\Protocol\Data\ElectLeadersResponseReplicaElectionResult;
use Protocol\Kafka\Protocol\Request\ElectLeadersRequest;
use Protocol\Kafka\Protocol\Request\ElectLeadersResponse;

/**
 * Exercises the ElectLeaders api (key 43, v0) against the Kafka 3.9.2 KRaft node of this line.
 *
 * KIP-183 added the api in Kafka 2.2 as **ElectPreferredLeaders**: it asks the active controller to move the
 * leadership of a partition back to its preferred replica. On a **one-broker** container every partition is led by
 * its only replica, which is always the preferred one, so the election this suite can really observe is the one
 * that is **not needed** — the 84 of a partition that already has the right leader — and the refusals of the api.
 * That is what a client sees on a healthy cluster, and it is the only shape a shared container can offer: an
 * election with a **null** topic array would elect for every partition of every other suite, and no test of this
 * repository ever sends one (the document describes what it answers, measured once).
 *
 * The election is run by the **KRaft controller** here: `ReplicationControlManager.electLeader` @ 3.9.2 answers
 * `No such topic as <topic>` and `No such partition as <topic>-<partition>` where `KafkaController` @ 2.8.2
 * answered the generic message of the error code 3. The code itself is unchanged, and so is the 84 of a partition
 * that needs no election.
 *
 * @see docs/protocol/3.9.md, section "ElectLeaders API (key 43, v0 to v2)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(Client::class)]
#[CoversClass(ElectionType::class)]
#[CoversClass(ElectLeadersRequest::class)]
#[CoversClass(ElectLeadersResponse::class)]
#[CoversClass(ElectLeadersRequestTopicPartitions::class)]
#[CoversClass(ElectLeadersResponseReplicaElectionResult::class)]
#[CoversClass(ElectLeadersResponsePartitionResult::class)]
final class ElectLeadersApiTest extends IntegrationTestCase
{
    /**
     * Client id of every request of this class
     */
    private const string CLIENT_ID = 't4-elect';

    private Cluster $cluster;

    private AdminClient $admin;

    /**
     * Topics this test class created, deleted again after every test
     *
     * @var list<string>
     */
    private array $createdTopics = [];

    protected function setUp(): void
    {
        parent::setUp();

        $this->cluster = Cluster::bootstrap($this->configuration());
        $this->admin   = new AdminClient($this->cluster, $this->configuration());
    }

    protected function tearDown(): void
    {
        if ($this->createdTopics !== []) {
            $this->admin->deleteTopics($this->createdTopics);
            $this->createdTopics = [];
        }
    }

    /**
     * A partition whose leader is already its preferred replica is answered with 84, per partition
     */
    public function testAPartitionThatNeedsNoElectionIsAnsweredWithEightyFour(): void
    {
        $topic = $this->topic('not-needed');

        $result = $this->admin->electLeaders(ElectionType::PREFERRED, [$topic => [0, 1]]);

        $partitions = array_keys($result[$topic]);
        sort($partitions);

        self::assertSame([$topic], array_keys($result), 'the answer is grouped by topic');
        self::assertSame(
            [0, 1],
            $partitions,
            'and every requested partition is in it - in the order the controller walked its map, not the one of'
            . ' the request'
        );
        foreach ($result[$topic] as $partition => $error) {
            self::assertInstanceOf(
                ElectionNotNeededException::class,
                $error,
                "the partition {$partition} is led by its only replica, which is the preferred one"
            );
            self::assertSame(
                'Leader election not needed for topic partition.',
                $error->getContext()['error'],
                'and the controller says so in the message of the partition result'
            );
        }
    }

    /**
     * The very same answer for a {@see TopicPartition} list, which is the other notation of the argument
     */
    public function testThePartitionsMayBeGivenAsTopicPartitionObjects(): void
    {
        $topic = $this->topic('objects');

        $result = $this->admin->electLeaders(ElectionType::PREFERRED, [new TopicPartition($topic, 0)]);

        self::assertInstanceOf(ElectionNotNeededException::class, $result[$topic][0]);
    }

    /**
     * A topic the cluster does not have is 3 with the message of the controller - not a topic that gets created
     */
    public function testAPartitionOfAnUnknownTopicIsUnknownTopicOrPartition(): void
    {
        $absent = self::uniqueTopicName('t4-elect-never-created');

        $result = $this->admin->electLeaders(ElectionType::PREFERRED, [$absent => [0]]);

        self::assertInstanceOf(UnknownTopicOrPartitionException::class, $result[$absent][0]);
        self::assertSame("No such topic as {$absent}", $result[$absent][0]->getContext()['error']);
        self::assertNotContains($absent, $this->admin->listTopics(), 'and the topic was not created either');
    }

    /**
     * A partition index the topic does not have is the same 3, for the same reason
     */
    public function testAPartitionTheTopicDoesNotHaveIsUnknownTopicOrPartition(): void
    {
        $topic = $this->topic('no-partition');

        $result = $this->admin->electLeaders(ElectionType::PREFERRED, [$topic => [7]]);

        self::assertInstanceOf(UnknownTopicOrPartitionException::class, $result[$topic][7]);
        self::assertSame(
            "No such partition as {$topic}-7",
            $result[$topic][7]->getContext()['error'],
            'the controller names the partition it could not find'
        );
    }

    /**
     * The **unclean** election of KIP-460 is sendable from the version 1 - and answers the same 84 here
     *
     * `ReplicationControlManager.electLeader` @ 3.9.2 answers 84 for an unclean election as soon as
     * `partition.hasLeader()` - as `KafkaController.processReplicaLeaderElection` @ 2.8.2 did with
     * `currentLeader == LeaderAndIsr.NoLeader || !liveBrokerIds.contains(currentLeader)` - and the one broker of
     * the container is the leader of everything it hosts, so the partitions of this suite are never electable in
     * that sense. What the test proves is that the type byte travels and that the controller does NOT elect where
     * nothing has failed.
     */
    public function testAnUncleanElectionOfAHealthyPartitionIsElectionNotNeededAsWell(): void
    {
        $topic = $this->topic('unclean');

        $result = $this->admin->electLeaders(ElectionType::UNCLEAN, [$topic => [0]]);

        self::assertInstanceOf(
            ElectionNotNeededException::class,
            $result[$topic][0],
            'the partition still has a live leader, so neither election type has anything to do'
        );
        self::assertSame(2, ElectLeadersRequest::VERSION, 'the flexible version of KIP-482, which this line sends');
    }

    /**
     * An unclean election of a topic the cluster does not have is the same 3 as a preferred one
     */
    public function testAnUncleanElectionOfAnUnknownTopicIsUnknownTopicOrPartition(): void
    {
        $absent = self::uniqueTopicName('t4-elect-unclean-never-created');

        $result = $this->admin->electLeaders(ElectionType::UNCLEAN, [$absent => [0]]);

        self::assertInstanceOf(UnknownTopicOrPartitionException::class, $result[$absent][0]);
        self::assertSame("No such topic as {$absent}", $result[$absent][0]->getContext()['error']);
    }

    /**
     * An election type that is neither of the two of KIP-460 never reaches the broker
     */
    public function testAnUnknownElectionTypeIsRefusedByTheClient(): void
    {
        $this->expectException(UnsupportedVersionException::class);

        $this->admin->electLeaders(7, ['no-such-topic' => [0]]);
    }

    /**
     * Creates a topic of two partitions for this test and waits until the cluster serves it
     */
    private function topic(string $purpose): string
    {
        $topic                 = self::uniqueTopicName("t4-elect-{$purpose}");
        $this->createdTopics[] = $topic;

        self::assertSame([$topic => null], $this->admin->createTopics([new NewTopic($topic, 2, 1)]));

        // A fresh topic is not in the metadata cache of the node for a moment, and an election of a partition it
        // does not know yet would be the 3 of an unknown partition. `Cluster::partitionsForTopic()` throws for a
        // topic the metadata answer does not carry, which is exactly that moment, so the exception is part of the
        // wait: since the io fix of #166 the create answer comes back fast enough for the first read to miss it.
        $deadline = microtime(true) + 30.0;
        do {
            $this->cluster->reload();
            try {
                $partitions = $this->cluster->partitionsForTopic($topic);
            } catch (InvalidTopicException) {
                $partitions = [];
            }
            if (count($partitions) === 2) {
                return $topic;
            }
            usleep(200000);
        } while (microtime(true) < $deadline);

        self::fail("The topic {$topic} did not become servable in time");
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
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::REQUEST_TIMEOUT_MS        => 40000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
        ];
    }
}
