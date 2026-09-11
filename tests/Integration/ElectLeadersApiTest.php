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
use Protocol\Kafka\Common\Errors\UnknownTopicOrPartitionException;
use Protocol\Kafka\Common\Errors\UnsupportedVersionException;
use Protocol\Kafka\Common\TopicPartition;
use Protocol\Kafka\Protocol\Data\ElectLeadersRequestTopicPartitions;
use Protocol\Kafka\Protocol\Data\ElectLeadersResponsePartitionResult;
use Protocol\Kafka\Protocol\Data\ElectLeadersResponseReplicaElectionResult;
use Protocol\Kafka\Protocol\Request\ElectLeadersRequest;
use Protocol\Kafka\Protocol\Request\ElectLeadersResponse;

/**
 * Exercises the ElectLeaders api (key 43, v0) against a real Kafka 2.8.2 broker.
 *
 * KIP-183 added the api in Kafka 2.2 as **ElectPreferredLeaders**: it asks the active controller to move the
 * leadership of a partition back to its preferred replica. On a **one-broker** container every partition is led by
 * its only replica, which is always the preferred one, so the election this suite can really observe is the one
 * that is **not needed** — the 84 of a partition that already has the right leader — and the refusals of the api.
 * That is what a client sees on a healthy cluster, and it is the only shape a shared container can offer: an
 * election with a **null** topic array would elect for every partition of every other suite, and no test of this
 * repository ever sends one (the document describes what it answers, measured once).
 *
 * @see docs/protocol/2.8.md, section "ElectLeaders API (key 43, v0)"
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

        self::assertSame([$topic], array_keys($result), 'the answer is grouped by topic');
        self::assertSame([0, 1], array_keys($result[$topic]), 'and every requested partition is in it');
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
        self::assertSame('The partition does not exist.', $result[$absent][0]->getContext()['error']);
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
    }

    /**
     * The unclean election of KIP-460 needs the version 1 of the api, and this line sends the version 0
     */
    public function testAnUncleanElectionIsRefusedByTheClientAndNeverSent(): void
    {
        $topic = $this->topic('unclean');

        try {
            $this->admin->electLeaders(ElectionType::UNCLEAN, [$topic => [0]]);
            self::fail('an unclean election has to be refused by a client that sends the version 0');
        } catch (UnsupportedVersionException $exception) {
            self::assertSame('UNCLEAN', $exception->getContext()['electionType']);
            self::assertSame(
                'API Version 0 only supports PREFERRED election type',
                $exception->getContext()['error'],
                'the message of `ElectLeadersRequest.Builder.toRequestData` @ 2.8.2'
            );
        }

        self::assertSame(0, ElectLeadersRequest::VERSION, 'the version this line sends');
    }

    /**
     * Creates a topic of two partitions for this test and waits until the cluster serves it
     */
    private function topic(string $purpose): string
    {
        $topic                 = self::uniqueTopicName("t4-elect-{$purpose}");
        $this->createdTopics[] = $topic;

        self::assertSame([$topic => null], $this->admin->createTopics([new NewTopic($topic, 2, 1)]));

        // A fresh topic is not in the metadata cache of the controller for a moment, and an election of a
        // partition it does not know yet would be the 3 of an unknown partition
        $deadline = microtime(true) + 30.0;
        do {
            $this->cluster->reload();
            $partitions = $this->cluster->partitionsForTopic($topic);
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
