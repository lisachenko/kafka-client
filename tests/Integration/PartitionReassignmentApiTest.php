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
use Protocol\Kafka\Admin\NewPartitionReassignment;
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Admin\PartitionReassignment;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\InvalidReplicaAssignmentException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\NoReassignmentInProgressException;
use Protocol\Kafka\Common\Errors\UnknownTopicOrPartitionException;
use Protocol\Kafka\Protocol\Request\AlterPartitionReassignmentsRequest;
use Protocol\Kafka\Protocol\Request\AlterPartitionReassignmentsResponse;
use Protocol\Kafka\Protocol\Request\ListPartitionReassignmentsRequest;
use Protocol\Kafka\Protocol\Request\ListPartitionReassignmentsResponse;
use Throwable;

/**
 * Exercises the two partition-reassignment apis of KIP-455 - AlterPartitionReassignments (45) and
 * ListPartitionReassignments (46) - against a real Kafka 2.8.2 broker.
 *
 * Kafka 2.4 added both, and with them the **first flexible frames this package sends to an admin api**: the request
 * header v2, compact strings and arrays and a tagged-field section at the end of every structure. A broker that
 * cannot parse such a frame does not answer at all - it closes the connection - so every answer here is also a
 * statement that the compact encoding of the engine is right.
 *
 * **What a one-broker cluster cannot show is a reassignment in progress.** Every replica of every partition is
 * already on the only broker, so the controller completes a reassignment before it answers the request that
 * submitted it, and {@see AdminClient::listPartitionReassignments()} always answers the empty list. The shape of a
 * partition in flight is documented from the sources instead; what is measured here is every error the api answers,
 * and the empty list itself.
 *
 * The topic of this class is created by the class and never touched by another suite, and **no test ever sends a
 * null topic array**: that would ask about - or reassign - the partitions of every other suite on the shared
 * container.
 *
 * @see docs/protocol/2.8.md, sections "AlterPartitionReassignments API (key 45, v0)" and
 *      "ListPartitionReassignments API (key 46, v0)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(AlterPartitionReassignmentsRequest::class)]
#[CoversClass(AlterPartitionReassignmentsResponse::class)]
#[CoversClass(ListPartitionReassignmentsRequest::class)]
#[CoversClass(ListPartitionReassignmentsResponse::class)]
#[CoversClass(NewPartitionReassignment::class)]
#[CoversClass(PartitionReassignment::class)]
final class PartitionReassignmentApiTest extends IntegrationTestCase
{
    /**
     * A broker id this container does not have, so that a replica set can name a broker that is not alive
     */
    private const int UNKNOWN_BROKER_ID = 7;

    /**
     * Topic of this test class, created once: it is the only one any of these tests ever names
     */
    private static ?string $topic = null;

    private AdminClient $admin;

    protected function setUp(): void
    {
        parent::setUp();

        $configuration = $this->configuration();
        $this->admin   = new AdminClient(Cluster::bootstrap($configuration), $configuration);
    }

    /**
     * Removes the topic of this class from the shared container, which several suites work on at once
     */
    public static function tearDownAfterClass(): void
    {
        $topic       = self::$topic;
        self::$topic = null;

        if ($topic === null || self::bootstrapServers() === []) {
            return;
        }

        try {
            $configuration = [
                ClientConfig::BOOTSTRAP_SERVERS  => ['tcp://' . self::firstBootstrapServer()],
                ClientConfig::CLIENT_ID          => 'kafka-client-t1-reassign',
                ClientConfig::REQUEST_TIMEOUT_MS => 20000,
            ];
            new AdminClient(Cluster::bootstrap($configuration), $configuration)->deleteTopics([$topic]);
        } catch (Throwable) {
            // A broker that is gone or busy is not a failure of these tests - the topic is named uniquely
        }
    }

    /**
     * A reassignment to the replica set a partition already has is accepted, and nothing moves
     *
     * The one-broker cluster makes this the only successful reassignment that can be submitted here, and it is a
     * real one as far as the controller is concerned: it registers the target assignment, notices that there is
     * nothing to add and nothing to remove, and completes it before it answers.
     */
    public function testAReassignmentToTheCurrentReplicaSetIsAccepted(): void
    {
        $topic    = $this->topic();
        $brokerId = array_key_first($this->admin->findAllBrokers());

        $result = $this->admin->alterPartitionReassignments([
            $topic => [0 => new NewPartitionReassignment([$brokerId])],
        ]);

        self::assertSame([$topic => [0 => null]], $result, 'the controller accepted it');
        self::assertSame([], $this->admin->listPartitionReassignments([$topic => [0]]), 'and completed it at once');
    }

    /**
     * A cancellation without a reassignment in progress is the error code 85, which Kafka 2.4 added for it
     */
    public function testACancellationWithNothingInProgressIsRefusedWithTheCode85(): void
    {
        $topic = $this->topic();

        $result = $this->admin->alterPartitionReassignments([$topic => [0 => null]]);

        $error = $result[$topic][0];

        self::assertInstanceOf(NoReassignmentInProgressException::class, $error);
        self::assertSame(KafkaException::NO_REASSIGNMENT_IN_PROGRESS, $error->getCode());
        self::assertStringContainsString('No partition reassignment is in progress.', $error->getMessage());
    }

    /**
     * A replica set that names a broker the cluster does not have is refused with 39, and the message says so
     */
    public function testAReplicaSetWithABrokerThatIsNotAliveIsRefusedWithTheCode39(): void
    {
        $topic    = $this->topic();
        $brokerId = array_key_first($this->admin->findAllBrokers());

        $result = $this->admin->alterPartitionReassignments([
            $topic => [0 => new NewPartitionReassignment([$brokerId, self::UNKNOWN_BROKER_ID])],
        ]);

        $error = $result[$topic][0];

        self::assertInstanceOf(InvalidReplicaAssignmentException::class, $error);
        self::assertSame(KafkaException::INVALID_REPLICA_ASSIGNMENT, $error->getCode());
        self::assertStringContainsString(
            'Replica assignment has brokers that are not alive',
            $error->getMessage(),
            'the controller names the replica list it was given and the brokers it knows'
        );
    }

    /**
     * A topic the cluster does not have is answered per partition, with the code 3 and not with a top-level error
     */
    public function testAnUnknownTopicIsRefusedPerPartitionWithTheCode3(): void
    {
        $unknown = self::uniqueTopicName('t1-reassign-unknown');

        $result = $this->admin->alterPartitionReassignments([$unknown => [0 => [0]]]);

        $error = $result[$unknown][0];

        self::assertInstanceOf(UnknownTopicOrPartitionException::class, $error);
        self::assertSame(KafkaException::UNKNOWN_TOPIC_OR_PARTITION, $error->getCode());
        self::assertStringContainsString('The partition does not exist.', $error->getMessage());
    }

    /**
     * A partition the topic does not have answers 85 for a cancellation, because the map is consulted first
     *
     * `KafkaController.alterPartitionReassignments` @ 2.8.2 looks a cancellation up in the reassignments it has in
     * flight before it asks whether the partition exists at all, so a cancellation of a partition that never
     * existed is "no reassignment in progress" and not "unknown topic or partition".
     */
    public function testACancellationOfAPartitionThatDoesNotExistIsAlso85(): void
    {
        $topic = $this->topic();

        $result = $this->admin->alterPartitionReassignments([$topic => [42 => null]]);

        self::assertInstanceOf(NoReassignmentInProgressException::class, $result[$topic][42]);
    }

    /**
     * Several partitions of one request are answered one by one, and one refusal does not spoil the others
     */
    public function testEveryPartitionOfARequestCarriesItsOwnError(): void
    {
        $topic    = $this->topic();
        $brokerId = array_key_first($this->admin->findAllBrokers());

        $result = $this->admin->alterPartitionReassignments([
            $topic => [
                0 => new NewPartitionReassignment([$brokerId]),
                1 => new NewPartitionReassignment([$brokerId, self::UNKNOWN_BROKER_ID]),
                2 => null,
            ],
        ]);

        self::assertNull($result[$topic][0], 'the accepted one');
        self::assertInstanceOf(InvalidReplicaAssignmentException::class, $result[$topic][1]);
        self::assertInstanceOf(NoReassignmentInProgressException::class, $result[$topic][2]);
    }

    /**
     * The list api answers the empty list, and an unknown topic is not an error but simply absent
     */
    public function testTheListOfReassignmentsIsEmptyWhileNothingIsMoving(): void
    {
        $topic   = $this->topic();
        $unknown = self::uniqueTopicName('t1-reassign-unknown');

        self::assertSame([], $this->admin->listPartitionReassignments([$topic => [0, 1, 2]]));
        self::assertSame(
            [],
            $this->admin->listPartitionReassignments([$unknown => [0]]),
            'a topic that does not exist is not reported as an error'
        );
        self::assertSame(
            [],
            $this->admin->listPartitionReassignments([]),
            'and the empty topic array asks about nothing at all'
        );
    }

    /**
     * Returns the topic of this test class, created by the first test that needs it
     */
    private function topic(): string
    {
        if (self::$topic !== null) {
            return self::$topic;
        }

        $topic = self::uniqueTopicName('t1-reassign');
        $this->admin->createTopics([new NewTopic($topic, 3, 1)]);

        return self::$topic = $topic;
    }

    /**
     * @return array<string, mixed> Client configuration for this test class
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => 'kafka-client-t1-reassign',
            ClientConfig::REQUEST_TIMEOUT_MS        => 20000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
        ];
    }
}
