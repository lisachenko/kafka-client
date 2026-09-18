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
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\CoordinatorLookup;
use Protocol\Kafka\Common\Errors\InvalidRequestException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\OffsetResetStrategy;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Protocol\Data\FindCoordinatorResponseCoordinator;
use Protocol\Kafka\Protocol\Data\OffsetFetchRequestGroup;
use Protocol\Kafka\Protocol\Data\OffsetFetchResponseGroup;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorRequest;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorResponse;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequest;
use Protocol\Kafka\Protocol\Request\OffsetFetchResponse;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;

/**
 * The batched group apis of Kafka 3.0 against the 3.9.2 KRaft node: FindCoordinator v4 and OffsetFetch v8.
 *
 * **FindCoordinator v4** (KIP-699) replaced the single `key` of the request with an array of `coordinator_keys`
 * and the five top-level fields of the answer with one entry per key, each with an error code of its own; the
 * coordinator *type* stays a single field, so a batch never mixes groups and transactional ids.
 * **OffsetFetch v8** did the same to the offsets: the group id and the topic array moved into a `groups` array and
 * the group-level error code moved into the entry of its group, which leaves the answer without a top-level error
 * code at all.
 *
 * Every topic, group and transactional id of this class is named `t3-30-…`, so that it can run next to the other
 * suites on the shared node.
 *
 * @see docs/protocol/3.9.md, sections "GroupCoordinator API (key 10, v0 to v4)" and "OffsetFetch API (key 9,
 *      v0 to v8)"
 */
#[CoversClass(GroupCoordinatorRequest::class)]
#[CoversClass(GroupCoordinatorResponse::class)]
#[CoversClass(FindCoordinatorResponseCoordinator::class)]
#[CoversClass(OffsetFetchRequest::class)]
#[CoversClass(OffsetFetchResponse::class)]
#[CoversClass(OffsetFetchRequestGroup::class)]
#[CoversClass(OffsetFetchResponseGroup::class)]
#[CoversClass(CoordinatorLookup::class)]
#[CoversClass(Client::class)]
#[CoversClass(AdminClient::class)]
final class BatchedGroupApiTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t3-30-batch';

    private const int REQUEST_TIMEOUT_MS = 30000;

    private const float TOPIC_TIMEOUT = 30.0;

    /**
     * The cluster is resolved once: every test of this class talks to the same node
     */
    private static ?Cluster $sharedCluster = null;

    private Client $client;

    private AdminClient $admin;

    /**
     * Topic of the current test, deleted again when the class ends
     */
    private string $topic;

    protected function setUp(): void
    {
        parent::setUp();

        $this->client = new Client($this->cluster(), $this->configuration());
        $this->admin  = new AdminClient($this->cluster(), $this->configuration());
        $this->topic  = self::uniqueTopicName('t3-30-batch');

        self::assertSame([$this->topic => null], $this->admin->createTopics([new NewTopic($this->topic, 1, 1)]));
        $this->awaitTopic($this->topic);
    }

    protected function tearDown(): void
    {
        if (isset($this->topic)) {
            try {
                $this->admin->deleteTopics([$this->topic]);
            } catch (KafkaException) {
                // A node that can not delete the topic right now must not fail the test that just passed
            }
        }

        parent::tearDown();
    }

    /**
     * The batched lookup answers one coordinator per key, and every key of this one-node cluster is the node 1
     */
    public function testSeveralGroupCoordinatorsAreLookedUpInOneRequest(): void
    {
        $groups = [self::uniqueGroupName(), self::uniqueGroupName(), self::uniqueGroupName()];

        $coordinators = $this->client->getGroupCoordinators($groups);

        self::assertSame($groups, array_keys($coordinators), 'every key of the batch is answered');
        foreach ($coordinators as $groupId => $node) {
            self::assertSame(
                $this->client->getGroupCoordinator($groupId)->nodeId,
                $node->nodeId,
                'the batched answer names the same coordinator as the single lookup of that group'
            );
        }
    }

    /**
     * The coordinator type is one field for the whole batch: the type 1 looks transactional ids up
     */
    public function testSeveralTransactionCoordinatorsAreLookedUpInOneRequest(): void
    {
        $ids = ['t3-30-batch-tx-' . bin2hex(random_bytes(4)), 't3-30-batch-tx-' . bin2hex(random_bytes(4))];

        $coordinators = $this->client->getTransactionCoordinators($ids);

        self::assertSame($ids, array_keys($coordinators));
        foreach ($coordinators as $transactionalId => $node) {
            self::assertSame(
                $this->client->getTransactionCoordinator($transactionalId)->nodeId,
                $node->nodeId,
                'looking an id up does not register it, so the answer is the same every time'
            );
        }
    }

    /**
     * A duplicate key is asked for once, and the answer is read by key
     */
    public function testADuplicateKeyIsLookedUpOnlyOnce(): void
    {
        $groupId = self::uniqueGroupName();

        $coordinators = $this->client->getGroupCoordinators([$groupId, $groupId]);

        self::assertSame([$groupId], array_keys($coordinators));
    }

    /**
     * An empty `coordinator_keys` array is a legal frame that the node answers with an empty `coordinators` array
     */
    public function testTheNodeAnswersAnEmptyCoordinatorBatch(): void
    {
        $stream = $this->connect([ClientConfig::REQUEST_TIMEOUT_MS => self::REQUEST_TIMEOUT_MS]);

        GroupCoordinatorRequest::forKeys([], GroupCoordinatorRequest::COORDINATOR_TYPE_GROUP, self::CLIENT_ID, 3101)
            ->writeTo($stream);
        $answer = GroupCoordinatorResponse::unpack($stream);

        self::assertSame(3101, $answer->getCorrelationId());
        self::assertSame([], $answer->coordinators, 'no key was asked for, so no coordinator is answered');
        self::assertSame(0, $answer->throttleTimeMs);
    }

    /**
     * A coordinator type this node does not serve is refused per key with the error code 42
     */
    public function testAnUnknownCoordinatorTypeIsRefusedPerKey(): void
    {
        $stream  = $this->connect([ClientConfig::REQUEST_TIMEOUT_MS => self::REQUEST_TIMEOUT_MS]);
        $groupId = self::uniqueGroupName();

        // The type 2 is the `SHARE` coordinator of KIP-932, which `KafkaApis.getCoordinator` @ 3.9.2 refuses
        // below the version 6 of the api; 99 is a type the enum does not know at all
        foreach ([2, 99] as $coordinatorType) {
            GroupCoordinatorRequest::forKeys([$groupId], $coordinatorType, self::CLIENT_ID, 3102)
                ->writeTo($stream);
            $entry = GroupCoordinatorResponse::unpack($stream)->coordinatorOf($groupId);

            self::assertSame(
                KafkaException::INVALID_REQUEST,
                $entry->errorCode,
                "the coordinator type {$coordinatorType} is answered 42, and the connection stays open"
            );
            self::assertSame(-1, $entry->nodeId, 'with the placeholder coordinator of an error');
            self::assertSame('', $entry->host);
            self::assertSame(-1, $entry->port);
        }
    }

    /**
     * The batched fetch answers one entry per group, each with its own topics and its own group-level error code
     */
    public function testTheCommittedOffsetsOfSeveralGroupsAreFetchedInOneRequest(): void
    {
        $committed = self::uniqueGroupName();
        $unknown   = self::uniqueGroupName();
        $node      = $this->client->getGroupCoordinator($committed);

        $this->client->commitGroupOffsets($node, $committed, '', -1, [$this->topic => [0 => 25]], -1);

        $offsets = $this->client->fetchOffsetsOfGroups(
            $node,
            [$committed => [$this->topic => [0]], $unknown => [$this->topic => [0]]]
        );

        self::assertSame([$committed, $unknown], array_keys($offsets), 'one entry per group of the request');
        self::assertSame([$this->topic => [0 => 25]], $offsets[$committed]);
        self::assertSame(
            [$this->topic => [0 => -1]],
            $offsets[$unknown],
            'a group the coordinator never heard of is not an error: the partition answers the offset -1'
        );
    }

    /**
     * A `null` value of the batch asks for every topic-partition that group committed, per group
     */
    public function testANullTopicArrayOfOneGroupOfTheBatchAsksForEveryTopicOfThatGroup(): void
    {
        $first  = self::uniqueGroupName();
        $second = self::uniqueGroupName();
        $node   = $this->client->getGroupCoordinator($first);

        $this->client->commitGroupOffsets($node, $first, '', -1, [$this->topic => [0 => 11]], -1);

        $offsets = $this->client->fetchOffsetsOfGroups($node, [$first => null, $second => null]);

        self::assertSame([$this->topic => [0 => 11]], $offsets[$first]);
        self::assertSame([], $offsets[$second], 'a group without committed offsets answers no topic at all');
    }

    /**
     * The single-group method keeps its signature and sends the very same version 8 frame with one group
     */
    public function testTheSingleGroupFetchStillWorksOnTheBatchedVersion(): void
    {
        $groupId = self::uniqueGroupName();
        $node    = $this->client->getGroupCoordinator($groupId);

        $this->client->commitGroupOffsets($node, $groupId, '', -1, [$this->topic => [0 => 7]], -1);

        self::assertSame(
            [$this->topic => [0 => 7]],
            $this->client->fetchGroupOffsets($node, $groupId, [$this->topic => [0]])
        );
    }

    /**
     * The administrative half of the batch: the groups are split by coordinator and answered per group
     */
    public function testTheAdminClientListsTheOffsetsOfSeveralGroupsAtOnce(): void
    {
        $first  = self::uniqueGroupName();
        $second = self::uniqueGroupName();
        $node   = $this->client->getGroupCoordinator($first);

        $this->client->commitGroupOffsets($node, $first, '', -1, [$this->topic => [0 => 3]], -1);
        $this->client->commitGroupOffsets($node, $second, '', -1, [$this->topic => [0 => 4]], -1);

        $offsets = $this->admin->listConsumerGroupOffsets([$first => null, $second => [$this->topic => [0]]]);

        self::assertSame([$first, $second], array_keys($offsets));
        self::assertSame(3, $offsets[$first][$this->topic]->partitions[0]->offset);
        self::assertSame(4, $offsets[$second][$this->topic]->partitions[0]->offset);
        self::assertEquals(
            $offsets[$first],
            $this->admin->listGroupOffsets($first),
            'the batched answer of a group is the answer the single-group method gives'
        );
    }

    public function testAnEmptyBatchOfTheAdminClientSendsNothingAtAll(): void
    {
        self::assertSame([], $this->admin->listConsumerGroupOffsets([]));
    }

    /**
     * The one frame this client never puts on the wire: the node answers `groups = []` with nothing at all and
     * leaves the connection owing an answer that never comes
     */
    public function testAnEmptyGroupBatchIsRefusedBeforeItReachesTheNode(): void
    {
        $this->expectException(InvalidRequestException::class);

        OffsetFetchRequest::forGroups([], self::CLIENT_ID, 3103);
    }

    /**
     * Waits until the fresh partition has a leader that answers, which a brand new topic needs a moment for
     */
    private function awaitTopic(string $topic): void
    {
        $deadline = microtime(true) + self::TOPIC_TIMEOUT;
        do {
            try {
                $this->cluster()->reload();
                $this->admin->listOffsets([$topic => [0]], OffsetsRequest::LATEST);

                return;
            } catch (KafkaException $exception) {
                if (microtime(true) >= $deadline) {
                    throw $exception;
                }
                usleep(200000);
            }
        } while (true);
    }

    private function cluster(): Cluster
    {
        return self::$sharedCluster ??= Cluster::bootstrap($this->configuration());
    }

    /**
     * @return array<string, mixed> Client configuration of this test class
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ClientConfig::RETRY_BACKOFF_MS          => 250,
            ClientConfig::REQUEST_TIMEOUT_MS        => self::REQUEST_TIMEOUT_MS,

            ProducerConfig::ACKS                    => ProducerConfig::ACKS_ALL,

            ConsumerConfig::AUTO_OFFSET_RESET       => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT      => false,
            ConsumerConfig::FETCH_MAX_WAIT_MS       => 250,
        ] + ConsumerConfig::getDefaultConfiguration();
    }

    /**
     * Builds a consumer group name that is unique for this test run
     */
    private static function uniqueGroupName(): string
    {
        return 't3-30-batch-group-' . bin2hex(random_bytes(6));
    }
}
