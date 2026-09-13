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
use Protocol\Kafka\Common\Errors\GroupIdNotFoundException;
use Protocol\Kafka\Common\Errors\GroupNotEmptyException;
use Protocol\Kafka\Common\Errors\GroupSubscribedToTopicException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\MemberIdRequiredException;
use Protocol\Kafka\Common\Errors\RebalanceInProgressException;
use Protocol\Kafka\Common\Errors\UnknownMemberIdException;
use Protocol\Kafka\Common\Errors\UnknownTopicOrPartitionException;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Common\TopicPartition;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\Subscription;
use Protocol\Kafka\Protocol\Data\DescribeGroupResponseMetadata;
use Protocol\Kafka\Protocol\Request\JoinGroupResponse;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequest;
use Protocol\Kafka\Protocol\Request\OffsetDeleteRequest;
use Protocol\Kafka\Protocol\Request\OffsetDeleteResponse;
use Throwable;

/**
 * Exercises the OffsetDelete api of KIP-496 (key 47, v0) against a real Kafka 2.8.2 broker.
 *
 * Kafka 2.4 added it so that a group that keeps running can be made to forget the committed offset of a single
 * partition - DeleteGroups can only throw the whole group away. What the coordinator allows depends on the state of
 * the group, and only part of it is reported per partition: a group error answers no topic at all, which is why the
 * client throws it.
 *
 * Every group and every topic of this class carries a `t1-` prefix of its own and is created by the class itself.
 *
 * @see docs/protocol/2.8.md, section "OffsetDelete API (key 47, v0)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(Client::class)]
#[CoversClass(OffsetDeleteRequest::class)]
#[CoversClass(OffsetDeleteResponse::class)]
final class OffsetDeleteApiTest extends IntegrationTestCase
{
    private const int SESSION_TIMEOUT_MS = 30000;

    /**
     * Topic of this class, created once by {@see self::topic()}
     */
    private static ?string $topic = null;

    /**
     * A second topic, which no group of this class ever subscribes to
     */
    private static ?string $otherTopic = null;

    private AdminClient $admin;

    private Client $client;

    protected function setUp(): void
    {
        parent::setUp();

        $configuration = $this->configuration();
        $cluster       = Cluster::bootstrap($configuration);
        $this->admin   = new AdminClient($cluster, $configuration);
        $this->client  = new Client($cluster, $configuration);
    }

    /**
     * Removes the two topics of this class from the shared container, which several suites work on at once
     */
    public static function tearDownAfterClass(): void
    {
        $topics = array_values(array_filter([self::$topic, self::$otherTopic]));
        self::$topic = self::$otherTopic = null;

        if ($topics === [] || self::bootstrapServers() === []) {
            return;
        }

        try {
            $configuration = [
                ClientConfig::BOOTSTRAP_SERVERS  => ['tcp://' . self::firstBootstrapServer()],
                ClientConfig::CLIENT_ID          => 'kafka-client-t1-offsetdel',
                ClientConfig::REQUEST_TIMEOUT_MS => 20000,
            ];
            new AdminClient(Cluster::bootstrap($configuration), $configuration)->deleteTopics($topics);
        } catch (Throwable) {
            // A broker that is gone or busy is not a failure of these tests - the topics are named uniquely
        }
    }

    /**
     * An `Empty` group hands over every partition of the request, and the committed offsets are really gone
     */
    public function testAnEmptyGroupLosesEveryPartitionOfTheRequest(): void
    {
        $topic   = $this->topic();
        $groupId = $this->emptyGroupWithOffsets($topic, [0 => 11, 1 => 22, 2 => 33]);

        $result = $this->admin->deleteConsumerGroupOffsets($groupId, [
            new TopicPartition($topic, 0),
            new TopicPartition($topic, 2),
        ]);

        self::assertSame([$topic => [0 => null, 2 => null]], $result);
        self::assertSame(
            [$topic => [0 => -1, 1 => 22, 2 => -1]],
            $this->client->fetchGroupOffsets($this->coordinator($groupId), $groupId, [$topic => [0, 1, 2]]),
            'the two deleted offsets answer -1 afterwards, and the third one is untouched'
        );
    }

    /**
     * A partition that never had a committed offset is answered with 0, exactly like one that had
     *
     * `GroupCoordinator.handleDeleteOffsets` @ 2.8.2 collects the partitions it considers eligible and writes
     * `NONE` for all of them without asking how many offsets were really removed, so a caller can not tell "there
     * was one and it is gone" from "there was nothing".
     */
    public function testAPartitionWithoutACommittedOffsetIsAnsweredWithTheCodeZero(): void
    {
        $topic   = $this->topic();
        $groupId = $this->emptyGroupWithOffsets($topic, [0 => 7]);

        $result = $this->admin->deleteConsumerGroupOffsets($groupId, [new TopicPartition($topic, 2)]);

        self::assertSame([$topic => [2 => null]], $result, 'partition 2 of this group was never committed');
    }

    /**
     * Deleting the last committed offset of an `Empty` group deletes the group itself
     */
    public function testTheLastOffsetOfAnEmptyGroupTakesTheGroupWithIt(): void
    {
        $topic   = $this->topic();
        $groupId = $this->emptyGroupWithOffsets($topic, [0 => 1, 1 => 2]);

        $this->admin->deleteConsumerGroupOffsets($groupId, [new TopicPartition($topic, 0)]);

        self::assertSame(
            DescribeGroupResponseMetadata::STATE_EMPTY,
            $this->admin->describeGroup($groupId)->state,
            'one of the two offsets is left, so the group is still there'
        );

        $this->admin->deleteConsumerGroupOffsets($groupId, [new TopicPartition($topic, 1)]);

        self::assertSame(
            DescribeGroupResponseMetadata::STATE_DEAD,
            $this->admin->describeGroup($groupId)->state,
            'an Empty group without a single offset left is transitioned to Dead and dropped from the cache'
        );
        $this->expectException(GroupIdNotFoundException::class);
        $this->admin->deleteConsumerGroupOffsets($groupId, [new TopicPartition($topic, 1)]);
    }

    /**
     * A live `consumer` group keeps the offsets of the topics its members are subscribed to, with the code 86
     */
    public function testALiveConsumerGroupRefusesTheTopicItIsSubscribedToWith86(): void
    {
        $topic   = $this->topic();
        $groupId = $this->uniqueGroupId('live');
        $this->joinGroup($groupId, 'consumer', new Subscription([$topic])->pack());

        $result = $this->admin->deleteConsumerGroupOffsets($groupId, [
            new TopicPartition($topic, 0),
            new TopicPartition($topic, 1),
        ]);

        self::assertInstanceOf(GroupSubscribedToTopicException::class, $result[$topic][0]);
        self::assertSame(KafkaException::GROUP_SUBSCRIBED_TO_TOPIC, $result[$topic][0]->getCode());
        self::assertInstanceOf(GroupSubscribedToTopicException::class, $result[$topic][1]);
    }

    /**
     * The very same live group loses the offsets of every topic it is **not** subscribed to
     */
    public function testALiveConsumerGroupStillLosesATopicItDoesNotConsume(): void
    {
        $topic   = $this->topic();
        $other   = $this->otherTopic();
        $groupId = $this->uniqueGroupId('mixed');
        $member  = $this->joinGroup($groupId, 'consumer', new Subscription([$topic])->pack());

        $this->client->commitGroupOffsets(
            $this->coordinator($groupId),
            $groupId,
            $member['memberId'],
            $member['generationId'],
            [$other => [0 => 4]],
            OffsetCommitRequest::DEFAULT_RETENTION_TIME
        );

        $result = $this->admin->deleteConsumerGroupOffsets($groupId, [
            new TopicPartition($topic, 0),
            new TopicPartition($other, 0),
        ]);

        self::assertInstanceOf(GroupSubscribedToTopicException::class, $result[$topic][0], 'it consumes this one');
        self::assertNull($result[$other][0], 'and this one it does not, so the offset is gone');
    }

    /**
     * A live group whose members joined with another protocol type is refused as a whole with the code 68
     */
    public function testALiveGroupOfAnotherProtocolTypeIsRefusedWithNonEmptyGroup(): void
    {
        $topic   = $this->topic();
        $groupId = $this->uniqueGroupId('foreign');
        $this->joinGroup($groupId, 't1-offsetdelete-protocol', 'whatever');

        $this->expectException(GroupNotEmptyException::class);
        $this->expectExceptionCode(KafkaException::NON_EMPTY_GROUP);

        $this->admin->deleteConsumerGroupOffsets($groupId, [new TopicPartition($topic, 0)]);
    }

    /**
     * A group the coordinator does not know is the top-level 69, and it is thrown
     */
    public function testAnUnknownGroupIsRefusedWithGroupIdNotFound(): void
    {
        $topic = $this->topic();

        $this->expectException(GroupIdNotFoundException::class);
        $this->expectExceptionCode(KafkaException::GROUP_ID_NOT_FOUND);

        $this->admin->deleteConsumerGroupOffsets($this->uniqueGroupId('unknown'), [new TopicPartition($topic, 0)]);
    }

    /**
     * A topic the broker does not have is answered per partition with the code 3, next to the partitions it has
     */
    public function testAnUnknownTopicIsRefusedPerPartitionWithTheCode3(): void
    {
        $topic   = $this->topic();
        $missing = self::uniqueTopicName('t1-offsetdel-missing');
        $groupId = $this->emptyGroupWithOffsets($topic, [0 => 9, 1 => 9]);

        $result = $this->admin->deleteConsumerGroupOffsets($groupId, [
            new TopicPartition($missing, 0),
            new TopicPartition($topic, 0),
        ]);

        self::assertInstanceOf(UnknownTopicOrPartitionException::class, $result[$missing][0]);
        self::assertSame(KafkaException::UNKNOWN_TOPIC_OR_PARTITION, $result[$missing][0]->getCode());
        self::assertNull($result[$topic][0], 'and one refused partition does not spoil the others');
    }

    /**
     * The empty partition list never reaches the broker, and the empty topic array is a legal frame
     */
    public function testTheEmptyRequestAsksAboutNothing(): void
    {
        $topic   = $this->topic();
        $groupId = $this->emptyGroupWithOffsets($topic, [0 => 3]);

        self::assertSame([], $this->admin->deleteConsumerGroupOffsets($groupId, []));
        self::assertSame(
            [],
            $this->client->deleteGroupOffsets($this->coordinator($groupId), $groupId, []),
            'and the coordinator answers the empty topic array with the error code of the group alone'
        );
    }

    /**
     * Returns the topic of this class, created on its first use with three partitions
     */
    private function topic(): string
    {
        if (self::$topic === null) {
            self::$topic = self::uniqueTopicName('t1-offsetdel');
            $this->admin->createTopics([new NewTopic(self::$topic, 3, 1)]);
            $this->awaitTopic(self::$topic);
        }

        return self::$topic;
    }

    /**
     * Returns the second topic of this class, which no group of it ever subscribes to
     */
    private function otherTopic(): string
    {
        if (self::$otherTopic === null) {
            self::$otherTopic = self::uniqueTopicName('t1-offsetdel-other');
            $this->admin->createTopics([new NewTopic(self::$otherTopic, 1, 1)]);
            $this->awaitTopic(self::$otherTopic);
        }

        return self::$otherTopic;
    }

    /**
     * Waits until the cluster metadata knows the partitions of a freshly created topic
     */
    private function awaitTopic(string $topic): void
    {
        $cluster = Cluster::bootstrap($this->configuration());
        for ($attempt = 0; $attempt < 40; $attempt++) {
            $cluster->reload();
            if ($cluster->partitionsForTopic($topic) !== []) {
                return;
            }
            usleep(250000);
        }

        self::fail("The topic {$topic} did not appear in the metadata of the cluster");
    }

    /**
     * Creates a group that has committed the given offsets and has no member left
     *
     * @param array<int, int> $offsets Offset to commit, per partition of $topic
     */
    private function emptyGroupWithOffsets(string $topic, array $offsets): string
    {
        $groupId     = $this->uniqueGroupId('empty');
        $coordinator = $this->coordinator($groupId);
        $member      = $this->joinGroup($groupId, 'consumer', new Subscription([$topic])->pack());

        $this->client->commitGroupOffsets(
            $coordinator,
            $groupId,
            $member['memberId'],
            $member['generationId'],
            [$topic => $offsets],
            OffsetCommitRequest::DEFAULT_RETENTION_TIME
        );
        $this->client->leaveGroup($coordinator, $groupId, $member['memberId']);

        return $groupId;
    }

    /**
     * Joins a group as its only member and publishes the assignment of the generation
     *
     * @return array{memberId: string, generationId: int}
     */
    private function joinGroup(string $groupId, string $protocolType, string $metadata): array
    {
        $coordinator = $this->coordinator($groupId);
        $protocols   = ['range' => $metadata];

        // The container is shared, and under load the coordinator can forget the member the KIP-394 exchange just
        // created before its SyncGroup arrives - which is the 25 a real consumer answers by rejoining from
        // scratch. Two attempts are enough to keep the suite honest without hiding a real refusal.
        for ($attempt = 1; ; $attempt++) {
            try {
                $join = $this->join($coordinator, $groupId, $protocolType, $protocols);

                $this->client->syncGroup(
                    $coordinator,
                    $groupId,
                    $join->memberId,
                    $join->generationId,
                    [$join->memberId => '']
                );

                return ['memberId' => $join->memberId, 'generationId' => $join->generationId];
            } catch (UnknownMemberIdException | RebalanceInProgressException $exception) {
                if ($attempt === 3) {
                    throw $exception;
                }
            }
        }
    }

    /**
     * Joins the group once, answering the 79 of KIP-394 with the member id the coordinator assigned
     */
    private function join(Node $coordinator, string $groupId, string $protocolType, array $protocols): JoinGroupResponse
    {
        try {
            return $this->client->joinGroup($coordinator, $groupId, '', $protocolType, $protocols);
        } catch (MemberIdRequiredException $exception) {
            // KIP-394, Kafka 2.2: the version 4 of JoinGroup refuses the first join of a member that has no id
            // yet with the code 79 and hands out the id it assigned, and the member joins again with that id
            return $this->client->joinGroup(
                $coordinator,
                $groupId,
                (string) $exception->getContext()['assignedMemberId'],
                $protocolType,
                $protocols
            );
        }
    }

    private function coordinator(string $groupId): Node
    {
        return $this->admin->findCoordinator($groupId);
    }

    private function uniqueGroupId(string $kind): string
    {
        return 't1-offsetdel-' . $kind . '-' . bin2hex(random_bytes(6));
    }

    /**
     * @return array<string, mixed> Client configuration for this test class
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => 'kafka-client-t1-offsetdel',
            ClientConfig::REQUEST_TIMEOUT_MS        => 20000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ConsumerConfig::SESSION_TIMEOUT_MS      => self::SESSION_TIMEOUT_MS,
        ];
    }
}
