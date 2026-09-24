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
use Protocol\Kafka\Common\AclOperation;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\CoordinatorLookup;
use Protocol\Kafka\Common\Errors\FencedMemberEpochException;
use Protocol\Kafka\Common\Errors\GroupIdNotFoundException;
use Protocol\Kafka\Common\Errors\InvalidRequestException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\UnknownMemberIdException;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\Internals\ConsumerGroupHeartbeatCoordinator;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Data\ShareGroupDescribedGroup;
use Protocol\Kafka\Protocol\Request\ShareGroupDescribeRequest;
use Protocol\Kafka\Protocol\Request\ShareGroupDescribeResponse;
use Protocol\Kafka\Protocol\Request\ShareGroupHeartbeatRequest;
use Protocol\Kafka\Protocol\Request\ShareGroupHeartbeatResponse;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * ShareGroupHeartbeat (key 76) and ShareGroupDescribe (key 77) of KIP-932 against the 4.3.1 KRaft node, at their
 * version 1 (Kafka 4.1), and the deletion of a share group through DeleteGroups (key 42).
 *
 * The membership of a share group is the KIP-848 heartbeat without everything a partition *owner* needs: no
 * rebalance timeout, no instance id, no assignor choice and no owned partitions, because a share member owns
 * nothing - it only reads next to the others. The node's `share.version` 1 serves both apis without
 * `unstable.api.versions.enable`.
 *
 * Every group of this class carries the `t3-41-sg-` prefix, every member of it leaves with the epoch -1 in
 * {@see self::tearDownAfterClass()} at the latest, and every group is deleted there.
 *
 * @see docs/protocol/4.3.md, section "ShareGroupHeartbeat API (key 76, v1)"
 * @see docs/protocol/4.3.md, section "ShareGroupDescribe API (key 77, v1)"
 */
#[CoversClass(Client::class)]
#[CoversClass(AdminClient::class)]
#[CoversClass(ShareGroupHeartbeatRequest::class)]
#[CoversClass(ShareGroupHeartbeatResponse::class)]
#[CoversClass(ShareGroupDescribeRequest::class)]
#[CoversClass(ShareGroupDescribeResponse::class)]
final class ShareGroupApiTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t3-41-sg';

    private const string TOPIC_PREFIX = 't3-41-sg';

    private const int REQUEST_TIMEOUT_MS = 30000;

    /**
     * How long a test heartbeats for an assignment the coordinator computes in the background, in seconds
     */
    private const float ASSIGNMENT_TIMEOUT = 20.0;

    private static ?Cluster $sharedCluster = null;

    /**
     * Every group this class created
     *
     * @var list<string>
     */
    private static array $groups = [];

    /**
     * Every member this class left in a group, as `[group id, member id]`
     *
     * @var list<array{string, string}>
     */
    private static array $members = [];

    public static function tearDownAfterClass(): void
    {
        foreach (self::$members as [$groupId, $memberId]) {
            self::leaveQuietly($groupId, $memberId);
        }
        self::$members = [];

        foreach (self::$groups as $groupId) {
            self::deleteGroupQuietly($groupId);
        }
        self::$groups = [];

        parent::tearDownAfterClass();
    }

    /**
     * The member id is the client's own, the join is answered an epoch and no partition, the partitions come later
     */
    public function testTheJoinIsAnsweredAnEpochAndTheAssignmentFollowsOnALaterHeartbeat(): void
    {
        $topic   = $this->topic(3);
        $groupId = $this->uniqueGroupName();
        $member  = ConsumerGroupHeartbeatCoordinator::newMemberId();

        $join = $this->client()->joinShareGroup($this->coordinator($groupId), $groupId, $member, [$topic]);
        self::$members[] = [$groupId, $member];

        self::assertSame(KafkaException::NO_ERROR, $join->errorCode);
        self::assertNull($join->errorMessage);
        self::assertSame($member, $join->memberId, 'the member id the client generated for itself comes back');
        self::assertGreaterThan(0, $join->memberEpoch, 'a join is answered an epoch of the group, never the 0 it sent');
        self::assertSame(5000, $join->heartbeatIntervalMs, 'group.share.heartbeat.interval.ms of the node');
        self::assertSame(
            [],
            $join->assignment?->partitionsByTopicId() ?? [],
            'the join itself assigns nothing: the share state of the partitions is initialised first'
        );

        $assigned = $this->heartbeatUntilAssigned($groupId, $member, $join->memberEpoch);

        self::assertSame(KafkaException::NO_ERROR, $assigned->errorCode);
        self::assertGreaterThan($join->memberEpoch, $assigned->memberEpoch, 'the assignment comes with a new epoch');
        self::assertNotNull($assigned->assignment);
        self::assertSame(
            [self::topicIdOf($topic) => [0, 1, 2]],
            $assigned->assignment->partitionsByTopicId(),
            'the one member of the group reads from every partition, named by the topic id'
        );

        // and a steady-state heartbeat is answered the null of "nothing changed"
        $steady = $this->client()->shareGroupHeartbeat(
            $this->coordinator($groupId),
            $groupId,
            $member,
            $assigned->memberEpoch
        );

        self::assertSame($assigned->memberEpoch, $steady->memberEpoch);
        self::assertNull($steady->assignment, 'a null assignment is "nothing changed", not "you read nothing"');
    }

    /**
     * Two members of a share group may read from the same partitions: the simple assignor shares them out
     */
    public function testASecondMemberIsAssignedPartitionsOfTheFirst(): void
    {
        $topic   = $this->topic(3);
        $groupId = $this->uniqueGroupName();
        [$first, $firstEpoch] = $this->joinedMember($groupId, [$topic]);
        $this->heartbeatUntilAssigned($groupId, $first, $firstEpoch);

        [$second, $secondEpoch] = $this->joinedMember($groupId, [$topic]);
        $assigned = $this->heartbeatUntilAssigned($groupId, $second, $secondEpoch);

        self::assertNotNull($assigned->assignment);
        $partitions = $assigned->assignment->partitionsByTopicId()[self::topicIdOf($topic)] ?? [];
        self::assertNotSame([], $partitions, 'the second member is given partitions of the topic as well');

        $group = $this->admin()->describeShareGroup($groupId);
        self::assertCount(2, $group->members);
        self::assertSame(ShareGroupDescribedGroup::STATE_STABLE, $group->groupState);
    }

    /**
     * The shape rules of the join: a member id of the client's own and the whole subscription
     */
    public function testAJoinWithoutAMemberIdOrWithoutTopicsIsRefusedWithTheFortyTwo(): void
    {
        $topic   = $this->topic(1);
        $groupId = $this->uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);

        $noMemberId = $this->send($stream, ShareGroupHeartbeatRequest::forJoin($groupId, '', [$topic], null, self::CLIENT_ID));
        self::assertSame(KafkaException::INVALID_REQUEST, $noMemberId->errorCode);
        self::assertNull($noMemberId->errorMessage, 'the 4.3.1 node gives no message for the empty member id');

        $noTopics = $this->send($stream, new ShareGroupHeartbeatRequest(
            $groupId,
            ConsumerGroupHeartbeatCoordinator::newMemberId(),
            ShareGroupHeartbeatRequest::JOIN_MEMBER_EPOCH,
            [],
            null,
            self::CLIENT_ID
        ));
        self::assertSame(KafkaException::INVALID_REQUEST, $noTopics->errorCode);
        self::assertSame('SubscribedTopicNames must be set in first request.', $noTopics->errorMessage);

        $this->expectException(InvalidRequestException::class);
        $this->client()->joinShareGroup($this->coordinator($groupId), $groupId, '', [$topic]);
    }

    /**
     * The epoch rules of KIP-848 hold for share members as well: 110 for a wrong epoch, 25 for an unknown member
     */
    public function testAHeartbeatOfAWrongEpochOrOfAnUnknownMemberIsRefused(): void
    {
        $topic   = $this->topic(1);
        $groupId = $this->uniqueGroupName();
        [$member, $epoch] = $this->joinedMember($groupId, [$topic]);
        $node    = $this->coordinator($groupId);

        try {
            $this->client()->shareGroupHeartbeat($node, $groupId, $member, $epoch + 10);
            self::fail('a heartbeat ahead of the epoch of the coordinator is fenced');
        } catch (FencedMemberEpochException $fenced) {
            self::assertStringContainsString('greater member epoch', (string) ($fenced->getContext()['error'] ?? ''));
        }

        $this->expectException(UnknownMemberIdException::class);
        $this->client()->shareGroupHeartbeat($node, $groupId, ConsumerGroupHeartbeatCoordinator::newMemberId(), 1);
    }

    /**
     * A heartbeat of a non-zero epoch does not create a group: the 69 of a group the coordinator does not know
     */
    public function testAHeartbeatOfANonZeroEpochToAnUnknownGroupIsTheSixtyNine(): void
    {
        $groupId = $this->uniqueGroupName();

        $this->expectException(GroupIdNotFoundException::class);
        $this->client()->shareGroupHeartbeat(
            $this->coordinator($groupId),
            $groupId,
            ConsumerGroupHeartbeatCoordinator::newMemberId(),
            1
        );
    }

    /**
     * ShareGroupDescribe names the epochs, the assignor, and per member its subscription and assignment by name
     */
    public function testAShareGroupIsDescribedWithItsMembersAndTheirAssignment(): void
    {
        $topic   = $this->topic(2);
        $groupId = $this->uniqueGroupName();
        [$member, $epoch] = $this->joinedMember($groupId, [$topic]);
        $assigned = $this->heartbeatUntilAssigned($groupId, $member, $epoch);

        $group = $this->admin()->describeShareGroup($groupId, true);

        self::assertSame(KafkaException::NO_ERROR, $group->errorCode);
        self::assertSame($groupId, $group->groupId);
        self::assertSame(ShareGroupDescribedGroup::STATE_STABLE, $group->groupState);
        self::assertSame('simple', $group->assignorName, 'the one assignor of share groups at 4.3.1');
        self::assertGreaterThan(0, $group->groupEpoch);
        self::assertSame($group->groupEpoch, $group->assignmentEpoch, 'a settled group assigned its latest epoch');
        self::assertNotSame(AclOperation::NOT_REQUESTED, $group->authorizedOperations);
        self::assertSame([$member], array_keys($group->members));

        $described = $group->members[$member];
        self::assertSame($assigned->memberEpoch, $described->memberEpoch);
        self::assertSame(self::CLIENT_ID, $described->clientId);
        self::assertNotSame('', $described->clientHost);
        self::assertNull($described->rackId);
        self::assertSame([$topic], $described->subscribedTopicNames);
        self::assertSame([$topic => [0, 1]], $described->assignment->partitionsByTopic());
    }

    /**
     * A group that does not exist is the 69 inside its own entry, with a message and the empty state
     */
    public function testAnUnknownGroupIsDescribedWithTheSixtyNine(): void
    {
        $groupId = $this->uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);

        new ShareGroupDescribeRequest([$groupId], false, self::CLIENT_ID)->writeTo($stream);
        $answer = ShareGroupDescribeResponse::unpack($stream);

        $entry = $answer->groups[$groupId];
        self::assertSame(KafkaException::GROUP_ID_NOT_FOUND, $entry->errorCode);
        self::assertSame("Group {$groupId} not found.", $entry->errorMessage);
        self::assertSame('', $entry->groupState);
        self::assertSame([], $entry->members);

        $this->expectException(GroupIdNotFoundException::class);
        $this->admin()->describeShareGroups([$groupId]);
    }

    /**
     * The leave is the epoch -1, the group is Empty afterwards, and DeleteGroups (42) deletes a share group
     */
    public function testTheLastMemberLeavesAndTheEmptyShareGroupIsDeletedWithDeleteGroups(): void
    {
        $topic   = $this->topic(1);
        $groupId = $this->uniqueGroupName();
        [$member, $epoch] = $this->joinedMember($groupId, [$topic]);
        $this->heartbeatUntilAssigned($groupId, $member, $epoch);

        $leave = $this->client()->leaveShareGroup($this->coordinator($groupId), $groupId, $member);

        self::assertSame(KafkaException::NO_ERROR, $leave->errorCode);
        self::assertSame($member, $leave->memberId);
        self::assertSame(ShareGroupHeartbeatRequest::LEAVE_MEMBER_EPOCH, $leave->memberEpoch);
        self::assertSame(0, $leave->heartbeatIntervalMs);
        self::assertNull($leave->assignment);

        $empty = $this->admin()->describeShareGroup($groupId);
        self::assertSame(ShareGroupDescribedGroup::STATE_EMPTY, $empty->groupState);
        self::assertSame([], $empty->members);

        $deleted = $this->admin()->deleteConsumerGroups([$groupId]);
        self::assertSame([$groupId => null], $deleted, 'DeleteGroups deletes an empty share group');

        $this->expectException(GroupIdNotFoundException::class);
        $this->admin()->describeShareGroup($groupId);
    }

    /**
     * Creates a topic of this class and waits until every partition of it has a leader
     */
    private function topic(int $partitions): string
    {
        $topic = self::uniqueTopicName(self::TOPIC_PREFIX);

        $this->admin()->createTopics([new NewTopic($topic, $partitions, 1)]);
        new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, self::TOPIC_PREFIX)
            ->awaitTopicWithLeaders($topic);

        return $topic;
    }

    /**
     * Joins a group of this class and answers the member id and the epoch it was given
     *
     * @param list<string> $topics
     *
     * @return array{string, int}
     */
    private function joinedMember(string $groupId, array $topics): array
    {
        $member = ConsumerGroupHeartbeatCoordinator::newMemberId();
        $answer = $this->client()->joinShareGroup($this->coordinator($groupId), $groupId, $member, $topics);
        self::$members[] = [$groupId, $member];

        self::assertSame(KafkaException::NO_ERROR, $answer->errorCode);

        return [$member, $answer->memberEpoch];
    }

    /**
     * Heartbeats a member until an answer carries partitions, which the coordinator assigns in the background
     */
    private function heartbeatUntilAssigned(string $groupId, string $memberId, int $epoch): ShareGroupHeartbeatResponse
    {
        $deadline = microtime(true) + self::ASSIGNMENT_TIMEOUT;
        $node     = $this->coordinator($groupId);

        while (true) {
            $answer = $this->client()->shareGroupHeartbeat($node, $groupId, $memberId, $epoch);
            if (($answer->assignment?->partitionsByTopicId() ?? []) !== [] || microtime(true) >= $deadline) {
                return $answer;
            }
            $epoch = $answer->memberEpoch;
            usleep(250000);
        }
    }

    /**
     * Sends one frame over an open coordinator connection and reads its answer
     */
    private function send(Stream $stream, ShareGroupHeartbeatRequest $request): ShareGroupHeartbeatResponse
    {
        $request->writeTo($stream);

        return ShareGroupHeartbeatResponse::unpack($stream);
    }

    private function client(): Client
    {
        return new Client($this->cluster(), $this->configuration());
    }

    private function admin(): AdminClient
    {
        return new AdminClient($this->cluster(), $this->configuration());
    }

    private function coordinator(string $groupId): Node
    {
        return new CoordinatorLookup($this->cluster(), $this->configuration())->findCoordinator($groupId);
    }

    private function coordinatorStream(string $groupId): Stream
    {
        return $this->coordinator($groupId)->getConnection($this->configuration());
    }

    private function cluster(): Cluster
    {
        return self::$sharedCluster ??= Cluster::bootstrap($this->configuration());
    }

    /**
     * @return array<string, mixed>
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ClientConfig::RETRY_BACKOFF_MS          => 250,
            ClientConfig::RETRIES                   => 2,
            ClientConfig::REQUEST_TIMEOUT_MS        => self::REQUEST_TIMEOUT_MS,
        ] + ConsumerConfig::getDefaultConfiguration();
    }

    private function uniqueGroupName(): string
    {
        $groupId        = 't3-41-sg-group-' . bin2hex(random_bytes(6));
        self::$groups[] = $groupId;

        return $groupId;
    }

    /**
     * Takes one member of this class out of its group with the epoch -1, ignoring a member that is gone already
     */
    private static function leaveQuietly(string $groupId, string $memberId): void
    {
        $configuration = self::cleanupConfiguration();

        try {
            $cluster = Cluster::bootstrap($configuration);
            new Client($cluster, $configuration)->leaveShareGroup(
                new CoordinatorLookup($cluster, $configuration)->findCoordinator($groupId),
                $groupId,
                $memberId
            );
        } catch (KafkaException) {
            // A member that left in its test, or a group that is gone, must not fail the suite
        }
    }

    private static function deleteGroupQuietly(string $groupId): void
    {
        try {
            $configuration = self::cleanupConfiguration();

            new AdminClient(Cluster::bootstrap($configuration), $configuration)->deleteConsumerGroups([$groupId]);
        } catch (KafkaException) {
            // A group that is gone already must not fail the suite
        }
    }

    /**
     * @return array<string, mixed>
     */
    private static function cleanupConfiguration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ClientConfig::REQUEST_TIMEOUT_MS        => self::REQUEST_TIMEOUT_MS,
        ] + ConsumerConfig::getDefaultConfiguration();
    }
}
