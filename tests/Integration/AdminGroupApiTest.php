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
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\GroupIdNotFoundException;
use Protocol\Kafka\Common\Errors\GroupNotEmptyException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Data\DescribeGroupResponseMember;
use Protocol\Kafka\Protocol\Data\DescribeGroupResponseMetadata;
use Protocol\Kafka\Protocol\Data\ListGroupResponseProtocol;
use Protocol\Kafka\Protocol\Request\DeleteGroupsRequest;
use Protocol\Kafka\Protocol\Request\DeleteGroupsResponse;
use Protocol\Kafka\Protocol\Request\DescribeGroupsRequest;
use Protocol\Kafka\Protocol\Request\DescribeGroupsResponse;
use Protocol\Kafka\Protocol\Request\ListGroupsRequest;
use Protocol\Kafka\Protocol\Request\ListGroupsResponse;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequest;
use Protocol\Kafka\Tests\Fixture\RawGroupMember;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * Exercises the group admin apis - ListGroups (16) and DescribeGroups (15) of Kafka 0.9, and the DeleteGroups (42)
 * of Kafka 1.1 - against a real broker.
 *
 * The groups these tests look at are created with {@see RawGroupMember}, which speaks the JoinGroup and SyncGroup
 * requests of the membership protocol directly: a group only exists on the coordinator while it has a member, and
 * the client classes of those two apis live on another branch of this line.
 *
 * The broker is shared with the other suites and coordinates their groups too, so every assertion here is about the
 * groups of this class and never about the whole answer.
 *
 * @see docs/protocol/2.8.md, sections "DescribeGroups API (key 15, v0 and v1)", "ListGroups API (key 16, v0 and v1)"
 *      and "DeleteGroups API (key 42, v0)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(DescribeGroupsRequest::class)]
#[CoversClass(DescribeGroupsResponse::class)]
#[CoversClass(DescribeGroupResponseMetadata::class)]
#[CoversClass(DescribeGroupResponseMember::class)]
#[CoversClass(ListGroupsRequest::class)]
#[CoversClass(ListGroupsResponse::class)]
#[CoversClass(ListGroupResponseProtocol::class)]
#[CoversClass(DeleteGroupsRequest::class)]
#[CoversClass(DeleteGroupsResponse::class)]
final class AdminGroupApiTest extends IntegrationTestCase
{
    /**
     * Topic the members of these groups subscribe to; it never has to exist for the group apis to work
     */
    private const string TOPIC = 't4-admin-groups-topic';

    /**
     * Session timeout of the raw members, long enough for a test to look at the group they left behind
     */
    private const int SESSION_TIMEOUT_MS = 30000;

    private Cluster $cluster;

    private AdminClient $admin;

    /**
     * Members that a test created, closed again after it
     *
     * @var list<RawGroupMember>
     */
    private array $members = [];

    protected function setUp(): void
    {
        parent::setUp();

        $this->cluster = Cluster::bootstrap($this->configuration());
        $this->admin   = new AdminClient($this->cluster, $this->configuration());
    }

    protected function tearDown(): void
    {
        foreach ($this->members as $member) {
            try {
                $member->leave();
            } catch (\RuntimeException) {
                // The group is gone already, which is what leaving it is supposed to achieve
            }
            $member->close();
        }
        $this->members = [];
    }

    public function testListGroupsReportsAGroupOfTheCoordinatorWithItsProtocolType(): void
    {
        $groupId = $this->uniqueGroupId();
        $this->joinGroup($groupId, 't4-list-member');

        $coordinator = $this->admin->findCoordinator($groupId);
        $groups      = $this->admin->listGroups($coordinator);

        self::assertArrayHasKey($groupId, $groups, 'the coordinator lists the group it just accepted a member for');
        self::assertSame($groupId, $groups[$groupId]->groupId);
        self::assertSame('consumer', $groups[$groupId]->protocolType);
    }

    public function testListAllGroupsMergesTheGroupsOfEveryBroker(): void
    {
        $groupId = $this->uniqueGroupId();
        $this->joinGroup($groupId, 't4-list-all-member');

        $groups = $this->admin->listAllGroups();

        self::assertArrayHasKey($groupId, $groups);
        self::assertSame('consumer', $groups[$groupId]->protocolType);
        foreach ($groups as $listedGroupId => $group) {
            self::assertSame($listedGroupId, $group->groupId, 'the merged map is indexed by the group id');
        }
    }

    public function testDescribeGroupReportsTheMemberOfAStableGroup(): void
    {
        $groupId      = $this->uniqueGroupId();
        $clientId     = 't4-describe-member';
        $subscription = self::subscription();
        $assignment   = self::assignment();
        $member       = $this->joinGroup($groupId, $clientId, $subscription, $assignment);

        $group = $this->admin->describeGroup($groupId);

        self::assertSame($groupId, $group->groupId);
        self::assertSame(KafkaException::NO_ERROR, $group->errorCode);
        self::assertSame(DescribeGroupResponseMetadata::STATE_STABLE, $group->state);
        self::assertSame('consumer', $group->protocolType);
        self::assertSame('range', $group->protocol, 'the protocol is only reported for a stable group');

        self::assertSame([$member->getMemberId()], array_keys($group->members));
        $described = $group->members[$member->getMemberId()];
        self::assertSame($clientId, $described->clientId);
        self::assertStringStartsWith('/', $described->clientHost, 'the host is written as Java prints an address');
        self::assertSame($subscription, $described->memberMetadata, 'the metadata comes back as it was sent');
        self::assertSame($assignment, $described->memberAssignment, 'and so does the assignment of the leader');
    }

    /**
     * The state between the last JoinGroup and the leader's SyncGroup is called `CompletingRebalance` from Kafka 1.0
     *
     * `GroupMetadata.scala` called it `AwaitingSync` from 0.9 to 0.11 and the coordinator answered that string;
     * Kafka 1.0 renamed the state and nothing else about it, so
     * {@see DescribeGroupResponseMetadata::STATE_AWAITING_SYNC} is kept for the lines below and a broker of this
     * line never sends it.
     */
    public function testDescribeGroupReportsCompletingRebalanceWhileTheLeaderHasNotPublishedTheAssignment(): void
    {
        $groupId = $this->uniqueGroupId();
        $member  = new RawGroupMember(
            $this->coordinatorAddress($groupId),
            $groupId,
            't4-awaiting-member',
            self::SESSION_TIMEOUT_MS
        );
        $this->members[] = $member;
        $member->join('range', self::subscription());

        $group = $this->admin->describeGroup($groupId);

        self::assertSame(DescribeGroupResponseMetadata::STATE_COMPLETING_REBALANCE, $group->state);
        self::assertNotSame(
            DescribeGroupResponseMetadata::STATE_AWAITING_SYNC,
            $group->state,
            'the 0.9 to 0.11 name of the very same state, which a 1.x broker never answers'
        );
        self::assertSame('consumer', $group->protocolType);
        self::assertSame('', $group->protocol, 'the protocol is empty until the group is stable');
        self::assertCount(1, $group->members, 'the members are already known in this state');
    }

    public function testDescribeGroupReportsAnUnknownGroupAsDeadWithoutAnError(): void
    {
        $group = $this->admin->describeGroup($this->uniqueGroupId());

        self::assertSame(KafkaException::NO_ERROR, $group->errorCode, 'an unknown group is not an error');
        self::assertSame(DescribeGroupResponseMetadata::STATE_DEAD, $group->state);
        self::assertSame('', $group->protocolType);
        self::assertSame('', $group->protocol);
        self::assertSame([], $group->members);
    }

    public function testDescribeGroupsDescribesSeveralGroupsWithOneRequest(): void
    {
        $first  = $this->uniqueGroupId();
        $second = $this->uniqueGroupId();
        $this->joinGroup($first, 't4-batch-member-a');
        $this->joinGroup($second, 't4-batch-member-b');

        $groups = $this->admin->describeGroups([$first, $second]);

        self::assertSame([$first, $second], array_keys($groups));
        self::assertSame(DescribeGroupResponseMetadata::STATE_STABLE, $groups[$first]->state);
        self::assertSame(DescribeGroupResponseMetadata::STATE_STABLE, $groups[$second]->state);
    }

    public function testAGroupStaysBehindEmptyWhenItsLastMemberLeaves(): void
    {
        // Kafka 0.10.1 gave the coordinator a fifth group state. A 0.9.0.1 coordinator dropped a group the moment
        // its last member left, so the group vanished from ListGroups and DescribeGroups answered `Dead` for it.
        // From 0.10.1 on the group moves to `Empty` instead and keeps its committed offsets until
        // `offsets.retention.minutes` expires them; only the expiry makes it `Dead` and removes it
        // (`GroupMetadata`/`GroupCoordinator` @ 0.10.2.2). It is therefore still listed here.
        $groupId = $this->uniqueGroupId();
        $member  = $this->joinGroup($groupId, 't4-leaving-member');

        $member->leave();

        $coordinator = $this->admin->findCoordinator($groupId);
        self::assertArrayHasKey(
            $groupId,
            $this->admin->listGroups($coordinator),
            'the coordinator keeps a group that has no members left'
        );

        $description = $this->admin->describeGroup($groupId);

        self::assertSame(
            DescribeGroupResponseMetadata::STATE_EMPTY,
            $description->state,
            'and describes it as Empty, not as Dead - a client can tell "everybody left" from "never existed"'
        );
        self::assertSame([], $description->members, 'an empty group reports no members');
        self::assertSame('consumer', $description->protocolType, 'the protocol type of the group survives');
        self::assertSame('', $description->protocol, 'the protocol is only reported while the group is stable');
    }

    /**
     * KIP-229: a group without members is deleted, and its committed offsets go with it
     */
    public function testAnEmptyGroupIsDeletedWithItsCommittedOffsets(): void
    {
        $groupId = $this->emptyGroupWithACommittedOffset();
        $topic   = self::committedTopic();
        self::assertSame(7, $this->admin->listGroupOffsets($groupId)[$topic]->partitions[0]->offset);

        $result = $this->admin->deleteConsumerGroups([$groupId]);

        self::assertSame([$groupId => null], $result, 'a group nobody is a member of is deleted');
        self::assertArrayNotHasKey(
            $groupId,
            $this->admin->listGroups($this->admin->findCoordinator($groupId)),
            'and it is gone from the group list of its coordinator'
        );
        self::assertSame(
            [],
            $this->admin->listGroupOffsets($groupId),
            'the coordinator wrote a tombstone for every offset the group had committed'
        );
        self::assertSame(
            DescribeGroupResponseMetadata::STATE_DEAD,
            $this->admin->describeGroup($groupId)->state,
            'and describes it like a group that never existed'
        );
    }

    public function testAGroupWithAMemberIsRefusedWithNonEmptyGroup(): void
    {
        $groupId = $this->uniqueGroupId();
        $member  = $this->joinGroup($groupId, 't4-delete-member');

        $result = $this->admin->deleteConsumerGroups([$groupId]);

        self::assertInstanceOf(
            GroupNotEmptyException::class,
            $result[$groupId],
            'the offsets of a group whose consumers are running are never thrown away'
        );
        self::assertSame(
            DescribeGroupResponseMetadata::STATE_STABLE,
            $this->admin->describeGroup($groupId)->state,
            'and the group is untouched'
        );

        // The very same group is deletable the moment its last member has left it
        $member->leave();

        self::assertSame([$groupId => null], $this->admin->deleteConsumerGroups([$groupId]));
    }

    public function testAGroupTheCoordinatorDoesNotKnowIsReportedWithGroupIdNotFound(): void
    {
        $groupId = $this->uniqueGroupId();

        $result = $this->admin->deleteConsumerGroups([$groupId]);

        self::assertInstanceOf(
            GroupIdNotFoundException::class,
            $result[$groupId],
            'the one way to tell "there was nothing to delete" from "it is still in use"'
        );
        self::assertSame(['groupId' => $groupId], $result[$groupId]->getContext());
    }

    public function testTheGroupsOfOneCallAreReportedIndependently(): void
    {
        $deletable = $this->emptyGroupWithACommittedOffset();
        $held      = $this->uniqueGroupId();
        $unknown   = $this->uniqueGroupId();
        $this->joinGroup($held, 't4-delete-second');

        $result = $this->admin->deleteConsumerGroups([$deletable, $held, $unknown]);

        self::assertSame([$deletable, $held, $unknown], array_keys($result), 'in the order of the call');
        self::assertNull($result[$deletable]);
        self::assertInstanceOf(GroupNotEmptyException::class, $result[$held]);
        self::assertInstanceOf(GroupIdNotFoundException::class, $result[$unknown]);
    }

    public function testDeletingADeadGroupIsAnsweredWithGroupIdNotFound(): void
    {
        // A group that was deleted a moment ago is `Dead`, i.e. exactly what an unknown group looks like
        $groupId = $this->emptyGroupWithACommittedOffset();
        self::assertSame([$groupId => null], $this->admin->deleteConsumerGroups([$groupId]));

        $result = $this->admin->deleteConsumerGroups([$groupId]);

        self::assertInstanceOf(GroupIdNotFoundException::class, $result[$groupId]);
    }

    public function testDeletingNoGroupAtAllIsAnsweredWithAnEmptyResult(): void
    {
        self::assertSame([], $this->admin->deleteConsumerGroups([]));
    }

    /**
     * `listGroupOffsets()` without partitions asks for every topic-partition the group committed (OffsetFetch v2)
     *
     * That is the shape the method has on the `main` branch, and the nullable topic array of the version 2 of the
     * api (Kafka 0.10.2) is what makes it possible: a client that has to report the position of a group cannot know
     * beforehand which topics the group committed.
     */
    public function testListGroupOffsetsWithoutPartitionsReportsEveryCommittedTopicOfTheGroup(): void
    {
        $groupId     = $this->uniqueGroupId();
        $topic       = self::uniqueTopicName('t6-admin-offsets');
        $client      = new Client(Cluster::bootstrap($this->configuration()), $this->configuration());
        $coordinator = $client->getGroupCoordinator($groupId);

        // A commit is refused for a topic that does not exist, so the topic is created (and awaited) first
        new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, 't4-admin-groups')->awaitTopicWithLeaders($topic);

        $client->commitGroupOffsets(
            $coordinator,
            $groupId,
            OffsetCommitRequest::DEFAULT_MEMBER_NAME,
            OffsetCommitRequest::DEFAULT_GENERATION_ID,
            [$topic => [0 => 12, 2 => 34]],
            OffsetCommitRequest::DEFAULT_RETENTION_TIME
        );

        $topics = $this->admin->listGroupOffsets($groupId);

        self::assertSame([$topic], array_keys($topics), 'the group committed exactly one topic');
        self::assertSame([0, 2], array_keys($topics[$topic]->partitions));
        self::assertSame(12, $topics[$topic]->partitions[0]->offset);
        self::assertSame(34, $topics[$topic]->partitions[2]->offset);

        self::assertSame(
            [],
            $this->admin->listGroupOffsets($groupId, []),
            'an empty iterable names no topic at all, which is not the same request as null'
        );
    }

    /**
     * Creates a group that has a committed offset and no member at all, i.e. one in the state `Empty`
     *
     * A group is created by a plain OffsetCommit as well as by a member, and the offset is what makes the group
     * survive until something deletes it: the coordinator drops an `Empty` group that holds no offset at all the
     * next time `offsets.retention.check.interval.ms` fires, which on a container that several suites share is a
     * window a test must not depend on (the group is answered with 69 instead of being deleted).
     *
     * @return string Id of the group, whose offset 7 of the partition 0 of {@see self::committedTopic()} is set
     */
    private function emptyGroupWithACommittedOffset(): string
    {
        $groupId = $this->uniqueGroupId();
        $topic   = self::committedTopic();
        $client  = new Client(Cluster::bootstrap($this->configuration()), $this->configuration());

        // A commit is refused for a topic that does not exist, so the topic is created (and awaited) first
        new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, 't4-admin-groups')
            ->awaitTopicWithLeaders($topic);

        $client->commitGroupOffsets(
            $client->getGroupCoordinator($groupId),
            $groupId,
            OffsetCommitRequest::DEFAULT_MEMBER_NAME,
            OffsetCommitRequest::DEFAULT_GENERATION_ID,
            [$topic => [0 => 7]],
            OffsetCommitRequest::DEFAULT_RETENTION_TIME
        );

        return $groupId;
    }

    /**
     * Topic the groups of the DeleteGroups tests commit an offset of, created once for the whole class
     */
    private static function committedTopic(): string
    {
        return self::$committedTopic ??= self::uniqueTopicName('t4-admin-delete');
    }

    /**
     * Name of that topic, resolved by {@see self::committedTopic()} on its first use
     */
    private static ?string $committedTopic = null;

    /**
     * Creates a group with a single member that has joined and published an assignment, i.e. a stable group
     */
    private function joinGroup(
        string $groupId,
        string $clientId,
        ?string $subscription = null,
        ?string $assignment = null
    ): RawGroupMember {
        $member = new RawGroupMember(
            $this->coordinatorAddress($groupId),
            $groupId,
            $clientId,
            self::SESSION_TIMEOUT_MS
        );
        $this->members[] = $member;
        $member->joinAndSync('range', $subscription ?? self::subscription(), $assignment ?? self::assignment());

        return $member;
    }

    /**
     * Returns the coordinator of a group as a `host:port` string
     */
    private function coordinatorAddress(string $groupId): string
    {
        $coordinator = $this->admin->findCoordinator($groupId);

        return $coordinator->host . ':' . $coordinator->port;
    }

    /**
     * Builds a group id that no other suite and no other run of this one uses
     */
    private function uniqueGroupId(): string
    {
        return 't4-admin-groups-' . bin2hex(random_bytes(6));
    }

    /**
     * The `Subscription` of the consumer protocol: version int16, topics [string], user data bytes
     */
    private static function subscription(): string
    {
        return pack('n', 0) . pack('N', 1) . pack('n', strlen(self::TOPIC)) . self::TOPIC . pack('N', 0);
    }

    /**
     * The `MemberAssignment` of the consumer protocol: version int16, [topic [partition]], user data bytes
     */
    private static function assignment(): string
    {
        return pack('n', 0) . pack('N', 1) . pack('n', strlen(self::TOPIC)) . self::TOPIC
            . pack('N', 1) . pack('N', 0) . pack('N', 0);
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
            ClientConfig::CLIENT_ID                 => 't4-admin-groups',
            ClientConfig::REQUEST_TIMEOUT_MS        => 10000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
        ];
    }
}
