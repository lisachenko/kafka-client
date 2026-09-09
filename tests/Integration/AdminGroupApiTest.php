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
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Protocol\Data\DescribeGroupResponseMember;
use Protocol\Kafka\Protocol\Data\DescribeGroupResponseMetadata;
use Protocol\Kafka\Protocol\Data\ListGroupResponseProtocol;
use Protocol\Kafka\Protocol\Request\DescribeGroupsRequest;
use Protocol\Kafka\Protocol\Request\DescribeGroupsResponse;
use Protocol\Kafka\Protocol\Request\ListGroupsRequest;
use Protocol\Kafka\Protocol\Request\ListGroupsResponse;
use Protocol\Kafka\Tests\Fixture\RawGroupMember;

/**
 * Exercises the group admin apis of Kafka 0.9 - ListGroups (16) and DescribeGroups (15) - against a real broker.
 *
 * The groups these tests look at are created with {@see RawGroupMember}, which speaks the JoinGroup and SyncGroup
 * requests of the membership protocol directly: a group only exists on the coordinator while it has a member, and
 * the client classes of those two apis live on another branch of this line.
 *
 * The broker is shared with the other suites and coordinates their groups too, so every assertion here is about the
 * groups of this class and never about the whole answer.
 *
 * @see docs/protocol/0.10.2.md, sections "DescribeGroups API (key 15, v0)" and "ListGroups API (key 16, v0)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(DescribeGroupsRequest::class)]
#[CoversClass(DescribeGroupsResponse::class)]
#[CoversClass(DescribeGroupResponseMetadata::class)]
#[CoversClass(DescribeGroupResponseMember::class)]
#[CoversClass(ListGroupsRequest::class)]
#[CoversClass(ListGroupsResponse::class)]
#[CoversClass(ListGroupResponseProtocol::class)]
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

    public function testDescribeGroupReportsAwaitingSyncWhileTheLeaderHasNotPublishedTheAssignment(): void
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

        self::assertSame(DescribeGroupResponseMetadata::STATE_AWAITING_SYNC, $group->state);
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

    public function testAGroupIsForgottenWhenItsLastMemberLeaves(): void
    {
        $groupId = $this->uniqueGroupId();
        $member  = $this->joinGroup($groupId, 't4-leaving-member');

        $member->leave();

        $coordinator = $this->admin->findCoordinator($groupId);
        self::assertArrayNotHasKey(
            $groupId,
            $this->admin->listGroups($coordinator),
            'the coordinator drops a group that has no members left'
        );
        self::assertSame(
            DescribeGroupResponseMetadata::STATE_DEAD,
            $this->admin->describeGroup($groupId)->state,
            'and describes it exactly like a group it has never heard of'
        );
    }

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
