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
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Protocol\Data\DescribeGroupResponseMetadata;
use Protocol\Kafka\Protocol\Data\ListGroupResponseProtocol;
use Protocol\Kafka\Protocol\Request\ListGroupsRequest;
use Protocol\Kafka\Protocol\Request\ListGroupsRequestV3;
use Protocol\Kafka\Protocol\Request\ListGroupsResponse;
use Protocol\Kafka\Protocol\Request\ListGroupsResponseV3;
use Protocol\Kafka\Tests\Fixture\RawGroupMember;

/**
 * The group states of KIP-518 (Kafka 2.6) against a real Kafka 2.8.2 broker.
 *
 * ListGroups **v4** is the first version of the api whose request carries a field - the `states_filter` that bounds
 * the answer to the groups in one of the named states - and whose answer reports the `group_state` of every entry,
 * the very name a DescribeGroups answer carries. Before it, a listing said nothing about the state of a group and
 * an operator had to describe every one of them to find, say, the empty ones.
 *
 * The groups here are created with {@see RawGroupMember}, which speaks JoinGroup and SyncGroup directly, and every
 * group of this class is named `t3-26-…`. The broker is shared with the other suites and coordinates hundreds of
 * their groups, so every assertion is about the group of the test and never about the whole answer.
 *
 * @see docs/protocol/2.8.md, sections "The group states of KIP-518 (Kafka 2.6)" and "ListGroups API (key 16, v0 to
 *      v4)"
 */
#[CoversClass(ListGroupsRequest::class)]
#[CoversClass(ListGroupsResponse::class)]
#[CoversClass(ListGroupsRequestV3::class)]
#[CoversClass(ListGroupsResponseV3::class)]
#[CoversClass(ListGroupResponseProtocol::class)]
#[CoversClass(AdminClient::class)]
final class GroupStatesApiTest extends IntegrationTestCase
{
    /**
     * Client id of this class, which is also the client id of the raw members it creates
     */
    private const string CLIENT_ID = 'kafka-client-t3-26';

    /**
     * Topic the members subscribe to; it never has to exist for the group apis to work
     */
    private const string TOPIC = 't3-26-states-topic';

    /**
     * Session timeout of the raw members, long enough for a test to look at the group they left behind
     */
    private const int SESSION_TIMEOUT_MS = 30000;

    private Cluster $cluster;

    private AdminClient $admin;

    /**
     * Members the current test created, closed again when it ends
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

        parent::tearDown();
    }

    /**
     * Every entry of a version 4 answer carries the state of that group
     */
    public function testTheListingReportsTheStateOfEveryGroup(): void
    {
        $groupId     = $this->uniqueGroupId();
        $coordinator = $this->admin->findCoordinator($groupId);
        $this->joinGroup($groupId);

        $groups = $this->admin->listGroups($coordinator);

        self::assertArrayHasKey($groupId, $groups);
        self::assertSame(
            DescribeGroupResponseMetadata::STATE_STABLE,
            $groups[$groupId]->groupState,
            'a group whose leader has published its assignment is Stable'
        );
        self::assertSame(
            $this->admin->describeGroup($groupId)->state,
            $groups[$groupId]->groupState,
            'and it is the very state DescribeGroups reports, which is what KIP-518 saves a request for'
        );
    }

    /**
     * The filter of the request keeps the groups in one of the named states and nothing else
     */
    public function testTheStatesFilterKeepsTheGroupsOfThatState(): void
    {
        $groupId     = $this->uniqueGroupId();
        $coordinator = $this->admin->findCoordinator($groupId);
        $this->joinGroup($groupId);

        self::assertArrayHasKey(
            $groupId,
            $this->admin->listGroups($coordinator, [DescribeGroupResponseMetadata::STATE_STABLE]),
            'the group is stable, so the filter of that state keeps it'
        );
        self::assertArrayNotHasKey(
            $groupId,
            $this->admin->listGroups($coordinator, [DescribeGroupResponseMetadata::STATE_EMPTY]),
            'and the filter of another state leaves it out'
        );
    }

    /**
     * A name that is not a state is not an error: the coordinator compares strings and finds no match
     */
    public function testAStateThatIsSpelledDifferentlyIsSimplyNoMatch(): void
    {
        $groupId     = $this->uniqueGroupId();
        $coordinator = $this->admin->findCoordinator($groupId);
        $this->joinGroup($groupId);

        self::assertArrayNotHasKey(
            $groupId,
            $this->admin->listGroups($coordinator, ['stable']),
            'the comparison is case sensitive'
        );
        self::assertSame(
            [],
            $this->admin->listGroups($coordinator, ['NotAState']),
            'and a name no group can ever be in answers an empty list with the error code 0'
        );
    }

    /**
     * The state follows the group through its life: the listing shows it leaving and being empty
     */
    public function testTheStateOfAGroupChangesWithItsMembership(): void
    {
        $groupId     = $this->uniqueGroupId();
        $coordinator = $this->admin->findCoordinator($groupId);
        $member      = $this->joinGroup($groupId);

        self::assertSame(
            DescribeGroupResponseMetadata::STATE_STABLE,
            $this->admin->listGroups($coordinator)[$groupId]->groupState
        );

        $member->leave();

        $empty = $this->admin->listGroups($coordinator, [DescribeGroupResponseMetadata::STATE_EMPTY]);

        self::assertArrayHasKey($groupId, $empty, 'the group outlives its last member as an Empty one');
        self::assertSame(DescribeGroupResponseMetadata::STATE_EMPTY, $empty[$groupId]->groupState);
    }

    /**
     * `listConsumerGroups()` merges the brokers of the cluster and keeps the groups of the `consumer` protocol
     */
    public function testListConsumerGroupsReportsTheConsumerGroupsOfTheWholeCluster(): void
    {
        $groupId = $this->uniqueGroupId();
        $this->joinGroup($groupId);

        $groups = $this->admin->listConsumerGroups([DescribeGroupResponseMetadata::STATE_STABLE]);

        self::assertArrayHasKey($groupId, $groups);
        foreach ($groups as $listedGroupId => $group) {
            self::assertSame(AdminClient::CONSUMER_PROTOCOL_TYPE, $group->protocolType);
            self::assertSame(
                DescribeGroupResponseMetadata::STATE_STABLE,
                $group->groupState,
                'every group of the answer is in one of the states the filter named'
            );
            self::assertSame($listedGroupId, $group->groupId, 'the merged map is indexed by the group id');
        }
    }

    /**
     * The version below reports no state at all, and its request has nowhere to put a filter
     */
    public function testTheVersionThreeAnswerCarriesNoState(): void
    {
        $groupId     = $this->uniqueGroupId();
        $coordinator = $this->admin->findCoordinator($groupId);
        $this->joinGroup($groupId);

        $stream = $coordinator->getConnection($this->configuration());
        new ListGroupsRequestV3(self::CLIENT_ID, 1401)->writeTo($stream);
        $answer = ListGroupsResponseV3::unpack($stream);

        self::assertArrayHasKey($groupId, $answer->groups, 'the listing itself is unchanged');
        self::assertSame('consumer', $answer->groups[$groupId]->protocolType);
        self::assertNull($answer->groups[$groupId]->groupState, 'the field arrived with version 4');
    }

    /**
     * Joins a group with a raw member and leaves it Stable
     */
    private function joinGroup(string $groupId): RawGroupMember
    {
        $member = new RawGroupMember(
            $this->coordinatorAddress($groupId),
            $groupId,
            self::CLIENT_ID,
            self::SESSION_TIMEOUT_MS
        );
        $this->members[] = $member;
        $member->joinAndSync('range', self::subscription(), self::assignment());

        return $member;
    }

    /**
     * The `Subscription` of the consumer protocol: version int16, [topic], user data bytes
     *
     * A 2.x coordinator parses the metadata of every member of a `consumer` group itself, so a member that sends
     * anything else wedges the group in `PreparingRebalance` - see the broker quirks of the document.
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
     * Returns the coordinator of a group as a `host:port` string
     */
    private function coordinatorAddress(string $groupId): string
    {
        $coordinator = $this->admin->findCoordinator($groupId);

        return $coordinator->host . ':' . $coordinator->port;
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
            ClientConfig::REQUEST_TIMEOUT_MS        => 30000,
        ] + ConsumerConfig::getDefaultConfiguration();
    }

    /**
     * Builds a group name that is unique for this test run
     */
    private function uniqueGroupId(): string
    {
        return 't3-26-states-group-' . bin2hex(random_bytes(6));
    }
}
