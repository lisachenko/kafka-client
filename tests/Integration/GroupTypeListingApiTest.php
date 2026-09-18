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
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\MemberAssignment;
use Protocol\Kafka\Consumer\Subscription;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Data\DescribeGroupResponseMetadata;
use Protocol\Kafka\Protocol\Data\ListGroupResponseProtocol;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorRequest;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorRequestV4;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorResponse;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorResponseV4;
use Protocol\Kafka\Protocol\Request\JoinGroupRequest;
use Protocol\Kafka\Protocol\Request\JoinGroupResponse;
use Protocol\Kafka\Protocol\Request\ListGroupsRequest;
use Protocol\Kafka\Protocol\Request\ListGroupsRequestV4;
use Protocol\Kafka\Protocol\Request\ListGroupsResponse;
use Protocol\Kafka\Protocol\Request\ListGroupsResponseV4;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequest;
use Protocol\Kafka\Protocol\Request\OffsetCommitResponse;
use Protocol\Kafka\Protocol\Request\SyncGroupRequest;
use Protocol\Kafka\Protocol\Request\SyncGroupResponse;
use Protocol\Kafka\Tests\Fixture\ConsumerGroupHeartbeatProbe;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;
use RuntimeException;

/**
 * What Kafka 3.8 added to the group apis, against the 3.9.2 KRaft node: **ListGroups v5** and **FindCoordinator v5**.
 *
 * ListGroups v5 (KIP-848) is the second half of the same idea as the version 4 of KIP-518: the request gained a
 * `types_filter` next to its `states_filter`, and every group entry of the answer gained a `group_type` next to
 * its `group_state`. The field exists because the `protocol_type` stopped saying what a group is - a node that
 * runs the new group coordinator holds classic groups and KIP-848 groups side by side, and the protocol type of
 * both is the string `consumer`.
 *
 * Every rule of that filter is driven here against the node: the two types the node writes, the filter that names
 * one of them, the filter that names both, the upper-case name that matches because the filter is *parsed* rather
 * than compared, the type the enum does not know, which is an empty answer and never an error, the two filters
 * combined with **and**, the version 4 answer of the same question, which has no type at all, and the group that
 * keeps its type after its last member has left.
 *
 * FindCoordinator v5 (KIP-890) added no field to either half of the api - it is the client's promise to
 * understand the error code 120 `TransactionAbortable` - so what is driven here is that the version this client
 * sends is 5, that the batch of KIP-699 and the transaction lookup still work at it, and that the two classes of
 * the versions 4 and 5 declare the very same body.
 *
 * The KIP-848 group is created with a hand-built **ConsumerGroupHeartbeat** (key 68,
 * {@see ConsumerGroupHeartbeatProbe}, shared with the 3.6 and 3.7 waves): the api itself is the last wave of this
 * line and has no classes yet.
 *
 * The node is shared with three other agents, so every listing is asserted as a **superset**: the groups of this
 * class have to be in it with the right type, and the ones a filter excludes have to be out of it. Every group
 * and topic carries the `t3-38-` prefix of the Kafka 3.8 wave and is removed again in
 * {@see self::tearDownAfterClass()} - every KIP-848 member with the leave heartbeat of the epoch -1 first,
 * because a group that still holds a member is not deletable.
 *
 * @see docs/protocol/3.9.md, section "The group types of KIP-848 (Kafka 3.8)"
 * @see docs/protocol/3.9.md, section "ListGroups API (key 16, v0 to v5)"
 * @see docs/protocol/3.9.md, section "GroupCoordinator API (key 10, v0 to v5)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(ListGroupsRequest::class)]
#[CoversClass(ListGroupsRequestV4::class)]
#[CoversClass(ListGroupsResponse::class)]
#[CoversClass(ListGroupsResponseV4::class)]
#[CoversClass(ListGroupResponseProtocol::class)]
#[CoversClass(GroupCoordinatorRequest::class)]
#[CoversClass(GroupCoordinatorRequestV4::class)]
#[CoversClass(GroupCoordinatorResponse::class)]
#[CoversClass(GroupCoordinatorResponseV4::class)]
final class GroupTypeListingApiTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t3-38';

    private const string TOPIC_PREFIX = 't3-38-listing';

    private const string PROTOCOL_TYPE = 'consumer';

    private const string PROTOCOL_NAME = 'range';

    /**
     * `group.min.session.timeout.ms` of the node is 1000, `group.max.session.timeout.ms` 60000
     */
    private const int SESSION_TIMEOUT_MS = 6000;

    private const int REBALANCE_TIMEOUT_MS = 8000;

    /**
     * A JoinGroup of a fresh group waits the 3 s of `group.initial.rebalance.delay.ms`
     */
    private const int REQUEST_TIMEOUT_MS = 30000;

    /**
     * The offset every group of this class commits, so that it outlives its last member
     */
    private const int COMMITTED_OFFSET = 38;

    private static ?Cluster $sharedCluster = null;

    /**
     * Every group this class created
     *
     * @var list<string>
     */
    private static array $groups = [];

    /**
     * Every classic member this class left in a group, as `[group id, member id]`
     *
     * @var list<array{string, string}>
     */
    private static array $members = [];

    /**
     * Every KIP-848 member this class left in a group, as `[group id, member id]`
     *
     * @var list<array{string, string}>
     */
    private static array $modernMembers = [];

    /**
     * Takes every member of this class out of its group first: a group that still holds one is not deletable
     */
    public static function tearDownAfterClass(): void
    {
        foreach (self::$modernMembers as [$groupId, $memberId]) {
            self::leaveModernQuietly($groupId, $memberId);
        }
        self::$modernMembers = [];

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
     * The listing of this client is the version 5, and every entry of it names the type of its group
     */
    public function testTheListingNamesTheTypeOfEveryGroup(): void
    {
        $topic    = $this->topic();
        $classic  = $this->uniqueGroupName();
        $modern   = $this->uniqueGroupName();
        $this->classicGroup($classic, $topic);
        $this->modernGroup($modern, $topic);

        $groups = $this->admin()->listGroups($this->coordinator($classic));

        self::assertSame(5, ListGroupsRequest::VERSION, 'the version this client sends since Kafka 3.8');
        self::assertSame(5, ListGroupsResponse::VERSION);
        self::assertArrayHasKey($classic, $groups);
        self::assertArrayHasKey($modern, $groups);
        self::assertSame(
            ListGroupResponseProtocol::TYPE_CLASSIC,
            $groups[$classic]->groupType,
            'a group that joined with JoinGroup and SyncGroup is `classic`'
        );
        self::assertSame(
            ListGroupResponseProtocol::TYPE_CONSUMER,
            $groups[$modern]->groupType,
            'a group a ConsumerGroupHeartbeat created is `consumer`'
        );
        self::assertSame(
            self::PROTOCOL_TYPE,
            $groups[$classic]->protocolType,
            'the protocol type of both is the same `consumer`, which is why the type field exists'
        );
        self::assertSame(self::PROTOCOL_TYPE, $groups[$modern]->protocolType);
    }

    /**
     * The types filter is applied by the coordinator, one type at a time or several at once
     */
    public function testTheTypesFilterBoundsTheListingToTheNamedTypes(): void
    {
        $topic   = $this->topic();
        $classic = $this->uniqueGroupName();
        $modern  = $this->uniqueGroupName();
        $this->classicGroup($classic, $topic);
        $this->modernGroup($modern, $topic);
        $node = $this->coordinator($classic);

        $consumerGroups = $this->admin()->listGroups($node, [], [ListGroupResponseProtocol::TYPE_CONSUMER]);

        self::assertArrayHasKey($modern, $consumerGroups);
        self::assertArrayNotHasKey($classic, $consumerGroups, 'the classic group is filtered out by the type');

        $classicGroups = $this->admin()->listGroups($node, [], [ListGroupResponseProtocol::TYPE_CLASSIC]);

        self::assertArrayHasKey($classic, $classicGroups);
        self::assertArrayNotHasKey($modern, $classicGroups);

        $both = $this->admin()->listGroups(
            $node,
            [],
            [ListGroupResponseProtocol::TYPE_CLASSIC, ListGroupResponseProtocol::TYPE_CONSUMER]
        );

        self::assertArrayHasKey($classic, $both, 'naming every type is the same question as naming none');
        self::assertArrayHasKey($modern, $both);
    }

    /**
     * `Group.GroupType.parse()` lower-cases the name, so the types filter is case insensitive
     */
    public function testTheTypesFilterIsCaseInsensitive(): void
    {
        $topic  = $this->topic();
        $modern = $this->uniqueGroupName();
        $this->modernGroup($modern, $topic);

        $groups = $this->admin()->listGroups($this->coordinator($modern), [], ['CONSUMER']);

        self::assertArrayHasKey($modern, $groups, 'the filter is parsed, not compared');
        self::assertSame(
            ListGroupResponseProtocol::TYPE_CONSUMER,
            $groups[$modern]->groupType,
            'the type of the answer is the lower-case name the enum prints, whatever case the filter had'
        );
    }

    /**
     * A type the enum does not define parses to `UNKNOWN`, which no group has: an empty answer, not an error
     */
    public function testATypeTheEnumDoesNotKnowIsAnEmptyAnswer(): void
    {
        $topic   = $this->topic();
        $classic = $this->uniqueGroupName();
        $modern  = $this->uniqueGroupName();
        $this->classicGroup($classic, $topic);
        $this->modernGroup($modern, $topic);
        $node = $this->coordinator($classic);

        foreach (['streams', '', ListGroupResponseProtocol::TYPE_UNKNOWN] as $type) {
            $groups = $this->admin()->listGroups($node, [], [$type]);

            self::assertSame([], $groups, "the type '{$type}' matches no group at all and is not an error");
        }

        // and an unknown entry next to a known one costs the known one nothing
        $mixed = $this->admin()->listGroups(
            $node,
            [],
            [ListGroupResponseProtocol::TYPE_CONSUMER, 'nonsense']
        );

        self::assertArrayHasKey($modern, $mixed);
        self::assertArrayNotHasKey($classic, $mixed);
    }

    /**
     * The states filter of KIP-518 and the types filter of KIP-848 are combined with `and`
     */
    public function testTheStatesFilterAndTheTypesFilterAreCombinedWithAnd(): void
    {
        $topic   = $this->topic();
        $classic = $this->uniqueGroupName();
        $modern  = $this->uniqueGroupName();
        $this->classicGroup($classic, $topic);
        $this->modernGroup($modern, $topic);
        $node = $this->coordinator($classic);

        $stableClassic = $this->admin()->listGroups(
            $node,
            [DescribeGroupResponseMetadata::STATE_STABLE],
            [ListGroupResponseProtocol::TYPE_CLASSIC]
        );

        self::assertArrayHasKey($classic, $stableClassic);
        self::assertArrayNotHasKey($modern, $stableClassic, 'the type excludes it, although the state matches');

        $emptyClassic = $this->admin()->listGroups(
            $node,
            [DescribeGroupResponseMetadata::STATE_EMPTY],
            [ListGroupResponseProtocol::TYPE_CLASSIC]
        );

        self::assertArrayNotHasKey($classic, $emptyClassic, 'the state excludes it, although the type matches');
    }

    /**
     * The version below has no types filter and no `group_type` at all - the cheapest proof of what v5 added
     */
    public function testTheVersionFourListingCarriesNoGroupType(): void
    {
        $topic   = $this->topic();
        $classic = $this->uniqueGroupName();
        $this->classicGroup($classic, $topic);

        $stream = $this->coordinatorStream($classic);
        new ListGroupsRequestV4(self::CLIENT_ID, 3810, [DescribeGroupResponseMetadata::STATE_STABLE])
            ->writeTo($stream);
        $answer = ListGroupsResponseV4::unpack($stream);

        self::assertSame(KafkaException::NO_ERROR, $answer->errorCode);
        self::assertArrayHasKey($classic, $answer->groups);
        self::assertNull(
            $answer->groups[$classic]->groupType,
            'the field is not on the wire of version 4, so it stays at its null default'
        );
        self::assertSame(
            DescribeGroupResponseMetadata::STATE_STABLE,
            $answer->groups[$classic]->groupState,
            'the group state of KIP-518 is there, as it is since Kafka 2.6'
        );
    }

    /**
     * A KIP-848 group keeps its type once its last member is gone: the type belongs to the group, not the member
     */
    public function testAnEmptyKip848GroupKeepsItsType(): void
    {
        $topic  = $this->topic();
        $modern = $this->uniqueGroupName();
        [$memberId] = $this->modernGroup($modern, $topic);

        new ConsumerGroupHeartbeatProbe(self::firstBootstrapServer())->leave($modern, $memberId, 3811);

        $node  = $this->coordinator($modern);
        $entry = null;
        for ($attempt = 0; $attempt < 100; $attempt++) {
            $groups = $this->admin()->listGroups(
                $node,
                [DescribeGroupResponseMetadata::STATE_EMPTY],
                [ListGroupResponseProtocol::TYPE_CONSUMER]
            );
            if (isset($groups[$modern])) {
                $entry = $groups[$modern];

                break;
            }
            usleep(100000);
        }

        self::assertNotNull($entry, 'the group that lost its member is still listed, with its committed offset');
        self::assertSame(DescribeGroupResponseMetadata::STATE_EMPTY, $entry->groupState);
        self::assertSame(
            ListGroupResponseProtocol::TYPE_CONSUMER,
            $entry->groupType,
            'an empty group of the new protocol stays a `consumer` group'
        );
    }

    /**
     * Every coordinator lookup of this client is the version 5 of KIP-890, which added no field at all
     */
    public function testTheCoordinatorLookupIsTheVersionFiveOfKip890(): void
    {
        $topic   = $this->topic();
        $classic = $this->uniqueGroupName();
        $this->classicGroup($classic, $topic);

        self::assertSame(5, GroupCoordinatorRequest::VERSION, 'the version this client sends since Kafka 3.8');
        self::assertSame(5, GroupCoordinatorResponse::VERSION);
        self::assertSame(
            GroupCoordinatorRequestV4::getScheme(),
            GroupCoordinatorRequest::getScheme(),
            'KIP-890 raised the number and not the body'
        );

        $node = $this->coordinator($classic);

        self::assertGreaterThan(0, $node->nodeId, 'the one broker of this node is the id 1, never 0');

        $stream = $this->connect();
        new GroupCoordinatorRequest($classic, GroupCoordinatorRequest::COORDINATOR_TYPE_GROUP, self::CLIENT_ID, 3820)
            ->writeTo($stream);
        $answer = GroupCoordinatorResponse::unpack($stream);
        $entry  = $answer->coordinatorOf($classic);

        self::assertSame(KafkaException::NO_ERROR, $entry->errorCode);
        self::assertSame($node->nodeId, $entry->nodeId);
        self::assertSame(
            '',
            $entry->errorMessage,
            'every v4-and-above entry of this node carries the empty-string default, never "NONE"'
        );

        // and the version 4 frame of the same question is answered exactly the same way
        new GroupCoordinatorRequestV4($classic, GroupCoordinatorRequest::COORDINATOR_TYPE_GROUP, self::CLIENT_ID, 3821)
            ->writeTo($stream);
        $below = GroupCoordinatorResponseV4::unpack($stream);

        self::assertSame($node->nodeId, $below->coordinatorOf($classic)->nodeId);
        self::assertSame(KafkaException::NO_ERROR, $below->coordinatorOf($classic)->errorCode);
    }

    /**
     * The batch of KIP-699 and the transaction lookup of KIP-98 are untouched by the version 5
     */
    public function testTheBatchedAndTheTransactionLookupStillWorkAtVersionFive(): void
    {
        $topic   = $this->topic();
        $classic = $this->uniqueGroupName();
        $modern  = $this->uniqueGroupName();
        $this->classicGroup($classic, $topic);
        $this->modernGroup($modern, $topic);

        $coordinators = new CoordinatorLookup($this->cluster(), $this->configuration())->findCoordinators(
            [$classic, $modern],
            GroupCoordinatorRequest::COORDINATOR_TYPE_GROUP
        );

        self::assertSame([$classic, $modern], array_keys($coordinators));
        self::assertSame($coordinators[$classic]->nodeId, $coordinators[$modern]->nodeId);

        $transactional = new CoordinatorLookup($this->cluster(), $this->configuration())->findCoordinator(
            't3-38-listing-tx-' . bin2hex(random_bytes(4)),
            GroupCoordinatorRequest::COORDINATOR_TYPE_TRANSACTION
        );

        self::assertSame(
            $coordinators[$classic]->nodeId,
            $transactional->nodeId,
            'one node serves both internal topics of this container'
        );
    }

    /**
     * Creates a classic group with one member that joined, synced and committed an offset
     */
    private function classicGroup(string $groupId, string $topic): void
    {
        $stream       = $this->coordinatorStream($groupId);
        $subscription = new Subscription([$topic])->pack();

        new JoinGroupRequest(
            $groupId,
            self::SESSION_TIMEOUT_MS,
            self::REBALANCE_TIMEOUT_MS,
            JoinGroupRequest::DEFAULT_MEMBER_ID,
            self::PROTOCOL_TYPE,
            [self::PROTOCOL_NAME => $subscription],
            self::CLIENT_ID,
            3800,
            null,
            'the t3-38 suite started'
        )->writeTo($stream);
        $refused = JoinGroupResponse::unpack($stream);

        self::assertSame(KafkaException::MEMBER_ID_REQUIRED, $refused->errorCode, 'the 79 of KIP-394');

        new JoinGroupRequest(
            $groupId,
            self::SESSION_TIMEOUT_MS,
            self::REBALANCE_TIMEOUT_MS,
            $refused->memberId,
            self::PROTOCOL_TYPE,
            [self::PROTOCOL_NAME => $subscription],
            self::CLIENT_ID,
            3801,
            null,
            'need to re-join with the given member-id: ' . $refused->memberId
        )->writeTo($stream);
        $joined = JoinGroupResponse::unpack($stream);

        self::assertSame(KafkaException::NO_ERROR, $joined->errorCode, 'The node refused the join');

        new SyncGroupRequest(
            $groupId,
            $joined->generationId,
            $joined->memberId,
            [$joined->memberId => new MemberAssignment([$topic => [0]])->pack()],
            self::CLIENT_ID,
            3802,
            null,
            $joined->protocolType,
            $joined->groupProtocol
        )->writeTo($stream);
        $synced = SyncGroupResponse::unpack($stream);

        self::assertSame(KafkaException::NO_ERROR, $synced->errorCode, 'The node refused the sync');

        self::$members[] = [$groupId, $joined->memberId];

        $this->commit($stream, $groupId, $joined->generationId, $joined->memberId, $topic, 3803);
    }

    /**
     * Creates a group of the KIP-848 protocol with one member and commits one offset as that member
     *
     * @return array{string, int} The member id and the member epoch the coordinator answered with
     */
    private function modernGroup(string $groupId, string $topic): array
    {
        $memberId = ConsumerGroupHeartbeatProbe::newMemberId();
        $epoch    = new ConsumerGroupHeartbeatProbe(self::firstBootstrapServer())
            ->join($groupId, $memberId, [$topic], self::REBALANCE_TIMEOUT_MS, 3804);

        self::$modernMembers[] = [$groupId, $memberId];

        self::assertGreaterThan(0, $epoch, 'a member that joined holds an epoch above zero');

        $this->commit($this->coordinatorStream($groupId), $groupId, $epoch, $memberId, $topic, 3805);

        return [$memberId, $epoch];
    }

    /**
     * Commits {@see self::COMMITTED_OFFSET} for the one partition of the topic, as the given member
     */
    private function commit(
        Stream $stream,
        string $groupId,
        int $generationId,
        string $memberId,
        string $topic,
        int $correlationId
    ): void {
        new OffsetCommitRequest(
            $groupId,
            $generationId,
            $memberId,
            OffsetCommitRequest::DEFAULT_RETENTION_TIME,
            [$topic => [0 => self::COMMITTED_OFFSET]],
            self::CLIENT_ID,
            $correlationId
        )->writeTo($stream);
        $answer = OffsetCommitResponse::unpack($stream);

        self::assertSame(
            KafkaException::NO_ERROR,
            $answer->topics[$topic]->partitions[0]->errorCode,
            'The node refused the commit that keeps this group alive'
        );
    }

    /**
     * Creates the one topic of a test and waits until every partition of it has a leader
     */
    private function topic(): string
    {
        $topic = self::uniqueTopicName(self::TOPIC_PREFIX);

        $this->admin()->createTopics([new NewTopic($topic, 1, 1)]);
        new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, self::TOPIC_PREFIX)
            ->awaitTopicWithLeaders($topic);

        return $topic;
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
        $configuration = $this->configuration();

        return new CoordinatorLookup($this->cluster(), $configuration)
            ->findCoordinator($groupId)
            ->getConnection($configuration);
    }

    private function cluster(): Cluster
    {
        return self::$sharedCluster ??= Cluster::bootstrap($this->configuration());
    }

    /**
     * @return array<string, mixed> Client configuration for this test class
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

            ConsumerConfig::SESSION_TIMEOUT_MS      => self::SESSION_TIMEOUT_MS,
            ConsumerConfig::MAX_POLL_INTERVAL_MS    => self::REBALANCE_TIMEOUT_MS,
        ] + ConsumerConfig::getDefaultConfiguration();
    }

    /**
     * Builds a group name that is unique for this run and is removed when the class is done
     */
    private function uniqueGroupName(): string
    {
        $groupId        = 't3-38-listing-' . bin2hex(random_bytes(6));
        self::$groups[] = $groupId;

        return $groupId;
    }

    /**
     * Takes one classic member of this class out of its group, ignoring a member or a group that is gone already
     */
    private static function leaveQuietly(string $groupId, string $memberId): void
    {
        try {
            $configuration = self::cleanupConfiguration();
            $cluster       = Cluster::bootstrap($configuration);

            new Client($cluster, $configuration)->leaveGroup(
                new CoordinatorLookup($cluster, $configuration)->findCoordinator($groupId),
                $groupId,
                $memberId,
                null,
                'the t3-38 suite is done'
            );
        } catch (KafkaException) {
            // A member the session timeout has already reaped, or a group that is gone, must not fail the suite
        }
    }

    /**
     * Takes one KIP-848 member out of its group with the leave heartbeat of the epoch -1
     */
    private static function leaveModernQuietly(string $groupId, string $memberId): void
    {
        try {
            new ConsumerGroupHeartbeatProbe(self::firstBootstrapServer())->leave($groupId, $memberId, 3899);
        } catch (RuntimeException) {
            // A member the session timeout has already reaped must not fail the suite
        }
    }

    /**
     * Removes a group of this class from the coordinator, ignoring a group that is gone or not empty any more
     */
    private static function deleteGroupQuietly(string $groupId): void
    {
        try {
            $configuration = self::cleanupConfiguration();

            new AdminClient(Cluster::bootstrap($configuration), $configuration)->deleteConsumerGroups([$groupId]);
        } catch (KafkaException) {
            // A group that is gone, or one the reaper has not emptied yet, must not fail the suite
        }
    }

    /**
     * @return array<string, mixed> Configuration of the connections that clean the shared node up again
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
