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
use Protocol\Kafka\Admin\MemberToRemove;
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\CoordinatorLookup;
use Protocol\Kafka\Common\Errors\FencedInstanceIdException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\UnknownMemberIdException;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\Internals\ConsumerCoordinator;
use Protocol\Kafka\Consumer\KafkaConsumer;
use Protocol\Kafka\Consumer\MemberAssignment;
use Protocol\Kafka\Consumer\OffsetResetStrategy;
use Protocol\Kafka\Consumer\Subscription;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Data\DescribeGroupResponseMetadata;
use Protocol\Kafka\Protocol\Data\LeaveGroupRequestMember;
use Protocol\Kafka\Protocol\Data\LeaveGroupResponseMember;
use Protocol\Kafka\Protocol\Request\DescribeGroupsRequest;
use Protocol\Kafka\Protocol\Request\DescribeGroupsRequestV2;
use Protocol\Kafka\Protocol\Request\DescribeGroupsResponse;
use Protocol\Kafka\Protocol\Request\DescribeGroupsResponseV2;
use Protocol\Kafka\Protocol\Request\HeartbeatRequest;
use Protocol\Kafka\Protocol\Request\HeartbeatResponse;
use Protocol\Kafka\Protocol\Request\JoinGroupRequest;
use Protocol\Kafka\Protocol\Request\JoinGroupResponse;
use Protocol\Kafka\Protocol\Request\LeaveGroupRequest;
use Protocol\Kafka\Protocol\Request\LeaveGroupResponse;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequest;
use Protocol\Kafka\Protocol\Request\OffsetCommitResponse;
use Protocol\Kafka\Protocol\Request\SyncGroupRequest;
use Protocol\Kafka\Protocol\Request\SyncGroupResponse;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * Static membership (KIP-345) and the authorized operations of a group (KIP-430) against a real Kafka 2.8.2 broker.
 *
 * Kafka 2.3 gave a consumer a name of its own, `group.instance.id`, which travels in the `group_instance_id` of
 * **JoinGroup v5, SyncGroup v3, Heartbeat v3 and OffsetCommit v7** and makes the coordinator remember the member
 * behind that name: a consumer that restarts joins under the very same identity, keeps its partitions and costs
 * the group no rebalance, and a second consumer that takes the identity over fences the first one for good with
 * the error code **82** (`FencedInstanceId`). The same release gave DescribeGroups its version 3, whose
 * `include_authorized_operations` asks the broker which operations the client may perform on each group.
 *
 * Kafka 2.4 added the half that makes static membership operable: the **batch LeaveGroup v3** of the same KIP,
 * with which an administrator removes a static instance that is not coming back -
 * {@see \Protocol\Kafka\Admin\AdminClient::removeMembersFromConsumerGroup()} - and which a member that removes
 * itself sends as a batch of one.
 *
 * Every group of this class is named `t3-345-…` and every consumer instance `t3-345-…`, so that the tests can run
 * next to the other suites on the shared container.
 *
 * @see docs/protocol/2.8.md, sections "Static membership (KIP-345)", "The authorized operations of a group (v3,
 *      KIP-430)", "JoinGroup API (key 11, v0 to v5)", "SyncGroup API (key 14, v0 to v3)", "Heartbeat API (key 12,
 *      v0 to v3)", "OffsetCommit API (key 8, v0 to v7)", "DescribeGroups API (key 15, v0 to v3)" and
 *      "The batch leave of KIP-345 (v3)"
 */
#[CoversClass(JoinGroupRequest::class)]
#[CoversClass(JoinGroupResponse::class)]
#[CoversClass(SyncGroupRequest::class)]
#[CoversClass(SyncGroupResponse::class)]
#[CoversClass(HeartbeatRequest::class)]
#[CoversClass(HeartbeatResponse::class)]
#[CoversClass(OffsetCommitRequest::class)]
#[CoversClass(DescribeGroupsRequest::class)]
#[CoversClass(DescribeGroupsResponse::class)]
#[CoversClass(DescribeGroupResponseMetadata::class)]
#[CoversClass(ConsumerCoordinator::class)]
#[CoversClass(KafkaConsumer::class)]
#[CoversClass(LeaveGroupRequest::class)]
#[CoversClass(LeaveGroupResponse::class)]
#[CoversClass(LeaveGroupRequestMember::class)]
#[CoversClass(LeaveGroupResponseMember::class)]
#[CoversClass(MemberToRemove::class)]
final class StaticMembershipApiTest extends IntegrationTestCase
{
    /**
     * Client id of this class, which is the prefix of the member id of a DYNAMIC member
     */
    private const string CLIENT_ID = 'kafka-client-t3-345';

    /**
     * Protocol type of a consumer group, whose member metadata a 2.x coordinator parses itself
     */
    private const string PROTOCOL_TYPE = 'consumer';

    /**
     * Name of the assignor the members of these groups offer
     */
    private const string PROTOCOL_NAME = 'range';

    /**
     * Session timeout of the members here; `group.min.session.timeout.ms` of the container is 1000
     */
    private const int SESSION_TIMEOUT_MS = 10000;

    /**
     * Rebalance timeout of the members here, which is also the deadline of their SyncGroup (KAFKA-9752)
     */
    private const int REBALANCE_TIMEOUT_MS = 15000;

    /**
     * Read timeout of the sockets, which has to cover a JoinGroup that waits for a whole rebalance
     */
    private const int REQUEST_TIMEOUT_MS = 30000;

    /**
     * How long a poll loop keeps going before the expected state of the consumer is given up on, in seconds
     */
    private const float POLL_TIMEOUT = 30.0;

    /**
     * How long to wait for a freshly created partition to start serving requests, in seconds
     */
    private const float TOPIC_TIMEOUT = 30.0;

    /**
     * The `authorized_operations` a broker without an authorizer reports for a group: READ, DELETE and DESCRIBE
     *
     * `AclEntry.supportedOperations(GROUP)` @ 2.8.2 is `{READ, DESCRIBE, DELETE}` and `AuthHelper` answers all of
     * them when there is no authorizer; the codes of `AclOperation` are 3, 6 and 8, so the bit set is 328.
     */
    private const int GROUP_OPERATIONS = (1 << 3) | (1 << 6) | (1 << 8);

    /**
     * The cluster is resolved once: every test of this class talks to the same brokers
     */
    private static ?Cluster $sharedCluster = null;

    /**
     * Topic of the current test, created with its three partitions by {@see self::setUp()}
     */
    private string $topic;

    /**
     * Consumers the current test built, released again when it ends
     *
     * @var list<KafkaConsumer>
     */
    private array $consumers = [];

    protected function setUp(): void
    {
        parent::setUp();

        $this->topic = self::uniqueTopicName('t3-345');
        new TopicMetadataProbe(fn(): Stream => $this->connect(), self::TOPIC_TIMEOUT, self::CLIENT_ID)
            ->awaitTopicWithLeaders($this->topic);
    }

    protected function tearDown(): void
    {
        foreach ($this->consumers as $consumer) {
            try {
                $consumer->unsubscribe();
            } catch (KafkaException) {
                // A fenced consumer can not speak to the coordinator any more, which is what its test proved
            }
        }
        $this->consumers = [];

        // The topic of the test is not needed afterwards, and a shared container that collects the debris of
        // thousands of test runs is what makes its controller and its log directories give up
        try {
            new AdminClient($this->cluster(), $this->configuration())->deleteTopics([$this->topic]);
        } catch (KafkaException) {
            // A broker that can not delete the topic right now must not fail the test that just passed
        }

        parent::tearDown();
    }

    /**
     * A join that names an instance id is never refused with the 79 of KIP-394
     *
     * `requireKnownMemberId` is `version >= 4 && groupInstanceId.isEmpty` in `KafkaApis.handleJoinGroupRequest`
     * @ 2.8.2, so a static member is added to the group with its very first request - and the id the coordinator
     * generates for it is built from the **instance id**, not from the client id.
     */
    public function testAStaticFirstJoinIsAddedToTheGroupWithoutTheMemberIdRequiredCode(): void
    {
        $groupId  = self::uniqueGroupName();
        $instance = self::uniqueInstanceId();
        $stream   = $this->coordinatorStream($groupId);

        $joined = $this->join($stream, $groupId, JoinGroupRequest::DEFAULT_MEMBER_ID, $instance, 701);

        self::assertSame(KafkaException::NO_ERROR, $joined->errorCode, 'no 79 for a member that names itself');
        self::assertSame(1, $joined->generationId, 'the first generation of the group');
        self::assertStringStartsWith($instance . '-', $joined->memberId, 'the member id is "<instance id>-<uuid>"');
        self::assertSame($joined->memberId, $joined->leaderId);
        self::assertSame(
            [$joined->memberId => $instance],
            array_map(static fn($member): ?string => $member->groupInstanceId, $joined->members),
            'version 5 gives every entry of the member array the instance id of that member'
        );

        // A DYNAMIC join of the very same version is still refused once, which is what the flag switches
        $refused = $this->join($stream, $groupId, JoinGroupRequest::DEFAULT_MEMBER_ID, null, 702);

        self::assertSame(KafkaException::MEMBER_ID_REQUIRED, $refused->errorCode);
        self::assertStringStartsWith(self::CLIENT_ID . '-', $refused->memberId, '"<client id>-<uuid>" for it');
    }

    /**
     * The point of the KIP: a static member that comes back keeps its assignment, and the group its generation
     */
    public function testAStaticMemberThatRejoinsKeepsItsGenerationAndItsAssignment(): void
    {
        $groupId  = self::uniqueGroupName();
        $instance = self::uniqueInstanceId();
        $stream   = $this->coordinatorStream($groupId);

        $joined = $this->join($stream, $groupId, JoinGroupRequest::DEFAULT_MEMBER_ID, $instance, 711);
        $this->sync($stream, $groupId, $joined, $instance, 712);

        // The same instance joins again, from another connection and without a member id, as a restart would
        $rejoinStream = $this->newCoordinatorStream($groupId);
        $started      = microtime(true);
        $rejoined     = $this->join($rejoinStream, $groupId, JoinGroupRequest::DEFAULT_MEMBER_ID, $instance, 713);
        $tookMs       = (int) ((microtime(true) - $started) * 1000);

        self::assertSame(KafkaException::NO_ERROR, $rejoined->errorCode);
        self::assertSame(
            $joined->generationId,
            $rejoined->generationId,
            'the group did not rebalance: a static rejoin that does not change the protocol keeps the generation'
        );
        self::assertNotSame($joined->memberId, $rejoined->memberId, 'the coordinator hands out a new member id');
        self::assertStringStartsWith($instance . '-', $rejoined->memberId);
        self::assertLessThan(
            self::REBALANCE_TIMEOUT_MS,
            $tookMs,
            'the answer is immediate, the coordinator does not wait for a rebalance that never starts'
        );

        // And the assignment of the old member id is handed to the new one
        $assignment = $this->sync($rejoinStream, $groupId, $rejoined, $instance, 714);

        self::assertSame(
            [$this->topic => [0, 1, 2]],
            MemberAssignment::unpack($assignment->memberAssignment)->partitions(),
            'the restarted instance gets its partitions back without a rebalance'
        );
    }

    /**
     * Two live consumers can never share one instance id: the second one to join fences the first
     */
    public function testASecondMemberUnderTheSameInstanceIdFencesTheFirstOne(): void
    {
        $groupId  = self::uniqueGroupName();
        $instance = self::uniqueInstanceId();
        $stream   = $this->coordinatorStream($groupId);

        $first = $this->join($stream, $groupId, JoinGroupRequest::DEFAULT_MEMBER_ID, $instance, 721);
        $this->sync($stream, $groupId, $first, $instance, 722);

        $secondStream = $this->newCoordinatorStream($groupId);
        $second       = $this->join($secondStream, $groupId, JoinGroupRequest::DEFAULT_MEMBER_ID, $instance, 723);

        self::assertSame(KafkaException::NO_ERROR, $second->errorCode);
        self::assertSame(
            $first->memberId,
            $second->leaderId,
            'the answer still names the old member id as the leader: `leaderOrNull` is read before the swap'
        );

        // Every request of the older member is answered 82 from here on
        new HeartbeatRequest($groupId, $first->generationId, $first->memberId, self::CLIENT_ID, 724, $instance)
            ->writeTo($stream);

        self::assertSame(
            KafkaException::FENCED_INSTANCE_ID,
            HeartbeatResponse::unpack($stream)->errorCode,
            'the heartbeat of the fenced member'
        );

        new SyncGroupRequest($groupId, $first->generationId, $first->memberId, [], self::CLIENT_ID, 725, $instance)
            ->writeTo($stream);

        self::assertSame(KafkaException::FENCED_INSTANCE_ID, SyncGroupResponse::unpack($stream)->errorCode);

        new OffsetCommitRequest(
            $groupId,
            $first->generationId,
            $first->memberId,
            OffsetCommitRequest::DEFAULT_RETENTION_TIME,
            [$this->topic => [0 => 9]],
            self::CLIENT_ID,
            726,
            $instance
        )->writeTo($stream);

        self::assertSame(
            KafkaException::FENCED_INSTANCE_ID,
            OffsetCommitResponse::unpack($stream)->topics[$this->topic]->partitions[0]->errorCode,
            'the commit reports the code per partition, as every error of that api does'
        );
    }

    /**
     * KIP-430: the operations of a group are reported only when the version 3 request asks for them
     */
    public function testTheAuthorizedOperationsOfAGroupAreOnlyReportedWhenTheyAreAskedFor(): void
    {
        $groupId  = self::uniqueGroupName();
        $instance = self::uniqueInstanceId();
        $stream   = $this->coordinatorStream($groupId);

        $joined = $this->join($stream, $groupId, JoinGroupRequest::DEFAULT_MEMBER_ID, $instance, 731);
        $this->sync($stream, $groupId, $joined, $instance, 732);

        new DescribeGroupsRequest([$groupId], self::CLIENT_ID, 733, true)->writeTo($stream);
        $withOperations = DescribeGroupsResponse::unpack($stream)->groups[$groupId];

        self::assertSame(KafkaException::NO_ERROR, $withOperations->errorCode);
        self::assertSame(DescribeGroupResponseMetadata::STATE_STABLE, $withOperations->state);
        self::assertSame(
            self::GROUP_OPERATIONS,
            $withOperations->authorizedOperations,
            'a broker without an authorizer answers every operation a group supports: READ, DELETE, DESCRIBE'
        );
        self::assertSame(328, $withOperations->authorizedOperations, 'which is the bit set 0b1_0100_1000');

        new DescribeGroupsRequest([$groupId], self::CLIENT_ID, 734, false)->writeTo($stream);
        $withoutOperations = DescribeGroupsResponse::unpack($stream)->groups[$groupId];

        self::assertSame(
            DescribeGroupResponseMetadata::OPERATIONS_NOT_REQUESTED,
            $withoutOperations->authorizedOperations,
            'a request that does not ask is answered Integer.MIN_VALUE, and not the empty bit set 0'
        );
        self::assertSame(-2147483648, $withoutOperations->authorizedOperations);

        // The version below it has no such field at all, and the rest of the entry is the same
        new DescribeGroupsRequestV2([$groupId], self::CLIENT_ID, 735)->writeTo($stream);
        $versionTwo = DescribeGroupsResponseV2::unpack($stream)->groups[$groupId];

        self::assertSame(DescribeGroupResponseMetadata::STATE_STABLE, $versionTwo->state);
        self::assertSame(
            DescribeGroupResponseMetadata::OPERATIONS_NOT_REQUESTED,
            $versionTwo->authorizedOperations,
            'the property keeps its default: the versions below 3 do not carry the field'
        );
        self::assertSame(array_keys($withOperations->members), array_keys($versionTwo->members));
    }

    /**
     * `AdminClient::describeGroup()` passes the flag through, which is the whole public surface of KIP-430 here
     */
    public function testTheAdminClientAsksForTheAuthorizedOperationsOnDemand(): void
    {
        $groupId  = self::uniqueGroupName();
        $instance = self::uniqueInstanceId();
        $stream   = $this->coordinatorStream($groupId);

        $joined = $this->join($stream, $groupId, JoinGroupRequest::DEFAULT_MEMBER_ID, $instance, 741);
        $this->sync($stream, $groupId, $joined, $instance, 742);

        $admin = new AdminClient($this->cluster(), $this->configuration());

        self::assertSame(
            DescribeGroupResponseMetadata::OPERATIONS_NOT_REQUESTED,
            $admin->describeGroup($groupId)->authorizedOperations,
            'the flag defaults to false, as it does in the Java admin client'
        );
        self::assertSame(self::GROUP_OPERATIONS, $admin->describeGroup($groupId, true)->authorizedOperations);
        self::assertSame(
            self::GROUP_OPERATIONS,
            $admin->describeGroups([$groupId], true)[$groupId]->authorizedOperations
        );
    }

    /**
     * A static consumer does not leave its group when it is closed, so the group keeps its partitions for it
     */
    public function testAStaticConsumerDoesNotLeaveItsGroupWhenItIsClosed(): void
    {
        $groupId  = self::uniqueGroupName();
        $instance = self::uniqueInstanceId();
        $consumer = $this->consumer($groupId, $instance);
        $consumer->subscribe([$this->topic]);
        $this->pollUntilAssigned($consumer);

        $memberId = $this->describeGroup($groupId);

        self::assertSame(DescribeGroupResponseMetadata::STATE_STABLE, $memberId->state);
        self::assertCount(1, $memberId->members);

        $consumer->close();

        $afterClose = $this->describeGroup($groupId);

        self::assertSame(
            DescribeGroupResponseMetadata::STATE_STABLE,
            $afterClose->state,
            'a static member sends no LeaveGroup: the group is not even rebalancing'
        );
        self::assertCount(1, $afterClose->members, 'and the coordinator still holds its partitions for it');

        // The same instance comes back and takes its assignment over, without a new generation
        $restarted = $this->consumer($groupId, $instance);
        $restarted->subscribe([$this->topic]);
        $this->pollUntilAssigned($restarted);

        self::assertSame([0, 1, 2], $this->assignedPartitions($restarted), 'it got its three partitions back');
        self::assertCount(1, $this->describeGroup($groupId)->members, 'and the group still has exactly one member');
    }

    /**
     * A fenced consumer is not recoverable: the exception reaches the application out of poll() and commitSync()
     */
    public function testAFencedStaticConsumerReportsTheErrorOutOfPollAndCommit(): void
    {
        $groupId  = self::uniqueGroupName();
        $instance = self::uniqueInstanceId();
        $first    = $this->consumer($groupId, $instance);
        $first->subscribe([$this->topic]);
        $this->pollUntilAssigned($first);

        // A second consumer of the same instance id takes the identity over
        $second = $this->consumer($groupId, $instance);
        $second->subscribe([$this->topic]);
        $this->pollUntilAssigned($second);

        self::assertSame([0, 1, 2], $this->assignedPartitions($second));

        $fenced = null;

        try {
            $first->commitSync([$this->topic => [0 => 1]]);
        } catch (FencedInstanceIdException $exception) {
            $fenced = $exception;
        }

        self::assertInstanceOf(
            FencedInstanceIdException::class,
            $fenced,
            'the commit of the fenced consumer is answered 82, and 82 is fatal'
        );
        self::assertSame(KafkaException::FENCED_INSTANCE_ID, $fenced->getCode());

        $this->expectException(FencedInstanceIdException::class);

        $deadline = microtime(true) + self::POLL_TIMEOUT;
        while (microtime(true) < $deadline) {
            $first->poll(250);
        }

        self::fail('The poll loop of a fenced consumer never reported the error');
    }

    /**
     * The batch of KIP-345: every entry is answered on its own, and the top-level code stays 0
     */
    public function testTheBatchLeaveAnswersEveryMemberOnItsOwn(): void
    {
        $groupId  = self::uniqueGroupName();
        $instance = self::uniqueInstanceId();
        $stream   = $this->coordinatorStream($groupId);

        $joined = $this->join($stream, $groupId, JoinGroupRequest::DEFAULT_MEMBER_ID, $instance, 751);
        $this->sync($stream, $groupId, $joined, $instance, 752);

        self::assertCount(1, $this->describeGroup($groupId)->members);

        new LeaveGroupRequest(
            $groupId,
            [
                new LeaveGroupRequestMember(LeaveGroupRequestMember::UNKNOWN_MEMBER_ID, $instance),
                new LeaveGroupRequestMember(LeaveGroupRequestMember::UNKNOWN_MEMBER_ID, $instance . '-nobody'),
            ],
            self::CLIENT_ID,
            753
        )->writeTo($stream);
        $answer = LeaveGroupResponse::unpack($stream);

        self::assertSame(
            KafkaException::NO_ERROR,
            $answer->errorCode,
            'the top-level code is about the request: a refused member does not fail it'
        );
        self::assertCount(2, $answer->members);
        self::assertSame(KafkaException::NO_ERROR, $answer->members[0]->errorCode, 'the static member was removed');
        self::assertSame($instance, $answer->members[0]->groupInstanceId);
        self::assertSame(
            '',
            $answer->members[0]->memberId,
            'the entry echoes the identity that was sent, not the member id the coordinator resolved'
        );
        self::assertSame(
            KafkaException::UNKNOWN_MEMBER_ID,
            $answer->members[1]->errorCode,
            'an instance id the group does not have is 25'
        );

        // The static member is gone at once: the group does not wait out its session timeout
        $description = $this->describeGroup($groupId);

        self::assertSame(DescribeGroupResponseMetadata::STATE_EMPTY, $description->state);
        self::assertSame([], $description->members);

        // An empty batch is legal and removes nothing
        new LeaveGroupRequest($groupId, [], self::CLIENT_ID, 754)->writeTo($stream);
        $empty = LeaveGroupResponse::unpack($stream);

        self::assertSame(KafkaException::NO_ERROR, $empty->errorCode);
        self::assertSame([], $empty->members, 'an empty batch is answered with an empty member array');
    }

    /**
     * A member that removes itself sends the batch of one that `Client::leaveGroup()` writes
     */
    public function testAMemberThatRemovesItselfSendsAOneElementBatch(): void
    {
        $groupId  = self::uniqueGroupName();
        $instance = self::uniqueInstanceId();
        $stream   = $this->coordinatorStream($groupId);

        $joined = $this->join($stream, $groupId, JoinGroupRequest::DEFAULT_MEMBER_ID, $instance, 761);
        $this->sync($stream, $groupId, $joined, $instance, 762);

        $configuration = $this->configuration();
        $client        = new Client($this->cluster(), $configuration);
        $coordinator   = $client->getGroupCoordinator($groupId);

        $client->leaveGroup($coordinator, $groupId, $joined->memberId, $instance);

        self::assertSame([], $this->describeGroup($groupId)->members, 'the member removed itself');

        // The same member a second time: the entry of the batch carries 25, and that is what the client reports
        $this->expectException(UnknownMemberIdException::class);

        $client->leaveGroup($coordinator, $groupId, $joined->memberId, $instance);
    }

    /**
     * `AdminClient::removeMembersFromConsumerGroup()` reports every member of the batch instead of throwing
     */
    public function testTheAdminClientRemovesMembersAndReportsEachOfThem(): void
    {
        $groupId  = self::uniqueGroupName();
        $instance = self::uniqueInstanceId();
        $stream   = $this->coordinatorStream($groupId);

        $joined = $this->join($stream, $groupId, JoinGroupRequest::DEFAULT_MEMBER_ID, $instance, 771);
        $this->sync($stream, $groupId, $joined, $instance, 772);

        $admin  = new AdminClient($this->cluster(), $this->configuration());
        $result = $admin->removeMembersFromConsumerGroup($groupId, [
            MemberToRemove::byInstanceId($instance),
            $instance . '-nobody',
            MemberToRemove::byMemberId('t3-345-no-such-member'),
        ]);

        self::assertSame([$instance, $instance . '-nobody', 't3-345-no-such-member'], array_keys($result));
        self::assertNull($result[$instance], 'the static member was removed by its instance id alone');
        self::assertInstanceOf(UnknownMemberIdException::class, $result[$instance . '-nobody']);
        self::assertInstanceOf(UnknownMemberIdException::class, $result['t3-345-no-such-member']);
        self::assertSame([], $this->describeGroup($groupId)->members);

        // An empty batch is a legal request and an empty result
        self::assertSame([], $admin->removeMembersFromConsumerGroup($groupId, []));
    }

    /**
     * Sends a JoinGroup v5 with the given member id and instance id and returns the answer
     */
    private function join(
        Stream $stream,
        string $groupId,
        string $memberId,
        ?string $groupInstanceId,
        int $correlationId
    ): JoinGroupResponse {
        new JoinGroupRequest(
            $groupId,
            self::SESSION_TIMEOUT_MS,
            self::REBALANCE_TIMEOUT_MS,
            $memberId,
            self::PROTOCOL_TYPE,
            [self::PROTOCOL_NAME => new Subscription([$this->topic])->pack()],
            self::CLIENT_ID,
            $correlationId,
            $groupInstanceId
        )->writeTo($stream);

        return JoinGroupResponse::unpack($stream);
    }

    /**
     * Publishes the whole topic to the given member, which is the leader of its generation here
     */
    private function sync(
        Stream $stream,
        string $groupId,
        JoinGroupResponse $member,
        ?string $groupInstanceId,
        int $correlationId
    ): SyncGroupResponse {
        $assignments = $member->memberId === $member->leaderId
            ? [$member->memberId => new MemberAssignment([$this->topic => [0, 1, 2]])->pack()]
            : [];

        new SyncGroupRequest(
            $groupId,
            $member->generationId,
            $member->memberId,
            $assignments,
            self::CLIENT_ID,
            $correlationId,
            $groupInstanceId
        )->writeTo($stream);

        $response = SyncGroupResponse::unpack($stream);

        self::assertSame(KafkaException::NO_ERROR, $response->errorCode, 'the member did not get its assignment');

        return $response;
    }

    /**
     * Polls the consumer until it holds an assignment
     */
    private function pollUntilAssigned(KafkaConsumer $consumer): void
    {
        $deadline = microtime(true) + self::POLL_TIMEOUT;
        while (microtime(true) < $deadline) {
            $consumer->poll(250);
            if ($consumer->assignment() !== []) {
                return;
            }
        }

        self::fail('The consumer never received an assignment');
    }

    /**
     * Returns the partitions of the topic under test that the consumer holds, in numeric order
     *
     * @return list<int>
     */
    private function assignedPartitions(KafkaConsumer $consumer): array
    {
        $partitions = array_map(intval(...), array_values($consumer->assignment()[$this->topic] ?? []));
        sort($partitions);

        return $partitions;
    }

    /**
     * Asks the coordinator what it knows about the group
     */
    private function describeGroup(string $groupId): DescribeGroupResponseMetadata
    {
        return new AdminClient($this->cluster(), $this->configuration())->describeGroup($groupId);
    }

    /**
     * Builds a static consumer for the broker under test and registers it for the clean-up
     */
    private function consumer(string $groupId, string $groupInstanceId): KafkaConsumer
    {
        $consumer = new KafkaConsumer([
            ConsumerConfig::GROUP_ID          => $groupId,
            ConsumerConfig::GROUP_INSTANCE_ID => $groupInstanceId,
        ] + $this->configuration());

        $this->consumers[] = $consumer;

        return $consumer;
    }

    /**
     * Opens the shared connection to the coordinator of the given group
     */
    private function coordinatorStream(string $groupId): Stream
    {
        $configuration = $this->configuration();

        return new CoordinatorLookup($this->cluster(), $configuration)
            ->findCoordinator($groupId)
            ->getConnection($configuration);
    }

    /**
     * Opens a second, uncached connection to the coordinator, for a member that acts next to another one
     */
    private function newCoordinatorStream(string $groupId): Stream
    {
        $configuration = $this->configuration();
        $coordinator   = new CoordinatorLookup($this->cluster(), $configuration)->findCoordinator($groupId);

        return new SocketStream("tcp://{$coordinator->host}:{$coordinator->port}", $configuration, 5.0);
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

            ConsumerConfig::SESSION_TIMEOUT_MS      => self::SESSION_TIMEOUT_MS,
            ConsumerConfig::MAX_POLL_INTERVAL_MS    => self::REBALANCE_TIMEOUT_MS,
            ConsumerConfig::HEARTBEAT_INTERVAL_MS   => 500,
            ConsumerConfig::FETCH_MAX_WAIT_MS       => 250,
            ConsumerConfig::AUTO_OFFSET_RESET       => OffsetResetStrategy::EARLIEST,
            ConsumerConfig::ENABLE_AUTO_COMMIT      => false,
        ] + ConsumerConfig::getDefaultConfiguration();
    }

    /**
     * Builds a consumer group name that is unique for this test run
     */
    private static function uniqueGroupName(): string
    {
        return 't3-345-group-' . bin2hex(random_bytes(6));
    }

    /**
     * Builds a `group.instance.id` that is unique for this test run
     */
    private static function uniqueInstanceId(): string
    {
        return 't3-345-instance-' . bin2hex(random_bytes(6));
    }
}
