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
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Consumer\MemberAssignment;
use Protocol\Kafka\Consumer\Subscription;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Data\OffsetForLeaderEpochResponsePartition;
use Protocol\Kafka\Protocol\Data\OffsetForLeaderEpochResponseTopic;
use Protocol\Kafka\Protocol\Request\CreateTopicsRequest;
use Protocol\Kafka\Protocol\Request\CreateTopicsResponse;
use Protocol\Kafka\Protocol\Request\DeleteTopicsRequest;
use Protocol\Kafka\Protocol\Request\DeleteTopicsResponse;
use Protocol\Kafka\Protocol\Request\DescribeGroupsRequest;
use Protocol\Kafka\Protocol\Request\DescribeGroupsResponse;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorRequest;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorResponse;
use Protocol\Kafka\Protocol\Request\HeartbeatRequest;
use Protocol\Kafka\Protocol\Request\HeartbeatResponse;
use Protocol\Kafka\Protocol\Request\JoinGroupRequest;
use Protocol\Kafka\Protocol\Request\JoinGroupResponse;
use Protocol\Kafka\Protocol\Request\LeaveGroupRequest;
use Protocol\Kafka\Protocol\Request\LeaveGroupResponse;
use Protocol\Kafka\Protocol\Request\ListGroupsRequest;
use Protocol\Kafka\Protocol\Request\ListGroupsResponse;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequest;
use Protocol\Kafka\Protocol\Request\OffsetCommitResponse;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequest;
use Protocol\Kafka\Protocol\Request\OffsetFetchResponse;
use Protocol\Kafka\Protocol\Request\OffsetForLeaderEpochRequest;
use Protocol\Kafka\Protocol\Request\OffsetForLeaderEpochResponse;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;
use Protocol\Kafka\Protocol\Request\OffsetsResponse;
use Protocol\Kafka\Protocol\Request\SyncGroupRequest;
use Protocol\Kafka\Protocol\Request\SyncGroupResponse;
use Protocol\Kafka\Tests\Fixture\RawApiProbe;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * Verifies against a real Kafka 0.11.0.3 broker what KIP-124 added to fourteen apis, and the two apis of 0.11 that
 * came with it: FindCoordinator v1 with its coordinator type and OffsetForLeaderEpoch.
 *
 * Every one of those apis answers with a `throttle_time_ms` of **0** on this container, which sets no quota at all;
 * `QuotaThrottleTest` is where a non-zero one is produced on purpose. What is worth verifying here is therefore not
 * the value but the LAYOUT: an answer whose leading four bytes are read as a field that is not there decodes into
 * garbage or runs off the end of the frame, so a green round trip through the version classes is the proof that the
 * field sits where the specification says it does.
 *
 * @see docs/protocol/1.1.md, sections "Quotas and throttle time" and "GroupCoordinator API (key 10, v0 and v1)"
 * @see docs/protocol/1.1.md, section "OffsetForLeaderEpoch API (key 23, v0)"
 */
#[CoversClass(Client::class)]
#[CoversClass(GroupCoordinatorRequest::class)]
#[CoversClass(GroupCoordinatorResponse::class)]
#[CoversClass(OffsetForLeaderEpochRequest::class)]
#[CoversClass(OffsetForLeaderEpochResponse::class)]
#[CoversClass(OffsetForLeaderEpochResponseTopic::class)]
#[CoversClass(OffsetForLeaderEpochResponsePartition::class)]
final class ThrottleTimeApiTest extends IntegrationTestCase
{
    /**
     * Client id of every request of this test class
     */
    private const string CLIENT_ID = 'kafka-client-t3-throttle';

    /**
     * How long the controller may take to elect the leaders of a fresh topic, in seconds
     */
    private const float LEADER_ELECTION_TIMEOUT = 30.0;

    /**
     * Session timeout of the group member this class joins with, inside `group.min/max.session.timeout.ms`
     */
    private const int SESSION_TIMEOUT_MS = 10000;

    /**
     * Topic of this class, created once: the broker is shared with the other suites
     */
    private static ?string $topic = null;

    /**
     * Cluster of this class, bootstrapped once
     */
    private static ?Cluster $sharedCluster = null;

    public function testEveryApiThatKip124TouchedAnswersWithAThrottleTimeOfZero(): void
    {
        $topic   = $this->topic();
        $groupId = self::uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);

        $throttleTimes = [];

        new OffsetsRequest(
            [$topic => [0 => OffsetsRequest::LATEST]],
            OffsetsRequest::CONSUMER_REPLICA_ID,
            FetchRequest::READ_UNCOMMITTED,
            self::CLIENT_ID,
            101
        )->writeTo($stream);
        $throttleTimes['offsets.v2'] = OffsetsResponse::unpack($stream)->throttleTimeMs;

        new OffsetCommitRequest(
            $groupId,
            OffsetCommitRequest::DEFAULT_GENERATION_ID,
            OffsetCommitRequest::DEFAULT_MEMBER_NAME,
            OffsetCommitRequest::DEFAULT_RETENTION_TIME,
            [$topic => [0 => 3]],
            self::CLIENT_ID,
            102
        )->writeTo($stream);
        $commit                           = OffsetCommitResponse::unpack($stream);
        $throttleTimes['offsetcommit.v3'] = $commit->throttleTimeMs;

        new OffsetFetchRequest($groupId, [$topic => [0]], self::CLIENT_ID, 103)->writeTo($stream);
        $fetch                           = OffsetFetchResponse::unpack($stream);
        $throttleTimes['offsetfetch.v3'] = $fetch->throttleTimeMs;

        new ListGroupsRequest(self::CLIENT_ID, 104)->writeTo($stream);
        $throttleTimes['listgroups.v1'] = ListGroupsResponse::unpack($stream)->throttleTimeMs;

        new DescribeGroupsRequest([$groupId], self::CLIENT_ID, 105)->writeTo($stream);
        $throttleTimes['describegroups.v1'] = DescribeGroupsResponse::unpack($stream)->throttleTimeMs;

        self::assertSame(
            [
                'offsets.v2'        => 0,
                'offsetcommit.v3'   => 0,
                'offsetfetch.v3'    => 0,
                'listgroups.v1'     => 0,
                'describegroups.v1' => 0,
            ],
            $throttleTimes,
            'the container sets no quota, so every answer reports a throttle time of zero'
        );

        // ... and the rest of the answer really is behind the field, not shifted by four bytes
        self::assertSame(KafkaException::NO_ERROR, $commit->topics[$topic]->partitions[0]->errorCode);
        self::assertSame(3, $fetch->topics[$topic]->partitions[0]->offset);
        self::assertSame(KafkaException::NO_ERROR, $fetch->errorCode, 'the group error still closes the answer');
    }

    public function testTheGroupMembershipApisCarryTheThrottleTimeOfTheirNewVersions(): void
    {
        $topic   = $this->topic();
        $groupId = self::uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);

        new JoinGroupRequest(
            $groupId,
            self::SESSION_TIMEOUT_MS,
            self::SESSION_TIMEOUT_MS,
            JoinGroupRequest::DEFAULT_MEMBER_ID,
            'consumer',
            ['range' => new Subscription([$topic])->pack()],
            self::CLIENT_ID,
            201
        )->writeTo($stream);
        $join = JoinGroupResponse::unpack($stream);

        self::assertSame(0, $join->throttleTimeMs);
        self::assertSame(KafkaException::NO_ERROR, $join->errorCode);
        self::assertSame($join->memberId, $join->leaderId, 'the first member of a group is its leader');
        self::assertSame([$join->memberId], array_keys($join->members));

        new SyncGroupRequest(
            $groupId,
            $join->generationId,
            $join->memberId,
            [$join->memberId => new MemberAssignment([$topic => [0, 1, 2]])->pack()],
            self::CLIENT_ID,
            202
        )->writeTo($stream);
        $sync = SyncGroupResponse::unpack($stream);

        self::assertSame(0, $sync->throttleTimeMs);
        self::assertSame(KafkaException::NO_ERROR, $sync->errorCode);
        self::assertSame(
            [0, 1, 2],
            MemberAssignment::unpack($sync->memberAssignment)->topicPartitions[$topic]->partitions,
            'the assignment survives the round trip behind the new field'
        );

        new HeartbeatRequest($groupId, $join->generationId, $join->memberId, self::CLIENT_ID, 203)->writeTo($stream);
        $heartbeat = HeartbeatResponse::unpack($stream);

        self::assertSame(0, $heartbeat->throttleTimeMs);
        self::assertSame(KafkaException::NO_ERROR, $heartbeat->errorCode);

        new LeaveGroupRequest($groupId, $join->memberId, self::CLIENT_ID, 204)->writeTo($stream);
        $leave = LeaveGroupResponse::unpack($stream);

        self::assertSame(0, $leave->throttleTimeMs);
        self::assertSame(KafkaException::NO_ERROR, $leave->errorCode);
    }

    public function testTheTopicAdminApisCarryTheThrottleTimeOfTheirNewVersions(): void
    {
        $topic  = self::uniqueTopicName('t3-throttle-admin');
        $stream = $this->connect();

        new CreateTopicsRequest(
            [new NewTopic($topic, 1, 1)],
            30000,
            false,
            self::CLIENT_ID,
            301
        )->writeTo($stream);
        $created = CreateTopicsResponse::unpack($stream);

        self::assertSame(0, $created->throttleTimeMs);
        self::assertSame(KafkaException::NO_ERROR, $created->topics[$topic]->errorCode);
        self::assertNull($created->topics[$topic]->errorMessage, 'a topic that was created carries no message');

        new DeleteTopicsRequest([$topic], 30000, self::CLIENT_ID, 302)->writeTo($stream);
        $deleted = DeleteTopicsResponse::unpack($stream);

        self::assertSame(0, $deleted->throttleTimeMs);
        self::assertSame(KafkaException::NO_ERROR, $deleted->topics[$topic]->errorCode);
    }

    public function testTheErrorMessageOfACoordinatorAnswerIsAlwaysNull(): void
    {
        // FindCoordinatorResponse @ 0.11.0.3 sets `errorMessage = null` in both of the constructors the broker
        // uses, so the NULLABLE_STRING that version 1 added is `ff ff` in every answer this client can provoke -
        // a successful lookup, and a lookup of a group that has never existed
        $stream = $this->connect();

        new GroupCoordinatorRequest(
            self::uniqueGroupName(),
            GroupCoordinatorRequest::COORDINATOR_TYPE_GROUP,
            self::CLIENT_ID,
            401
        )->writeTo($stream);
        $unknownGroup = GroupCoordinatorResponse::unpack($stream);

        self::assertNull($unknownGroup->errorMessage);
        self::assertSame(0, $unknownGroup->throttleTimeMs);
        self::assertContains(
            $unknownGroup->errorCode,
            [KafkaException::NO_ERROR, KafkaException::GROUP_COORDINATOR_NOT_AVAILABLE],
            'a group nobody has heard of gets a coordinator all the same, or a retriable 15'
        );

        $node = $this->client()->getGroupCoordinator(self::uniqueGroupName());

        self::assertContains($node->nodeId, array_keys($this->cluster()->nodes()));
    }

    public function testAnUnknownCoordinatorTypeClosesTheConnection(): void
    {
        // The type is parsed into an enum before the handler runs, and CoordinatorType.forId throws for anything
        // but 0 and 1; RequestChannel wraps that in an InvalidRequestException and SocketServer closes the channel,
        // exactly as for an unknown api key. There is no error code 42 with an error_message for this.
        $probe = new RawApiProbe(self::firstBootstrapServer());
        $body  = RawApiProbe::string('t3-throttle-bad-type') . "\x07";

        $answer = $probe->send(10, 1, $body, 501);

        self::assertSame(RawApiProbe::CLOSED, $answer['status']);
    }

    public function testATransactionalIdIsLookedUpWithTheCoordinatorTypeOne(): void
    {
        // The very first lookup of ANY transactional id creates `__transaction_state`, and is answered with the
        // error code 15 while that happens; CoordinatorLookup retries it. The id itself is neither created nor
        // registered - the broker only hashes it onto a partition of that topic - so an id that was never used is
        // a normal question with a normal answer, which is what a transactional producer sends first of all.
        $transactionalId = 't3-throttle-txn-' . bin2hex(random_bytes(6));

        $coordinator = $this->client()->getTransactionCoordinator($transactionalId);

        self::assertInstanceOf(Node::class, $coordinator);
        self::assertContains($coordinator->nodeId, array_keys($this->cluster()->nodes()));
        self::assertNotSame('', $coordinator->host);

        $admin = new AdminClient($this->cluster(), $this->configuration());
        self::assertContains(
            '__transaction_state',
            $admin->listTopics(),
            'the first type 1 lookup of the cluster is what creates the internal topic'
        );

        // The same id twice answers the same broker: the lookup is a hash of the key, not a registration
        self::assertSame($coordinator->nodeId, $this->client()->getTransactionCoordinator($transactionalId)->nodeId);
    }

    public function testTheIsolationLevelOfOffsetsVersionTwoIsAcceptedByTheBroker(): void
    {
        // Without an open transaction the last stable offset is the log end offset, so the two answers agree; the
        // point of the test is that the broker accepts and serves both levels of the new field
        $topic  = $this->topic();
        $stream = $this->connect();

        new OffsetsRequest(
            [$topic => [0 => OffsetsRequest::LATEST]],
            OffsetsRequest::CONSUMER_REPLICA_ID,
            FetchRequest::READ_UNCOMMITTED,
            self::CLIENT_ID,
            601
        )->writeTo($stream);
        $uncommitted = OffsetsResponse::unpack($stream);

        new OffsetsRequest(
            [$topic => [0 => OffsetsRequest::LATEST]],
            OffsetsRequest::CONSUMER_REPLICA_ID,
            FetchRequest::READ_COMMITTED,
            self::CLIENT_ID,
            602
        )->writeTo($stream);
        $committed = OffsetsResponse::unpack($stream);

        self::assertSame(KafkaException::NO_ERROR, $uncommitted->topics[$topic]->partitions[0]->errorCode);
        self::assertSame(KafkaException::NO_ERROR, $committed->topics[$topic]->partitions[0]->errorCode);
        self::assertSame(
            $uncommitted->topics[$topic]->partitions[0]->offset,
            $committed->topics[$topic]->partitions[0]->offset,
            'the last stable offset equals the log end offset while no transaction is open'
        );
    }

    public function testOffsetForLeaderEpochAnswersAnOrdinaryClientToo(): void
    {
        // KafkaApis.handleOffsetForLeaderEpochRequest authorizes `ClusterAction on Cluster`, which a broker without
        // an authorizer.class.name grants to everybody, so this broker-to-broker api answers a plain connection.
        // The partition of this class has never been written to, and the leader-epoch cache of a log is only
        // written when the first record is appended, so the leader cannot place the epoch and answers -1 - with the
        // error code 0, which is the "I have no entry for it" of this api rather than a failure.
        $topic  = $this->topic();
        $stream = $this->connect();

        new OffsetForLeaderEpochRequest([$topic => [0 => 0]], self::CLIENT_ID, 701)->writeTo($stream);
        $response = OffsetForLeaderEpochResponse::unpack($stream);

        self::assertSame(701, $response->getCorrelationId());
        self::assertSame([$topic], array_keys($response->topics));

        $partition = $response->topics[$topic]->partitions[0];
        self::assertSame(0, $partition->partition, 'the error code comes BEFORE the partition id in this answer');
        self::assertSame(KafkaException::NO_ERROR, $partition->errorCode);
        self::assertSame(OffsetForLeaderEpochResponsePartition::UNDEFINED_EPOCH_OFFSET, $partition->endOffset);
    }

    public function testOffsetForLeaderEpochOfAPartitionTheClusterDoesNotHostIsReportedPerPartition(): void
    {
        $absent = self::uniqueTopicName('t3-throttle-no-epoch');
        $stream = $this->connect();

        new OffsetForLeaderEpochRequest([$absent => [7 => 0]], self::CLIENT_ID, 702)->writeTo($stream);
        $response = OffsetForLeaderEpochResponse::unpack($stream);

        $partition = $response->topics[$absent]->partitions[7];
        self::assertSame(KafkaException::UNKNOWN_TOPIC_OR_PARTITION, $partition->errorCode);
        self::assertSame(
            OffsetForLeaderEpochResponsePartition::UNDEFINED_EPOCH_OFFSET,
            $partition->endOffset,
            'a leader that cannot place the epoch answers -1'
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

        $topic = self::uniqueTopicName('t3-throttle-api');
        new TopicMetadataProbe(
            fn(): Stream => $this->connect(),
            self::LEADER_ELECTION_TIMEOUT,
            self::CLIENT_ID
        )->awaitTopicWithLeaders($topic);

        return self::$topic = $topic;
    }

    /**
     * Opens a connection to the coordinator of the given group
     */
    private function coordinatorStream(string $groupId): Stream
    {
        return $this->client()->getGroupCoordinator($groupId)->getConnection($this->configuration());
    }

    private function client(): Client
    {
        return new Client($this->cluster(), $this->configuration());
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
            ClientConfig::REQUEST_TIMEOUT_MS        => 30000,
        ];
    }

    /**
     * Builds a consumer group name that is unique for this test run
     */
    private static function uniqueGroupName(): string
    {
        return 't3-throttle-group-' . bin2hex(random_bytes(6));
    }
}
