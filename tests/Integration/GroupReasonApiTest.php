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
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\CoordinatorLookup;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\MemberIdRequiredException;
use Protocol\Kafka\Common\Errors\UnknownMemberIdException;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\Internals\ConsumerCoordinator;
use Protocol\Kafka\Consumer\MemberAssignment;
use Protocol\Kafka\Consumer\Subscription;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Data\LeaveGroupRequestMember;
use Protocol\Kafka\Protocol\Data\LeaveGroupRequestMemberV3;
use Protocol\Kafka\Protocol\Request\JoinGroupRequest;
use Protocol\Kafka\Protocol\Request\JoinGroupRequestV7;
use Protocol\Kafka\Protocol\Request\JoinGroupRequestV8;
use Protocol\Kafka\Protocol\Request\JoinGroupResponse;
use Protocol\Kafka\Protocol\Request\JoinGroupResponseV7;
use Protocol\Kafka\Protocol\Request\JoinGroupResponseV8;
use Protocol\Kafka\Protocol\Request\LeaveGroupRequest;
use Protocol\Kafka\Protocol\Request\LeaveGroupRequestV4;
use Protocol\Kafka\Protocol\Request\LeaveGroupResponse;
use Protocol\Kafka\Protocol\Request\LeaveGroupResponseV4;
use Protocol\Kafka\Protocol\Request\SyncGroupRequest;
use Protocol\Kafka\Protocol\Request\SyncGroupResponse;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * What Kafka 3.2 added to the group apis, against the 3.9.2 KRaft node: KIP-800 and KIP-814.
 *
 * Two fields, one per half of the membership protocol, and neither of them changes what the coordinator does:
 *
 * * the **`reason`** of KIP-800, in the JoinGroup **v8** request and in every entry of a LeaveGroup **v5** batch,
 *   which the node writes into the log line of the rebalance it starts or of the member it removes and nowhere
 *   else - there is no api that reads it back, so what these tests can assert about it is that the node accepts
 *   every shape of it: a text, the compact null of a member that names none, and one longer than the 255
 *   characters the client cuts it at;
 * * the **`skip_assignment`** of KIP-814, one byte of the JoinGroup **v9** answer, which does change what a
 *   *client* does: a static member that comes back to a `Stable` group as its leader is told to keep the
 *   assignment the generation already has, and it then publishes an **empty** assignment array instead of a new
 *   one. That exchange is driven here end to end.
 *
 * Every group, topic and instance id of this class carries the `t3-32-` prefix of the Kafka 3.2 wave, and the
 * groups are deleted in {@see self::tearDownAfterClass()}.
 *
 * @see docs/protocol/4.3.md, section "The reason of KIP-800 and the skip_assignment of KIP-814 (v8 and v9)"
 * @see docs/protocol/4.3.md, section "The leave reason of KIP-800 (v5)"
 * @see docs/protocol/4.3.md, section "JoinGroup API (key 11, v0 to v9)"
 * @see docs/protocol/4.3.md, section "LeaveGroup API (key 13, v0 to v5)"
 */
#[CoversClass(Client::class)]
#[CoversClass(AdminClient::class)]
#[CoversClass(ConsumerCoordinator::class)]
#[CoversClass(JoinGroupRequest::class)]
#[CoversClass(JoinGroupRequestV7::class)]
#[CoversClass(JoinGroupRequestV8::class)]
#[CoversClass(JoinGroupResponse::class)]
#[CoversClass(JoinGroupResponseV7::class)]
#[CoversClass(JoinGroupResponseV8::class)]
#[CoversClass(LeaveGroupRequest::class)]
#[CoversClass(LeaveGroupRequestV4::class)]
#[CoversClass(LeaveGroupResponse::class)]
#[CoversClass(LeaveGroupResponseV4::class)]
#[CoversClass(LeaveGroupRequestMember::class)]
#[CoversClass(LeaveGroupRequestMemberV3::class)]
final class GroupReasonApiTest extends IntegrationTestCase
{
    /**
     * Protocol type of the groups of this class, deliberately not `consumer` where opaque metadata is sent
     */
    private const string PROTOCOL_TYPE = 't3-32-membership';

    /**
     * The protocol type of a real consumer group, which the static-member tests need
     */
    private const string CONSUMER_PROTOCOL_TYPE = 'consumer';

    private const string PROTOCOL_NAME = 'range';

    /**
     * Session timeout of every member here; `group.min.session.timeout.ms` of the node is 1000
     */
    private const int SESSION_TIMEOUT_MS = 6000;

    private const int REBALANCE_TIMEOUT_MS = 8000;

    /**
     * Read timeout of the connections: a JoinGroup of a fresh group waits the 3 s of the initial rebalance delay
     */
    private const int REQUEST_TIMEOUT_MS = 30000;

    /**
     * The topic the static member of this class subscribes to, so that its assignment is not empty
     */
    private const string TOPIC_PREFIX = 't3-32-reason';

    private static ?Cluster $sharedCluster = null;

    /**
     * Every group this class created, removed again in {@see self::tearDownAfterClass()}
     *
     * @var list<string>
     */
    private static array $groups = [];

    public static function tearDownAfterClass(): void
    {
        foreach (self::$groups as $groupId) {
            self::deleteGroupQuietly($groupId);
        }
        self::$groups = [];

        parent::tearDownAfterClass();
    }

    /**
     * KIP-800: the node accepts a reason at version 8, and answers the very frame of version 7
     */
    public function testAJoinOfVersionEightCarriesTheReasonAndIsAnsweredTheVersionSevenFrame(): void
    {
        $groupId = $this->uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);

        $refused = $this->rawJoinV8($stream, $groupId, JoinGroupRequest::DEFAULT_MEMBER_ID, 'the consumer started', 3201);

        self::assertSame(KafkaException::MEMBER_ID_REQUIRED, $refused->errorCode, 'the 79 of KIP-394');
        self::assertFalse(
            $refused->skipAssignment,
            'a version 8 answer has no such byte at all, so the flag of the class stays false'
        );

        $joined = $this->rawJoinV8(
            $stream,
            $groupId,
            $refused->memberId,
            ConsumerCoordinator::REJOIN_REASON_MEMBER_ID . $refused->memberId,
            3202
        );

        self::assertSame(KafkaException::NO_ERROR, $joined->errorCode, 'the node took the reason without a word');
        self::assertSame(1, $joined->generationId);
        self::assertSame($joined->memberId, $joined->leaderId);
        self::assertSame(self::PROTOCOL_TYPE, $joined->protocolType, 'the protocol type of KIP-559 is unchanged');

        $this->leaveWithReason($stream, $groupId, $joined->memberId, 'the consumer is being closed', 3203);
    }

    /**
     * A member that names no reason writes the compact null, and the node accepts that too
     */
    public function testAJoinWithoutAReasonIsAcceptedAtVersionNine(): void
    {
        $groupId = $this->uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);

        $refused = $this->rawJoin($stream, $groupId, JoinGroupRequest::DEFAULT_MEMBER_ID, null, 3204);

        self::assertSame(KafkaException::MEMBER_ID_REQUIRED, $refused->errorCode);
        self::assertFalse($refused->skipAssignment, 'every error answer of version 9 carries the flag false');
        self::assertSame(-1, $refused->generationId);

        $joined = $this->rawJoin($stream, $groupId, $refused->memberId, null, 3205);

        self::assertSame(KafkaException::NO_ERROR, $joined->errorCode);
        self::assertFalse(
            $joined->skipAssignment,
            'the leader of a fresh generation computes the assignment itself'
        );

        $this->leaveWithReason($stream, $groupId, $joined->memberId, null, 3206);
    }

    /**
     * The client cuts a reason at 255 characters, and the node takes the frame that carries it
     */
    public function testAReasonLongerThanTheBoundIsTruncatedAndStillAccepted(): void
    {
        $groupId = $this->uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);
        $reason  = str_repeat('t3-32 ', 100);

        self::assertGreaterThan(JoinGroupRequest::MAX_REASON_LENGTH, strlen($reason));

        $refused = $this->rawJoin($stream, $groupId, JoinGroupRequest::DEFAULT_MEMBER_ID, $reason, 3207);

        self::assertSame(KafkaException::MEMBER_ID_REQUIRED, $refused->errorCode);

        $joined = $this->rawJoin($stream, $groupId, $refused->memberId, $reason, 3208);

        self::assertSame(KafkaException::NO_ERROR, $joined->errorCode);

        $this->leaveWithReason($stream, $groupId, $joined->memberId, $reason, 3209);
    }

    /**
     * KIP-814: a static member that returns to a `Stable` group as its leader is told to keep the assignment
     *
     * The whole exchange: the instance joins a fresh group, publishes an assignment and makes the group `Stable`;
     * then the very same instance comes back with an **empty** member id, as a restarted consumer does, and the
     * coordinator answers `skip_assignment = true` without rebalancing anything - the generation is unchanged and
     * only the member id of the instance is new. That leader then syncs with an empty assignment array and is
     * answered the share the generation already agreed on.
     */
    public function testAStaticLeaderThatReturnsIsToldToSkipTheAssignment(): void
    {
        $topic   = self::uniqueTopicName(self::TOPIC_PREFIX);
        $groupId = $this->uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);

        new AdminClient($this->cluster(), $this->configuration())->createTopics([new NewTopic($topic, 1, 1)]);
        new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, 't3-32-reason')->awaitTopicWithLeaders($topic);

        $instanceId   = 't3-32-instance-' . bin2hex(random_bytes(4));
        $subscription = new Subscription([$topic])->pack();

        $first = $this->rawStaticJoin($stream, $groupId, $instanceId, $subscription, 'the static member starts', 3210);

        self::assertSame(KafkaException::NO_ERROR, $first->errorCode, 'a static join is never refused with the 79');
        self::assertSame(1, $first->generationId);
        self::assertStringStartsWith($instanceId . '-', $first->memberId);
        self::assertSame($first->memberId, $first->leaderId);
        self::assertFalse($first->skipAssignment, 'the group has no assignment yet, so its leader computes one');

        $assignment = new MemberAssignment([$topic => [0]])->pack();
        $this->sync($stream, $groupId, $first, $instanceId, [$first->memberId => $assignment], 3211);

        // The same instance comes back without the member id it lost, which is what a restart looks like
        $returned = $this->rawStaticJoin($stream, $groupId, $instanceId, $subscription, 'the consumer restarted', 3212);

        self::assertSame(KafkaException::NO_ERROR, $returned->errorCode);
        self::assertTrue($returned->skipAssignment, 'KIP-814: the group keeps the assignment it already has');
        self::assertSame(
            $first->generationId,
            $returned->generationId,
            'and the generation did not change, because no rebalance happened at all'
        );
        self::assertNotSame($first->memberId, $returned->memberId, 'only the member id of the instance is new');
        self::assertStringStartsWith($instanceId . '-', $returned->memberId);
        self::assertSame($returned->memberId, $returned->leaderId);

        // A leader that skips the assignment publishes NOTHING and is answered what the generation agreed on
        $synced = $this->sync($stream, $groupId, $returned, $instanceId, [], 3213);

        self::assertSame(
            $assignment,
            $synced->memberAssignment,
            'the coordinator answers the assignment of the generation, which nobody recomputed'
        );

        $this->leaveWithReason($stream, $groupId, $returned->memberId, 'the instance is retired', 3214, $instanceId);
    }

    /**
     * The same return asked at version 8 has no such byte, so the leader computes the assignment as before
     */
    public function testTheSameReturnAtVersionEightNeverSkipsTheAssignment(): void
    {
        $topic   = self::uniqueTopicName(self::TOPIC_PREFIX);
        $groupId = $this->uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);

        new AdminClient($this->cluster(), $this->configuration())->createTopics([new NewTopic($topic, 1, 1)]);
        new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, 't3-32-reason')->awaitTopicWithLeaders($topic);

        $instanceId   = 't3-32-instance-' . bin2hex(random_bytes(4));
        $subscription = new Subscription([$topic])->pack();

        $first = $this->rawStaticJoin($stream, $groupId, $instanceId, $subscription, null, 3215);
        self::assertSame(KafkaException::NO_ERROR, $first->errorCode);
        $this->sync(
            $stream,
            $groupId,
            $first,
            $instanceId,
            [$first->memberId => new MemberAssignment([$topic => [0]])->pack()],
            3216
        );

        new JoinGroupRequestV8(
            $groupId,
            self::SESSION_TIMEOUT_MS,
            self::REBALANCE_TIMEOUT_MS,
            JoinGroupRequest::DEFAULT_MEMBER_ID,
            self::CONSUMER_PROTOCOL_TYPE,
            [self::PROTOCOL_NAME => $subscription],
            $this->clientId(),
            3217,
            $instanceId,
            'a version 8 return'
        )->writeTo($stream);
        $returned = JoinGroupResponseV8::unpack($stream);

        self::assertSame(KafkaException::NO_ERROR, $returned->errorCode);
        self::assertFalse($returned->skipAssignment, 'there is no such field below version 9');

        $this->leaveWithReason($stream, $groupId, $returned->memberId, null, 3218, $instanceId);
    }

    /**
     * The public api of the client: `joinGroup()` and `leaveGroup()` take a reason and the node accepts both
     */
    public function testTheClientSendsTheReasonOfAJoinAndOfALeave(): void
    {
        $groupId     = $this->uniqueGroupName();
        $client      = new Client($this->cluster(), $this->configuration());
        $coordinator = $client->getGroupCoordinator($groupId);

        $assigned = null;
        try {
            $client->joinGroup(
                $coordinator,
                $groupId,
                JoinGroupRequest::DEFAULT_MEMBER_ID,
                self::PROTOCOL_TYPE,
                [self::PROTOCOL_NAME => 'metadata of the client'],
                null,
                null,
                'the consumer started'
            );
        } catch (MemberIdRequiredException $exception) {
            $assigned = $exception->getContext()['assignedMemberId'] ?? null;
        }

        self::assertIsString($assigned, 'the first join is refused with the id the coordinator assigned');

        $joined = $client->joinGroup(
            $coordinator,
            $groupId,
            $assigned,
            self::PROTOCOL_TYPE,
            [self::PROTOCOL_NAME => 'metadata of the client'],
            null,
            null,
            ConsumerCoordinator::REJOIN_REASON_MEMBER_ID . $assigned
        );

        self::assertSame(1, $joined->generationId);
        self::assertFalse($joined->skipAssignment);

        $client->leaveGroup($coordinator, $groupId, $joined->memberId, null, ConsumerCoordinator::LEAVE_REASON_CLOSED);

        // The member is gone: the same leave a second time is the 25 of a member the group does not have
        $this->expectException(UnknownMemberIdException::class);
        $client->leaveGroup($coordinator, $groupId, $joined->memberId, null, ConsumerCoordinator::LEAVE_REASON_CLOSED);
    }

    /**
     * The admin client writes its default reason into every entry of the batch it sends
     */
    public function testTheAdminBatchCarriesAReasonPerEntry(): void
    {
        $groupId = $this->uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);
        $admin   = new AdminClient($this->cluster(), $this->configuration());

        $refused = $this->rawJoin($stream, $groupId, JoinGroupRequest::DEFAULT_MEMBER_ID, 'the consumer started', 3219);
        $joined  = $this->rawJoin($stream, $groupId, $refused->memberId, 'the second join', 3220);

        self::assertSame(KafkaException::NO_ERROR, $joined->errorCode);

        $result = $admin->removeMembersFromConsumerGroup($groupId, [
            MemberToRemove::byMemberId($joined->memberId),
            MemberToRemove::byMemberId('t3-32-no-such-member'),
        ]);

        self::assertNull($result[$joined->memberId], 'the member of the group was removed');
        self::assertInstanceOf(
            UnknownMemberIdException::class,
            $result['t3-32-no-such-member'],
            'and the one the group does not have is 25 in its own entry, with the top-level code 0'
        );

        // A reason of the caller reaches the same api, and the node takes it just as well
        $second = $admin->removeMembersFromConsumerGroup(
            $groupId,
            [MemberToRemove::byMemberId('t3-32-no-such-member')],
            'the operator retired the instance'
        );

        self::assertInstanceOf(UnknownMemberIdException::class, $second['t3-32-no-such-member']);
    }

    /**
     * Sends a JoinGroup of the version this client speaks (v9) and returns whatever the node answered
     */
    private function rawJoin(
        Stream $stream,
        string $groupId,
        string $memberId,
        ?string $reason,
        int $correlationId
    ): JoinGroupResponse {
        new JoinGroupRequest(
            $groupId,
            self::SESSION_TIMEOUT_MS,
            self::REBALANCE_TIMEOUT_MS,
            $memberId,
            self::PROTOCOL_TYPE,
            [self::PROTOCOL_NAME => 'metadata of t3-32'],
            $this->clientId(),
            $correlationId,
            null,
            $reason
        )->writeTo($stream);

        return JoinGroupResponse::unpack($stream);
    }

    /**
     * The same join one api version lower, which carries the reason but is answered without the KIP-814 byte
     */
    private function rawJoinV8(
        Stream $stream,
        string $groupId,
        string $memberId,
        ?string $reason,
        int $correlationId
    ): JoinGroupResponseV8 {
        new JoinGroupRequestV8(
            $groupId,
            self::SESSION_TIMEOUT_MS,
            self::REBALANCE_TIMEOUT_MS,
            $memberId,
            self::PROTOCOL_TYPE,
            [self::PROTOCOL_NAME => 'metadata of t3-32'],
            $this->clientId(),
            $correlationId,
            null,
            $reason
        )->writeTo($stream);

        return JoinGroupResponseV8::unpack($stream);
    }

    /**
     * The join of a STATIC member of a `consumer` group, whose metadata the coordinator really parses
     */
    private function rawStaticJoin(
        Stream $stream,
        string $groupId,
        string $instanceId,
        string $subscription,
        ?string $reason,
        int $correlationId
    ): JoinGroupResponse {
        new JoinGroupRequest(
            $groupId,
            self::SESSION_TIMEOUT_MS,
            self::REBALANCE_TIMEOUT_MS,
            JoinGroupRequest::DEFAULT_MEMBER_ID,
            self::CONSUMER_PROTOCOL_TYPE,
            [self::PROTOCOL_NAME => $subscription],
            $this->clientId(),
            $correlationId,
            $instanceId,
            $reason
        )->writeTo($stream);

        return JoinGroupResponse::unpack($stream);
    }

    /**
     * @param array<string, string> $assignments Assignment of every member, empty for a leader that skips it
     */
    private function sync(
        Stream $stream,
        string $groupId,
        JoinGroupResponse $joined,
        ?string $instanceId,
        array $assignments,
        int $correlationId
    ): SyncGroupResponse {
        new SyncGroupRequest(
            $groupId,
            $joined->generationId,
            $joined->memberId,
            $assignments,
            $this->clientId(),
            $correlationId,
            $instanceId,
            $joined->protocolType,
            $joined->groupProtocol
        )->writeTo($stream);
        $synced = SyncGroupResponse::unpack($stream);

        self::assertSame(KafkaException::NO_ERROR, $synced->errorCode, 'The node refused the sync');

        return $synced;
    }

    private function leaveWithReason(
        Stream $stream,
        string $groupId,
        string $memberId,
        ?string $reason,
        int $correlationId,
        ?string $instanceId = null
    ): void {
        new LeaveGroupRequest(
            $groupId,
            [new LeaveGroupRequestMember($memberId, $instanceId, $reason)],
            $this->clientId(),
            $correlationId
        )->writeTo($stream);
        $left = LeaveGroupResponse::unpack($stream);

        self::assertSame(KafkaException::NO_ERROR, $left->errorCode, 'The node refused the batch');
        self::assertSame(KafkaException::NO_ERROR, $left->members[0]->errorCode, 'The node refused to remove it');
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
            ClientConfig::CLIENT_ID                 => $this->clientId(),
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ClientConfig::RETRY_BACKOFF_MS          => 250,
            ClientConfig::RETRIES                   => 2,
            ClientConfig::REQUEST_TIMEOUT_MS        => self::REQUEST_TIMEOUT_MS,

            ConsumerConfig::SESSION_TIMEOUT_MS      => self::SESSION_TIMEOUT_MS,
            ConsumerConfig::MAX_POLL_INTERVAL_MS    => self::REBALANCE_TIMEOUT_MS,
        ] + ConsumerConfig::getDefaultConfiguration();
    }

    private function clientId(): string
    {
        return 'kafka-client-t3-32';
    }

    /**
     * Builds a group name that is unique for this run and is removed when the class is done
     */
    private function uniqueGroupName(): string
    {
        $groupId        = 't3-32-group-' . bin2hex(random_bytes(6));
        self::$groups[] = $groupId;

        return $groupId;
    }

    /**
     * Removes a group of this class from the coordinator, ignoring a group that is gone or not empty any more
     */
    private static function deleteGroupQuietly(string $groupId): void
    {
        try {
            $configuration = [
                ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
                ClientConfig::CLIENT_ID                 => 'kafka-client-t3-32',
                ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ] + ConsumerConfig::getDefaultConfiguration();

            new AdminClient(Cluster::bootstrap($configuration), $configuration)->deleteConsumerGroups([$groupId]);
        } catch (KafkaException) {
            // A group that is gone, or one the reaper has not emptied yet, must not fail the suite
        }
    }
}
