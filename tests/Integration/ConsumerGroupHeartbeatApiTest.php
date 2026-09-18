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
use Protocol\Kafka\Common\Errors\FencedMemberEpochException;
use Protocol\Kafka\Common\Errors\GroupIdNotFoundException;
use Protocol\Kafka\Common\Errors\InvalidRequestException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\UnknownMemberIdException;
use Protocol\Kafka\Common\Errors\UnreleasedInstanceIdException;
use Protocol\Kafka\Common\Errors\UnsupportedAssignorException;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\Internals\ConsumerGroupHeartbeatCoordinator;
use Protocol\Kafka\Consumer\MemberAssignment;
use Protocol\Kafka\Consumer\Subscription;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Request\ConsumerGroupHeartbeatRequest;
use Protocol\Kafka\Protocol\Request\ConsumerGroupHeartbeatResponse;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * ConsumerGroupHeartbeat (key 68, Kafka 3.5, KIP-848) against the 3.9.2 KRaft node.
 *
 * The api is the whole membership protocol of the new consumer in one frame, and every rule of it is a rule about
 * the **delta encoding** of that frame: what a null field means, what the empty array means, what the member
 * epoch means. All of them are driven here against the real coordinator, which runs both protocols
 * (`group.coordinator.rebalance.protocols=classic,consumer`).
 *
 * The suite is the one place that observes the codes **110**, **111** and **112** of KIP-848, which no api of the
 * lines below can produce, and the **69** with which the node keeps the two protocols apart.
 *
 * Every group of this class carries the `t3-848-hb-` prefix and is removed in
 * {@see self::tearDownAfterClass()} - every member with a leave heartbeat of the epoch -1 first, because a group
 * that still holds one is not deletable.
 *
 * @see docs/protocol/3.9.md, section "ConsumerGroupHeartbeat API (key 68, v0)"
 */
#[CoversClass(Client::class)]
#[CoversClass(ConsumerGroupHeartbeatRequest::class)]
#[CoversClass(ConsumerGroupHeartbeatResponse::class)]
final class ConsumerGroupHeartbeatApiTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t3-848-hb';

    private const string TOPIC_PREFIX = 't3-848-hb';

    private const int REBALANCE_TIMEOUT_MS = 300000;

    private const int REQUEST_TIMEOUT_MS = 30000;

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
     * The join of a fresh group is answered the group epoch, the interval of the broker and - here - everything
     */
    public function testTheJoinOfAFreshGroupIsAnsweredTheAssignmentAtOnce(): void
    {
        $topic   = $this->topic(3);
        $groupId = $this->uniqueGroupName();
        $member  = ConsumerGroupHeartbeatCoordinator::newMemberId();

        $answer = $this->client()->joinConsumerGroup(
            $this->coordinator($groupId),
            $groupId,
            $member,
            [$topic],
            self::REBALANCE_TIMEOUT_MS
        );
        self::$members[] = [$groupId, $member];

        self::assertSame(KafkaException::NO_ERROR, $answer->errorCode);
        self::assertNull($answer->errorMessage);
        self::assertSame($member, $answer->memberId, 'the member id the consumer generated for itself comes back');
        self::assertGreaterThan(0, $answer->memberEpoch, 'a join is answered the GROUP epoch, never the 0 it sent');
        self::assertSame(5000, $answer->heartbeatIntervalMs, 'group.consumer.heartbeat.interval.ms of the broker');
        self::assertNotNull($answer->assignment, 'the one member of a fresh group is reconciled at once');
        self::assertSame(
            [$this->topicIdOf($topic) => [0, 1, 2]],
            $answer->assignment->partitionsByTopicId(),
            'and it holds every partition of the topic, named by the topic id'
        );

        // and the acknowledgement of that assignment is answered the null of "nothing changed"
        $acknowledged = $this->client()->consumerGroupHeartbeat(
            $this->coordinator($groupId),
            $groupId,
            $member,
            $answer->memberEpoch,
            null,
            [$this->topicIdOf($topic) => [0, 1, 2]]
        );

        self::assertSame(KafkaException::NO_ERROR, $acknowledged->errorCode);
        self::assertNull($acknowledged->assignment, 'a null assignment is "nothing changed", not "you own nothing"');
    }

    /**
     * A steady-state heartbeat names nothing at all and is answered nothing at all
     */
    public function testTheSteadyStateOfAMemberIsItsEpochAndFiveNulls(): void
    {
        $topic   = $this->topic(1);
        $groupId = $this->uniqueGroupName();
        [$member, $epoch] = $this->joinedMember($groupId, [$topic]);

        $request = ConsumerGroupHeartbeatRequest::forHeartbeat($groupId, $member, $epoch, clientId: self::CLIENT_ID);

        self::assertNull($request->getSubscribedTopicNames());
        self::assertNull($request->getTopicPartitions());

        $answer = $this->client()->consumerGroupHeartbeat($this->coordinator($groupId), $groupId, $member, $epoch);

        self::assertSame(KafkaException::NO_ERROR, $answer->errorCode);
        self::assertSame($epoch, $answer->memberEpoch, 'the epoch of a settled member does not move');
        self::assertNull($answer->assignment);
    }

    /**
     * The one shape rule of the api: a (re-)join carries the EMPTY partition array, and the null is refused
     */
    public function testAJoinWhoseTopicPartitionsAreNotEmptyIsRefusedWithTheFortyTwo(): void
    {
        $topic   = $this->topic(1);
        $groupId = $this->uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);

        $refusals = [
            'null'      => null,
            'non-empty' => [$this->topicIdOf($topic) => [0]],
        ];
        foreach ($refusals as $what => $partitions) {
            $answer = $this->send($stream, new ConsumerGroupHeartbeatRequest(
                $groupId,
                ConsumerGroupHeartbeatCoordinator::newMemberId(),
                ConsumerGroupHeartbeatRequest::JOIN_MEMBER_EPOCH,
                [$topic],
                $partitions,
                self::REBALANCE_TIMEOUT_MS,
                clientId: self::CLIENT_ID
            ));

            self::assertSame(KafkaException::INVALID_REQUEST, $answer->errorCode, "the {$what} array");
            self::assertSame(
                'TopicPartitions must be empty when (re-)joining.',
                $answer->errorMessage,
                'the api says which rule was broken in its own field'
            );
        }

        // and the client reports the refusal as its own exception
        $this->expectException(InvalidRequestException::class);

        $this->client()->consumerGroupHeartbeat(
            $this->coordinator($groupId),
            $groupId,
            ConsumerGroupHeartbeatCoordinator::newMemberId(),
            ConsumerGroupHeartbeatRequest::JOIN_MEMBER_EPOCH,
            [$topic],
            null
        );
    }

    /**
     * An epoch that is not the one the coordinator holds is the 110, below and above it alike
     */
    public function testAnEpochThatIsNotTheCurrentOneIsTheOneHundredAndTen(): void
    {
        $topic   = $this->topic(1);
        $second  = $this->topic(1);
        $groupId = $this->uniqueGroupName();
        [$member, $joinEpoch] = $this->joinedMember($groupId, [$topic]);
        $stream  = $this->coordinatorStream($groupId);

        // A subscription that changes earns the member a second epoch, so that there is one BELOW the current
        // one that is not the 0 of a (re-)join
        $epoch = $this->client()
            ->consumerGroupHeartbeat($this->coordinator($groupId), $groupId, $member, $joinEpoch, [$topic, $second])
            ->memberEpoch;

        self::assertGreaterThan(1, $epoch);

        foreach ([$epoch + 5, $epoch - 1] as $stale) {
            $answer = $this->send(
                $stream,
                ConsumerGroupHeartbeatRequest::forHeartbeat($groupId, $member, $stale, clientId: self::CLIENT_ID)
            );

            self::assertSame(
                KafkaException::FENCED_MEMBER_EPOCH,
                $answer->errorCode,
                "the epoch {$stale} is not the {$epoch} the coordinator holds"
            );
            self::assertNotNull($answer->errorMessage);
            self::assertStringContainsString('member epoch', (string) $answer->errorMessage);
            self::assertSame(0, $answer->memberEpoch, 'a refusal carries no epoch of its own');
            self::assertNull($answer->assignment);
        }

        $this->expectException(FencedMemberEpochException::class);

        $this->client()->consumerGroupHeartbeat($this->coordinator($groupId), $groupId, $member, $epoch + 1);
    }

    /**
     * A member id the group does not hold is the 25, whatever the epoch says
     */
    public function testAMemberIdTheGroupDoesNotHoldIsTheTwentyFive(): void
    {
        $topic   = $this->topic(1);
        $groupId = $this->uniqueGroupName();
        [, $epoch] = $this->joinedMember($groupId, [$topic]);

        $answer = $this->send(
            $this->coordinatorStream($groupId),
            ConsumerGroupHeartbeatRequest::forHeartbeat(
                $groupId,
                'not-a-member-of-this-group',
                $epoch,
                clientId: self::CLIENT_ID
            )
        );

        self::assertSame(KafkaException::UNKNOWN_MEMBER_ID, $answer->errorCode);
        self::assertStringContainsString('is not a member of group', (string) $answer->errorMessage);

        $this->expectException(UnknownMemberIdException::class);

        $this->client()->consumerGroupHeartbeat(
            $this->coordinator($groupId),
            $groupId,
            'not-a-member-of-this-group',
            $epoch
        );
    }

    /**
     * A `server_assignor` the broker does not have is the 112, and the message names the ones it does have
     */
    public function testAnAssignorTheBrokerDoesNotHaveIsTheOneHundredAndTwelve(): void
    {
        $topic   = $this->topic(1);
        $groupId = $this->uniqueGroupName();

        $answer = $this->send(
            $this->coordinatorStream($groupId),
            ConsumerGroupHeartbeatRequest::forJoin(
                $groupId,
                ConsumerGroupHeartbeatCoordinator::newMemberId(),
                [$topic],
                self::REBALANCE_TIMEOUT_MS,
                serverAssignor: 'sticky',
                clientId: self::CLIENT_ID
            )
        );

        self::assertSame(KafkaException::UNSUPPORTED_ASSIGNOR, $answer->errorCode);
        self::assertSame(
            'ServerAssignor sticky is not supported. Supported assignors: uniform, range.',
            $answer->errorMessage,
            'group.consumer.assignors of the node, verbatim'
        );

        $this->expectException(UnsupportedAssignorException::class);

        $this->client()->joinConsumerGroup(
            $this->coordinator($groupId),
            $groupId,
            ConsumerGroupHeartbeatCoordinator::newMemberId(),
            [$topic],
            self::REBALANCE_TIMEOUT_MS,
            serverAssignor: 'sticky'
        );
    }

    /**
     * And one the broker does have is accepted, which is what `group.remote.assignor` sends
     */
    public function testTheServerSideAssignorOfTheNodeIsAccepted(): void
    {
        $topic   = $this->topic(1);
        $groupId = $this->uniqueGroupName();
        $member  = ConsumerGroupHeartbeatCoordinator::newMemberId();

        $answer = $this->client()->joinConsumerGroup(
            $this->coordinator($groupId),
            $groupId,
            $member,
            [$topic],
            self::REBALANCE_TIMEOUT_MS,
            serverAssignor: 'range'
        );
        self::$members[] = [$groupId, $member];

        self::assertSame(KafkaException::NO_ERROR, $answer->errorCode);
        self::assertGreaterThan(0, $answer->memberEpoch);
    }

    /**
     * The reconciliation of two members: nothing is handed over before the first one acknowledged the revoke
     */
    public function testTheRebalanceOfTwoMembersIsDrivenByTheAcknowledgements(): void
    {
        $topic   = $this->topic(3);
        $topicId = $this->topicIdOf($topic);
        $groupId = $this->uniqueGroupName();
        $node    = $this->coordinator($groupId);
        $client  = $this->client();

        [$first, $firstEpoch] = $this->joinedMember($groupId, [$topic]);

        $second = ConsumerGroupHeartbeatCoordinator::newMemberId();
        $joined = $client->joinConsumerGroup($node, $groupId, $second, [$topic], self::REBALANCE_TIMEOUT_MS);
        self::$members[] = [$groupId, $second];

        self::assertNotNull($joined->assignment, 'the second member is answered an assignment');
        self::assertSame(
            [],
            $joined->assignment->topicPartitions,
            'an EMPTY one: it owns nothing while the first member still holds everything'
        );

        // the first member is told what it loses, on its next heartbeat and only once
        $revoked = $client->consumerGroupHeartbeat($node, $groupId, $first, $firstEpoch);

        self::assertNotNull($revoked->assignment);
        $kept = $revoked->assignment->partitionsByTopicId()[$topicId] ?? [];
        self::assertNotSame([0, 1, 2], $kept, 'the first member has to give partitions up');
        self::assertNotSame([], $kept, 'and keeps some of them - three partitions over two members');

        $repeated = $client->consumerGroupHeartbeat($node, $groupId, $first, $revoked->memberEpoch);

        self::assertNull($repeated->assignment, 'and the coordinator does not repeat the change');

        // until it acknowledges, the second member is answered nothing
        $waiting = $client->consumerGroupHeartbeat($node, $groupId, $second, $joined->memberEpoch);

        self::assertNull($waiting->assignment, 'the partitions are still held by the first member');

        $acknowledged = $client->consumerGroupHeartbeat($node, $groupId, $first, $revoked->memberEpoch, null, [
            $topicId => $kept,
        ]);

        self::assertSame(KafkaException::NO_ERROR, $acknowledged->errorCode);

        $handedOver = $client->consumerGroupHeartbeat($node, $groupId, $second, $joined->memberEpoch);

        self::assertNotNull($handedOver->assignment, 'now the coordinator hands them over');
        self::assertSame(
            array_values(array_diff([0, 1, 2], $kept)),
            $handedOver->assignment->partitionsByTopicId()[$topicId] ?? [],
            'exactly the partitions the first member released'
        );
    }

    /**
     * A subscription that changed needs no rejoin: it travels in an ordinary heartbeat
     */
    public function testASubscriptionThatChangedTravelsInAnOrdinaryHeartbeat(): void
    {
        $topic   = $this->topic(1);
        $second  = $this->topic(1);
        $groupId = $this->uniqueGroupName();
        [$member, $epoch] = $this->joinedMember($groupId, [$topic]);

        $answer = $this->client()->consumerGroupHeartbeat(
            $this->coordinator($groupId),
            $groupId,
            $member,
            $epoch,
            [$topic, $second]
        );

        self::assertSame(KafkaException::NO_ERROR, $answer->errorCode);
        self::assertGreaterThan($epoch, $answer->memberEpoch, 'the change earned the member a new epoch');
    }

    /**
     * A static member holds its instance id, and the leave of the epoch -2 hands it back
     */
    public function testTheStaticMemberOfKip848LeavesWithTheEpochMinusTwo(): void
    {
        $topic    = $this->topic(1);
        $groupId  = $this->uniqueGroupName();
        $instance = $groupId . '-one';
        $node     = $this->coordinator($groupId);
        $client   = $this->client();

        $member = ConsumerGroupHeartbeatCoordinator::newMemberId();
        $joined = $client->joinConsumerGroup(
            $node,
            $groupId,
            $member,
            [$topic],
            self::REBALANCE_TIMEOUT_MS,
            $instance
        );
        self::$members[] = [$groupId, $member];

        self::assertSame(KafkaException::NO_ERROR, $joined->errorCode);

        // a second member under the same instance id is refused while the first one holds it
        $taken = $this->send($this->coordinatorStream($groupId), ConsumerGroupHeartbeatRequest::forJoin(
            $groupId,
            ConsumerGroupHeartbeatCoordinator::newMemberId(),
            [$topic],
            self::REBALANCE_TIMEOUT_MS,
            $instance,
            clientId: self::CLIENT_ID
        ));

        self::assertSame(KafkaException::UNRELEASED_INSTANCE_ID, $taken->errorCode);
        self::assertStringContainsString('is owned by', (string) $taken->errorMessage);

        $this->expectExceptionOnce(UnreleasedInstanceIdException::class, function () use ($node, $groupId, $topic, $instance): void {
            $this->client()->joinConsumerGroup(
                $node,
                $groupId,
                ConsumerGroupHeartbeatCoordinator::newMemberId(),
                [$topic],
                self::REBALANCE_TIMEOUT_MS,
                $instance
            );
        });

        // the leave of the epoch -2 says "I will be back", and the answer echoes that epoch
        $left = $client->leaveConsumerGroup($node, $groupId, $member, true, $instance);

        self::assertSame(KafkaException::NO_ERROR, $left->errorCode);
        self::assertSame(
            ConsumerGroupHeartbeatRequest::STATIC_LEAVE_MEMBER_EPOCH,
            $left->memberEpoch,
            'the static leave is answered its own epoch back'
        );

        // and the instance comes back under a NEW member id, with the assignment it had
        $rejoined = ConsumerGroupHeartbeatCoordinator::newMemberId();
        $back     = $client->joinConsumerGroup(
            $node,
            $groupId,
            $rejoined,
            [$topic],
            self::REBALANCE_TIMEOUT_MS,
            $instance
        );
        self::$members[] = [$groupId, $rejoined];

        self::assertSame(KafkaException::NO_ERROR, $back->errorCode);
        self::assertSame($joined->memberEpoch, $back->memberEpoch, 'the restart cost the group no epoch at all');
        self::assertNotNull($back->assignment);
        self::assertSame(
            [$this->topicIdOf($topic) => [0]],
            $back->assignment->partitionsByTopicId(),
            'and the partitions of the instance are still its own'
        );
    }

    /**
     * A dynamic member leaves with the epoch -1, which is what replaces LeaveGroup in this protocol
     */
    public function testADynamicMemberLeavesWithTheEpochMinusOne(): void
    {
        $topic   = $this->topic(1);
        $groupId = $this->uniqueGroupName();
        [$member] = $this->joinedMember($groupId, [$topic]);

        $left = $this->client()->leaveConsumerGroup($this->coordinator($groupId), $groupId, $member);

        self::assertSame(KafkaException::NO_ERROR, $left->errorCode);
        self::assertSame(ConsumerGroupHeartbeatRequest::LEAVE_MEMBER_EPOCH, $left->memberEpoch);
        self::assertSame(0, $left->heartbeatIntervalMs, 'a member that is gone has no interval to keep');
        self::assertNull($left->assignment);
    }

    /**
     * A group the classic protocol created cannot be spoken to with this api
     */
    public function testAGroupOfTheClassicProtocolIsRefusedWithTheSixtyNine(): void
    {
        $topic   = $this->topic(1);
        $groupId = $this->uniqueGroupName();
        $this->classicGroup($groupId, $topic);

        $answer = $this->send($this->coordinatorStream($groupId), ConsumerGroupHeartbeatRequest::forJoin(
            $groupId,
            ConsumerGroupHeartbeatCoordinator::newMemberId(),
            [$topic],
            self::REBALANCE_TIMEOUT_MS,
            clientId: self::CLIENT_ID
        ));

        self::assertSame(KafkaException::GROUP_ID_NOT_FOUND, $answer->errorCode);
        self::assertSame("Group {$groupId} is not a consumer group.", $answer->errorMessage);

        $this->expectException(GroupIdNotFoundException::class);

        $this->client()->joinConsumerGroup(
            $this->coordinator($groupId),
            $groupId,
            ConsumerGroupHeartbeatCoordinator::newMemberId(),
            [$topic],
            self::REBALANCE_TIMEOUT_MS
        );
    }

    /**
     * The other direction is not refused at all: this is the online upgrade path of KIP-848
     */
    public function testAClassicJoinGroupIsAcceptedByAGroupOfTheNewProtocol(): void
    {
        $topic   = $this->topic(1);
        $groupId = $this->uniqueGroupName();
        [$member] = $this->joinedMember($groupId, [$topic]);

        $node   = $this->coordinator($groupId);
        $client = $this->client();

        try {
            $join = $client->joinGroup(
                $node,
                $groupId,
                '',
                'consumer',
                ['range' => new Subscription([$topic])->pack()],
                self::REBALANCE_TIMEOUT_MS
            );
        } catch (\Protocol\Kafka\Common\Errors\MemberIdRequiredException $needsId) {
            $assigned = (string) ($needsId->getContext()['assignedMemberId'] ?? '');

            self::assertNotSame('', $assigned, 'the 79 of KIP-394 carries the id the coordinator generated');
            self::assertStringNotContainsString(
                self::CLIENT_ID,
                $assigned,
                'and it is a base64 uuid here, not the <client.id>-<uuid> of a classic coordinator'
            );

            $join = $client->joinGroup(
                $node,
                $groupId,
                $assigned,
                'consumer',
                ['range' => new Subscription([$topic])->pack()],
                self::REBALANCE_TIMEOUT_MS
            );
        }

        self::assertGreaterThan(0, $join->generationId, 'the classic member is in the KIP-848 group');
        self::assertSame('', $join->leaderId, 'a group of this type has no leader: the coordinator assigns');

        $client->leaveGroup($node, $groupId, $join->memberId, null, 'the t3-848-hb suite is done');
        $client->leaveConsumerGroup($node, $groupId, $member);
    }

    /**
     * The one principal outside `super.users` is refused at the TOP level, because this api has no entries
     */
    public function testAnUnauthorizedPrincipalIsRefusedWithTheThirty(): void
    {
        if (self::saslBootstrapServer() === '') {
            self::markTestSkipped(self::SASL_BOOTSTRAP_SERVERS_ENV . ' is not set, an acl needs a principal');
        }

        $topic   = $this->topic(1);
        $groupId = $this->uniqueGroupName();

        // The principal may not even look the coordinator up - a FindCoordinator of the type 0 is `DESCRIBE` on
        // the group and is refused 30 as well - so the one broker of this node is asked directly
        $stream = new \Protocol\Kafka\IO\SocketStream(
            'tcp://' . self::saslBootstrapServer(),
            $this->unprivilegedConfiguration(),
            5.0
        );

        $answer = $this->send($stream, ConsumerGroupHeartbeatRequest::forJoin(
            $groupId,
            ConsumerGroupHeartbeatCoordinator::newMemberId(),
            [$topic],
            self::REBALANCE_TIMEOUT_MS,
            clientId: self::CLIENT_ID
        ));

        self::assertSame(KafkaException::GROUP_AUTHORIZATION_FAILED, $answer->errorCode);
        self::assertNull($answer->errorMessage, 'the authorizer adds no sentence of its own');
        self::assertSame(0, $answer->memberEpoch);
        self::assertNull($answer->assignment);
    }

    /**
     * Creates a topic of this class and waits until every partition of it has a leader
     */
    private function topic(int $partitions): string
    {
        $topic = self::uniqueTopicName(self::TOPIC_PREFIX);

        new AdminClient($this->cluster(), $this->configuration())
            ->createTopics([new NewTopic($topic, $partitions, 1)]);
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
        $answer = $this->client()->joinConsumerGroup(
            $this->coordinator($groupId),
            $groupId,
            $member,
            $topics,
            self::REBALANCE_TIMEOUT_MS
        );
        self::$members[] = [$groupId, $member];

        self::assertSame(KafkaException::NO_ERROR, $answer->errorCode);

        return [$member, $answer->memberEpoch];
    }

    /**
     * Creates a group of the CLASSIC protocol, so that the two can be asked about each other
     */
    private function classicGroup(string $groupId, string $topic): void
    {
        $client = $this->client();
        $node   = $this->coordinator($groupId);

        try {
            $join = $client->joinGroup(
                $node,
                $groupId,
                '',
                'consumer',
                ['range' => new Subscription([$topic])->pack()],
                self::REBALANCE_TIMEOUT_MS
            );
        } catch (\Protocol\Kafka\Common\Errors\MemberIdRequiredException $needsId) {
            $join = $client->joinGroup(
                $node,
                $groupId,
                (string) ($needsId->getContext()['assignedMemberId'] ?? ''),
                'consumer',
                ['range' => new Subscription([$topic])->pack()],
                self::REBALANCE_TIMEOUT_MS
            );
        }

        $client->syncGroup(
            $node,
            $groupId,
            $join->memberId,
            $join->generationId,
            [$join->memberId => new MemberAssignment([$topic => [0]])->pack()],
            null,
            'consumer',
            $join->groupProtocol
        );

        self::$members[] = [$groupId, $join->memberId];
    }

    /**
     * Sends one frame over an open coordinator connection and reads its answer
     */
    private function send(Stream $stream, ConsumerGroupHeartbeatRequest $request): ConsumerGroupHeartbeatResponse
    {
        $request->writeTo($stream);

        return ConsumerGroupHeartbeatResponse::unpack($stream);
    }

    /**
     * Runs a callable that has to throw, without ending the test the way `expectException()` does
     *
     * @param class-string<\Throwable> $exceptionClass
     */
    private function expectExceptionOnce(string $exceptionClass, callable $what): void
    {
        try {
            $what();
        } catch (\Throwable $thrown) {
            self::assertInstanceOf($exceptionClass, $thrown);

            return;
        }

        self::fail("Expected {$exceptionClass}, nothing was thrown");
    }

    private function client(): Client
    {
        return new Client($this->cluster(), $this->configuration());
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

    /**
     * @return array<string, mixed>
     */
    private function unprivilegedConfiguration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::saslBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ClientConfig::REQUEST_TIMEOUT_MS        => self::REQUEST_TIMEOUT_MS,
            ClientConfig::SECURITY_PROTOCOL         => \Protocol\Kafka\Common\Security\SecurityProtocol::SASL_PLAINTEXT,
            ClientConfig::SASL_MECHANISM            => \Protocol\Kafka\Common\Security\SaslMechanism::PLAIN,
            ClientConfig::SASL_USERNAME             => 'acltest',
            ClientConfig::SASL_PASSWORD             => 'acltest-secret',
        ] + ConsumerConfig::getDefaultConfiguration();
    }

    private function uniqueGroupName(): string
    {
        $groupId        = 't3-848-hb-group-' . bin2hex(random_bytes(6));
        self::$groups[] = $groupId;

        return $groupId;
    }

    /**
     * Takes one member of this class out of its group, ignoring a member or a group that is gone already
     */
    private static function leaveQuietly(string $groupId, string $memberId): void
    {
        $configuration = self::cleanupConfiguration();

        try {
            $cluster = Cluster::bootstrap($configuration);
            new Client($cluster, $configuration)->leaveConsumerGroup(
                new CoordinatorLookup($cluster, $configuration)->findCoordinator($groupId),
                $groupId,
                $memberId
            );
        } catch (KafkaException) {
            // A member the coordinator has already forgotten, or a classic one, must not fail the suite
        }

        try {
            $cluster = Cluster::bootstrap($configuration);
            new Client($cluster, $configuration)->leaveGroup(
                new CoordinatorLookup($cluster, $configuration)->findCoordinator($groupId),
                $groupId,
                $memberId,
                null,
                'the t3-848-hb suite is done'
            );
        } catch (KafkaException) {
            // The same member id is only a classic one in the two mixed-protocol tests
        }
    }

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
