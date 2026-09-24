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
use Protocol\Kafka\Common\Errors\InvalidRegularExpressionException;
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
use Protocol\Kafka\Protocol\Data\ConsumerGroupDescribeMember;
use Protocol\Kafka\Protocol\Request\ConsumerGroupHeartbeatRequest;
use Protocol\Kafka\Protocol\Request\ConsumerGroupHeartbeatRequestV0;
use Protocol\Kafka\Protocol\Request\ConsumerGroupHeartbeatResponse;
use Protocol\Kafka\Protocol\Request\ConsumerGroupHeartbeatResponseV0;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * ConsumerGroupHeartbeat (key 68, Kafka 3.5, KIP-848) against the 4.3.1 KRaft node, at its version 1 (Kafka 4.0).
 *
 * The api is the whole membership protocol of the new consumer in one frame, and every rule of it is a rule about
 * the **delta encoding** of that frame: what a null field means, what the empty array means, what the member
 * epoch means. All of them are driven here against the real coordinator, which runs both protocols
 * (`group.coordinator.rebalance.protocols=classic,consumer`).
 *
 * The suite is the one place that observes the codes **110**, **111** and **112** of KIP-848, which no api of the
 * lines below can produce, the **128** `InvalidRegularExpression` of the regex subscription of version 1, and the
 * **69** of a classic group the node cannot upgrade. Two things of the 4.3.1 coordinator shape it: a classic group
 * of the consumer protocol is **upgraded online** by the first heartbeat that names it
 * (`group.consumer.migration.policy`, `bidirectional` by default since Kafka 4.0), and the target assignment is
 * computed off the heartbeat path at most once per `group.consumer.assignment.interval.ms` (Kafka 4.3), so a test
 * that waits for an assignment heartbeats until it arrives.
 *
 * Every group of this class carries the `t3-848-hb-` prefix and is removed in
 * {@see self::tearDownAfterClass()} - every member with a leave heartbeat of the epoch -1 first, because a group
 * that still holds one is not deletable.
 *
 * @see docs/protocol/4.3.md, section "ConsumerGroupHeartbeat API (key 68, v0 and v1)"
 */
#[CoversClass(Client::class)]
#[CoversClass(ConsumerGroupHeartbeatRequest::class)]
#[CoversClass(ConsumerGroupHeartbeatRequestV0::class)]
#[CoversClass(ConsumerGroupHeartbeatResponse::class)]
#[CoversClass(ConsumerGroupHeartbeatResponseV0::class)]
final class ConsumerGroupHeartbeatApiTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t3-848-hb';

    private const string TOPIC_PREFIX = 't3-848-hb';

    private const int REBALANCE_TIMEOUT_MS = 300000;

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

        // the first member is told what it loses, on a heartbeat after the coordinator computed the new target
        // assignment - once per group.consumer.assignment.interval.ms on a 4.3 node - and only once
        $revoked = $this->heartbeatUntilAssigned($groupId, $first, $firstEpoch);
        $kept    = $revoked->assignment?->partitionsByTopicId()[$topicId] ?? [];
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

        $handedOver = $this->heartbeatUntilAssigned($groupId, $second, $waiting->memberEpoch);

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

        // the new epoch comes with the assignment the change produced, which a 4.3 node computes in the background
        $assigned = $this->heartbeatUntilAssigned($groupId, $member, $answer->memberEpoch);

        self::assertGreaterThan($epoch, $assigned->memberEpoch, 'the change earned the member a new epoch');
        self::assertCount(2, $assigned->assignment?->partitionsByTopicId() ?? [], 'and the second topic');
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
     * A live classic group of the consumer protocol is UPGRADED by the first heartbeat that names it (Kafka 4.0)
     *
     * `group.consumer.migration.policy` is `bidirectional` by default since Kafka 4.0 (it was `disabled` on the
     * 3.9.2 node, which refused this very frame with the 69): `GroupMetadataManager.getOrMaybeCreateConsumerGroup`
     * @ 4.3.1 converts the classic group into a consumer group, and its classic member stays in it as a member of
     * the type 0 of KIP-1099.
     */
    public function testALiveClassicGroupIsUpgradedOnlineByAHeartbeat(): void
    {
        $topic   = $this->topic(1);
        $groupId = $this->uniqueGroupName();
        $classic = $this->classicGroup($groupId, $topic);

        $member = ConsumerGroupHeartbeatCoordinator::newMemberId();
        $answer = $this->send($this->coordinatorStream($groupId), ConsumerGroupHeartbeatRequest::forJoin(
            $groupId,
            $member,
            [$topic],
            self::REBALANCE_TIMEOUT_MS,
            clientId: self::CLIENT_ID
        ));
        self::$members[] = [$groupId, $member];

        self::assertSame(KafkaException::NO_ERROR, $answer->errorCode, 'the online upgrade of KIP-848');
        self::assertGreaterThan(0, $answer->memberEpoch);

        $admin = new AdminClient($this->cluster(), $this->configuration());

        self::assertSame('consumer', $admin->listAllGroups()[$groupId]->groupType, 'the group is a consumer group now');

        $members = $admin->describeConsumerGroup($groupId)->members;

        self::assertSame(ConsumerGroupDescribeMember::MEMBER_TYPE_CLASSIC, $members[$classic]->memberType);
        self::assertSame(ConsumerGroupDescribeMember::MEMBER_TYPE_CONSUMER, $members[$member]->memberType);
    }

    /**
     * A classic group of another protocol type cannot be upgraded, and that is the 69 of this api
     */
    public function testAClassicGroupOfAnotherProtocolTypeIsRefusedWithTheSixtyNine(): void
    {
        $topic   = $this->topic(1);
        $groupId = $this->uniqueGroupName();
        $this->classicGroup($groupId, $topic, 'connect');

        $answer = $this->send($this->coordinatorStream($groupId), ConsumerGroupHeartbeatRequest::forJoin(
            $groupId,
            ConsumerGroupHeartbeatCoordinator::newMemberId(),
            [$topic],
            self::REBALANCE_TIMEOUT_MS,
            clientId: self::CLIENT_ID
        ));

        self::assertSame(KafkaException::GROUP_ID_NOT_FOUND, $answer->errorCode);
        self::assertSame(
            "Cannot upgrade classic group {$groupId} to consumer group because the group does not use the consumer "
            . 'embedded protocol.',
            $answer->errorMessage,
            '`GroupMetadataManager.validateOnlineUpgrade` @ 4.3.1'
        );

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
     * Version 1 (KIP-1082) wants the member id the consumer generated, and refuses a frame without one with the 42
     */
    public function testAJoinWithoutAMemberIdIsTheFortyTwoFromVersionOneOn(): void
    {
        $topic   = $this->topic(1);
        $groupId = $this->uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);

        $refused = $this->send($stream, ConsumerGroupHeartbeatRequest::forJoin(
            $groupId,
            '',
            [$topic],
            self::REBALANCE_TIMEOUT_MS,
            clientId: self::CLIENT_ID
        ));

        self::assertSame(KafkaException::INVALID_REQUEST, $refused->errorCode);
        self::assertSame("MemberId can't be empty.", $refused->errorMessage);

        // version 0, which a 4.3.1 node still serves, generates the id for such a member
        ConsumerGroupHeartbeatRequestV0::forJoin(
            $groupId,
            '',
            [$topic],
            self::REBALANCE_TIMEOUT_MS,
            clientId: self::CLIENT_ID
        )->writeTo($stream);
        $generated = ConsumerGroupHeartbeatResponseV0::unpack($stream);

        self::assertSame(KafkaException::NO_ERROR, $generated->errorCode);
        self::assertNotNull($generated->memberId);
        self::assertMatchesRegularExpression(
            '/^[A-Za-z0-9_-]{22}$/',
            $generated->memberId,
            'the base64 uuid of the coordinator, the same format the Java consumer generates for itself'
        );
        self::$members[] = [$groupId, $generated->memberId];
    }

    /**
     * The regex subscription of version 1: the coordinator matches it and assigns what matched
     */
    public function testARegexSubscriptionIsResolvedByTheCoordinator(): void
    {
        $topic   = $this->topic(2);
        $groupId = $this->uniqueGroupName();
        $member  = ConsumerGroupHeartbeatCoordinator::newMemberId();
        $regex   = preg_quote($topic, '/') . '.*';

        $joined = $this->client()->joinConsumerGroup(
            $this->coordinator($groupId),
            $groupId,
            $member,
            [],
            self::REBALANCE_TIMEOUT_MS,
            subscribedTopicRegex: $regex
        );
        self::$members[] = [$groupId, $member];

        self::assertSame(KafkaException::NO_ERROR, $joined->errorCode);

        $assignment = $joined->assignment?->partitionsByTopicId() ?? [];
        if ($assignment === []) {
            // the coordinator resolves a regex in the background: the join itself may assign nothing yet
            $assignment = $this->heartbeatUntilAssigned($groupId, $member, $joined->memberEpoch)
                ->assignment?->partitionsByTopicId() ?? [];
        }

        self::assertSame([$this->topicIdOf($topic) => [0, 1]], $assignment, 'the one topic the regex matches');

        $described = new AdminClient($this->cluster(), $this->configuration())
            ->describeConsumerGroup($groupId)->members[$member];

        self::assertSame($regex, $described->subscribedTopicRegex);
        self::assertSame([], $described->subscribedTopicNames, 'the member named no topic');
    }

    /**
     * A regex the coordinator cannot compile is the 128 of Kafka 4.0, with the sentence of RE2/J
     */
    public function testAnInvalidRegexIsTheOneHundredAndTwentyEight(): void
    {
        $groupId = $this->uniqueGroupName();

        $answer = $this->send($this->coordinatorStream($groupId), ConsumerGroupHeartbeatRequest::forJoin(
            $groupId,
            ConsumerGroupHeartbeatCoordinator::newMemberId(),
            [],
            self::REBALANCE_TIMEOUT_MS,
            clientId: self::CLIENT_ID,
            subscribedTopicRegex: 't3-848-hb-(unclosed'
        ));

        self::assertSame(KafkaException::INVALID_REGULAR_EXPRESSION, $answer->errorCode);
        self::assertSame(
            'SubscribedTopicRegex `t3-848-hb-(unclosed` is not a valid regular expression: missing closing ).',
            $answer->errorMessage
        );

        $this->expectException(InvalidRegularExpressionException::class);

        $this->client()->joinConsumerGroup(
            $this->coordinator($groupId),
            $groupId,
            ConsumerGroupHeartbeatCoordinator::newMemberId(),
            [],
            self::REBALANCE_TIMEOUT_MS,
            subscribedTopicRegex: '[unclosed'
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
     *
     * @return string Member id of its one member
     */
    private function classicGroup(string $groupId, string $topic, string $protocolType = 'consumer'): string
    {
        $client = $this->client();
        $node   = $this->coordinator($groupId);

        try {
            $join = $client->joinGroup(
                $node,
                $groupId,
                '',
                $protocolType,
                ['range' => new Subscription([$topic])->pack()],
                self::REBALANCE_TIMEOUT_MS
            );
        } catch (\Protocol\Kafka\Common\Errors\MemberIdRequiredException $needsId) {
            $join = $client->joinGroup(
                $node,
                $groupId,
                (string) ($needsId->getContext()['assignedMemberId'] ?? ''),
                $protocolType,
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
            $protocolType,
            $join->groupProtocol
        );

        self::$members[] = [$groupId, $join->memberId];

        return $join->memberId;
    }

    /**
     * Heartbeats a member until an answer carries an assignment, which a 4.3 coordinator computes in the background
     */
    private function heartbeatUntilAssigned(string $groupId, string $memberId, int $epoch): ConsumerGroupHeartbeatResponse
    {
        $deadline = microtime(true) + self::ASSIGNMENT_TIMEOUT;
        $node     = $this->coordinator($groupId);

        while (true) {
            $answer = $this->client()->consumerGroupHeartbeat($node, $groupId, $memberId, $epoch);
            if ($answer->assignment !== null || microtime(true) >= $deadline) {
                return $answer;
            }
            $epoch = $answer->memberEpoch;
            usleep(200000);
        }
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
