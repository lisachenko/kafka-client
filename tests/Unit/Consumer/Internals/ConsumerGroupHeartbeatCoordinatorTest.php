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

namespace Protocol\Kafka\Tests\Unit\Consumer\Internals;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\UnsupportedAssignorException;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\Internals\ConsumerGroupHeartbeatCoordinator;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Request\ConsumerGroupHeartbeatRequest;
use Protocol\Kafka\Tests\Compliance\MessageFields;
use Protocol\Kafka\Tests\Fixture\BrokerConnection;
use Protocol\Kafka\Tests\Fixture\ResponseFrame;
use Protocol\Kafka\Tests\Fixture\ScriptedConnections;

/**
 * The membership state machine of a **KIP-848** consumer, driven against scripted answers of key 68.
 *
 * What needs a script rather than a broker here is the *sequence*: the reconciliation of the new protocol is a
 * conversation of several frames, and every rule of it can only be seen by looking at what the client sent after
 * what it was told. The three that matter are
 *
 * * the **join** - a member id the member generated itself, the epoch 0 and the empty `topic_partitions` - and the
 *   **acknowledgement** that follows it, which echoes the partitions the coordinator handed over;
 * * the **incremental** revoke: only the partitions that were really taken away, never the whole assignment;
 * * the **fencing**, where a member that is answered 110 joins again with a *new* member id and the epoch 0.
 *
 * The frames the node really sent for each of these are the vectors of `consumer-group-heartbeat.json`.
 *
 * @see docs/protocol/3.9.md, section "ConsumerGroupHeartbeat API (key 68, v0)"
 */
#[CoversClass(ConsumerGroupHeartbeatCoordinator::class)]
final class ConsumerGroupHeartbeatCoordinatorTest extends TestCase
{
    private const string BOOTSTRAP_ADDRESS = 'tcp://bootstrap:9092';

    private const string COORDINATOR_ADDRESS = 'tcp://kafka-1:9092';

    private const string TOPIC = 'orders';

    private const string GROUP_ID = 't3-848-group';

    private ScriptedConnections $brokers;

    protected function setUp(): void
    {
        $this->brokers = new ScriptedConnections();
    }

    protected function tearDown(): void
    {
        ScriptedConnections::uninstall();
    }

    /**
     * The first poll of a member: the join, and the acknowledgement of what it was given
     */
    public function testAJoinIsFollowedByTheAcknowledgementOfItsAssignment(): void
    {
        $coordinator = new BrokerConnection(
            ResponseFrame::groupCoordinator(0, 0, 0, 'kafka-1', 9092),
            ResponseFrame::consumerGroupHeartbeat(0, assignment: [$this->topicId() => [0, 1]]),
            ResponseFrame::consumerGroupHeartbeat(0, memberEpoch: 2)
        );
        $this->connect($coordinator);

        $membership = $this->membership();
        $partitions = $membership->ensureActiveGroup([self::TOPIC], static fn(array $topics): array => []);

        self::assertSame([self::TOPIC => [0, 1]], $partitions, 'the topic ids are resolved into names');
        self::assertSame(2, $membership->getGenerationId(), 'the member epoch is the generation of this protocol');
        self::assertFalse($membership->needsRejoin());
        self::assertSame(5000, $membership->getHeartbeatIntervalMs(), 'the interval the coordinator dictated');

        $frames = $coordinator->getReceivedFrames();

        self::assertCount(3, $frames);
        self::assertSame(ApiKeys::GROUP_COORDINATOR, $this->apiKeyOf($frames[0]));
        self::assertSame(ApiKeys::CONSUMER_GROUP_HEARTBEAT, $this->apiKeyOf($frames[1]));
        self::assertSame(ApiKeys::CONSUMER_GROUP_HEARTBEAT, $this->apiKeyOf($frames[2]));

        $join = $this->heartbeatOf($frames[1]);

        self::assertSame(0, $join['apiVersion'], 'the api has one version');
        self::assertSame(ConsumerGroupHeartbeatRequest::JOIN_MEMBER_EPOCH, $join['memberEpoch']);
        self::assertSame([self::TOPIC], $join['subscribedTopicNames']);
        self::assertSame([], $join['topicPartitions'], 'a (re-)join carries the EMPTY array, never the null');
        self::assertNotSame('', $join['memberId'], 'the member generated an id for itself');
        self::assertSame(300000, $join['rebalanceTimeoutMs'], 'max.poll.interval.ms');

        $acknowledgement = $this->heartbeatOf($frames[2]);

        self::assertSame(1, $acknowledgement['memberEpoch'], 'the epoch the join was answered');
        self::assertNull($acknowledgement['subscribedTopicNames'], 'unchanged since the last heartbeat');
        self::assertSame(-1, $acknowledgement['rebalanceTimeoutMs'], 'and so is the rebalance timeout');
        self::assertSame(
            [['topicId' => ['$bytes' => bin2hex($this->topicId())], 'partitions' => [0, 1]]],
            $acknowledgement['topicPartitions'],
            'the acknowledgement echoes what the member owns now, by topic id'
        );
    }

    /**
     * A steady-state heartbeat is the group id, the member id, the epoch and five nulls
     */
    public function testAHeartbeatOfASettledMemberCarriesNothingButItsEpoch(): void
    {
        $coordinator = new BrokerConnection(
            ResponseFrame::groupCoordinator(0, 0, 0, 'kafka-1', 9092),
            ResponseFrame::consumerGroupHeartbeat(0, assignment: []),
            ResponseFrame::consumerGroupHeartbeat(0)
        );
        $this->connect($coordinator);

        $membership = $this->membership();
        $membership->ensureActiveGroup([self::TOPIC], static fn(array $topics): array => []);

        self::assertTrue($membership->maybeHeartbeat($this->nowMs() + 10000));

        $steady = $this->heartbeatOf($coordinator->getReceivedFrames()[2]);

        self::assertNull($steady['instanceId']);
        self::assertNull($steady['rackId']);
        self::assertNull($steady['subscribedTopicNames']);
        self::assertNull($steady['serverAssignor']);
        self::assertNull($steady['topicPartitions']);
        self::assertSame(-1, $steady['rebalanceTimeoutMs']);
    }

    /**
     * The interval of the answer is honoured, not the `heartbeat.interval.ms` of the consumer
     */
    public function testTheCoordinatorDictatesTheHeartbeatInterval(): void
    {
        $this->connect(new BrokerConnection(
            ResponseFrame::groupCoordinator(0, 0, 0, 'kafka-1', 9092),
            ResponseFrame::consumerGroupHeartbeat(0, heartbeatIntervalMs: 20000, assignment: []),
            ResponseFrame::consumerGroupHeartbeat(0, heartbeatIntervalMs: 20000)
        ));

        $membership = $this->membership();
        $membership->ensureActiveGroup([self::TOPIC], static fn(array $topics): array => []);

        self::assertSame(20000, $membership->getHeartbeatIntervalMs());
        self::assertFalse(
            $membership->maybeHeartbeat($this->nowMs() + 5000),
            'the 3000 of heartbeat.interval.ms has nothing to say in this protocol'
        );
        self::assertTrue($membership->maybeHeartbeat($this->nowMs() + 25000));
    }

    /**
     * Only the partitions the coordinator really took away are revoked, and only the new ones are announced
     */
    public function testTheRebalanceOfThisProtocolIsIncremental(): void
    {
        $coordinator = new BrokerConnection(
            ResponseFrame::groupCoordinator(0, 0, 0, 'kafka-1', 9092),
            ResponseFrame::consumerGroupHeartbeat(0, assignment: [$this->topicId() => [0, 1, 2]]),
            ResponseFrame::consumerGroupHeartbeat(0, memberEpoch: 1),
            // The second member of the group made the coordinator take two partitions away from this one
            ResponseFrame::consumerGroupHeartbeat(0, memberEpoch: 1, assignment: [$this->topicId() => [2]]),
            ResponseFrame::consumerGroupHeartbeat(0, memberEpoch: 4)
        );
        $this->connect($coordinator);

        $membership = $this->membership();
        $owned      = $membership->ensureActiveGroup([self::TOPIC], static fn(array $topics): array => []);

        self::assertSame([self::TOPIC => [0, 1, 2]], $owned);
        self::assertSame(
            [self::TOPIC => [0, 1, 2]],
            $membership->partitionsToAssign([], $owned),
            'a member that owned nothing is told about everything it was given'
        );

        // The heartbeat that carries the revocation, which is what a poll() runs into
        self::assertTrue($membership->maybeHeartbeat($this->nowMs() + 10000));
        self::assertTrue($membership->needsRejoin(), 'an assignment that changed asks for a reconciliation');
        self::assertSame(
            [self::TOPIC => [0, 1]],
            $membership->partitionsToRevoke($owned),
            'only the two partitions that were taken away, never the whole assignment'
        );

        $kept = $membership->ensureActiveGroup([self::TOPIC], static fn(array $topics): array => []);

        self::assertSame([self::TOPIC => [2]], $kept);
        self::assertSame([], $membership->partitionsToAssign($owned, $kept), 'nothing was added');
        self::assertSame(4, $membership->getGenerationId(), 'the acknowledgement earned the group epoch');
    }

    /**
     * A null assignment means "nothing changed" and must not be read as "you own nothing"
     */
    public function testANullAssignmentKeepsWhatTheMemberOwns(): void
    {
        $this->connect(new BrokerConnection(
            ResponseFrame::groupCoordinator(0, 0, 0, 'kafka-1', 9092),
            ResponseFrame::consumerGroupHeartbeat(0, assignment: [$this->topicId() => [0, 1]]),
            ResponseFrame::consumerGroupHeartbeat(0, memberEpoch: 2),
            ResponseFrame::consumerGroupHeartbeat(0, memberEpoch: 2),
            ResponseFrame::consumerGroupHeartbeat(0, memberEpoch: 2)
        ));

        $membership = $this->membership();
        $membership->ensureActiveGroup([self::TOPIC], static fn(array $topics): array => []);

        self::assertTrue($membership->maybeHeartbeat($this->nowMs() + 10000));
        self::assertFalse($membership->needsRejoin(), 'a null assignment changes nothing at all');
        self::assertSame(
            [],
            $membership->partitionsToRevoke([self::TOPIC => [0, 1]]),
            'and revokes nothing'
        );
    }

    /**
     * A fenced member joins again with the epoch 0 and a NEW member id
     */
    public function testAFencedMemberJoinsAgainUnderANewMemberId(): void
    {
        $coordinator = new BrokerConnection(
            ResponseFrame::groupCoordinator(0, 0, 0, 'kafka-1', 9092),
            ResponseFrame::consumerGroupHeartbeat(0, assignment: []),
            // The coordinator fences the member on its next heartbeat
            ResponseFrame::consumerGroupHeartbeat(
                0,
                KafkaException::FENCED_MEMBER_EPOCH,
                memberEpoch: 0,
                heartbeatIntervalMs: 0,
                errorMessage: 'The consumer group member has a smaller member epoch (1) than the one known by the '
                . 'group coordinator (3). The member must abandon all its partitions and rejoin.'
            ),
            ResponseFrame::consumerGroupHeartbeat(0, memberEpoch: 4, assignment: [])
        );
        $this->connect($coordinator);

        $membership = $this->membership();
        $membership->ensureActiveGroup([self::TOPIC], static fn(array $topics): array => []);
        $firstId = $membership->getMemberId();

        self::assertTrue($membership->maybeHeartbeat($this->nowMs() + 10000));
        self::assertSame('', $membership->getMemberId(), 'a fenced member forgets its identity');
        self::assertTrue($membership->needsRejoin());

        $membership->ensureActiveGroup([self::TOPIC], static fn(array $topics): array => []);

        self::assertNotSame($firstId, $membership->getMemberId(), 'and comes back as a new member');
        self::assertSame(4, $membership->getGenerationId());

        $rejoin = $this->heartbeatOf($coordinator->getReceivedFrames()[3]);

        self::assertSame(ConsumerGroupHeartbeatRequest::JOIN_MEMBER_EPOCH, $rejoin['memberEpoch']);
        self::assertSame([], $rejoin['topicPartitions'], 'a rejoin is a join: the empty array again');
    }

    /**
     * The 112 of an assignor the broker does not have is not retried at all
     */
    public function testAnUnsupportedAssignorReachesTheCaller(): void
    {
        $this->connect(new BrokerConnection(
            ResponseFrame::groupCoordinator(0, 0, 0, 'kafka-1', 9092),
            ...array_fill(0, 5, ResponseFrame::consumerGroupHeartbeat(
                0,
                KafkaException::UNSUPPORTED_ASSIGNOR,
                memberEpoch: 0,
                heartbeatIntervalMs: 0,
                errorMessage: 'ServerAssignor sticky is not supported. Supported assignors: uniform, range.'
            ))
        ));

        $this->expectException(UnsupportedAssignorException::class);

        $this->membership(serverAssignor: 'sticky')
            ->ensureActiveGroup([self::TOPIC], static fn(array $topics): array => []);
    }

    /**
     * A dynamic member leaves with the epoch -1, a static one with the -2 of "I will be back"
     */
    public function testALeaveIsTheEpochMinusOneAndAStaticLeaveTheEpochMinusTwo(): void
    {
        $dynamic = new BrokerConnection(
            ResponseFrame::groupCoordinator(0, 0, 0, 'kafka-1', 9092),
            ResponseFrame::consumerGroupHeartbeat(0, assignment: []),
            ResponseFrame::consumerGroupHeartbeat(0, memberEpoch: -1, heartbeatIntervalMs: 0)
        );
        $this->connect($dynamic);

        $membership = $this->membership();
        $membership->ensureActiveGroup([self::TOPIC], static fn(array $topics): array => []);
        $membership->leaveGroup();

        $leave = $this->heartbeatOf($dynamic->getReceivedFrames()[2]);

        self::assertSame(ConsumerGroupHeartbeatRequest::LEAVE_MEMBER_EPOCH, $leave['memberEpoch']);
        self::assertNull($leave['topicPartitions']);
        self::assertNull($leave['instanceId']);
        self::assertFalse($membership->isMember(), 'the membership is gone either way');

        ScriptedConnections::uninstall();
        $this->brokers = new ScriptedConnections();

        $static = new BrokerConnection(
            ResponseFrame::groupCoordinator(0, 0, 0, 'kafka-1', 9092),
            ResponseFrame::consumerGroupHeartbeat(0, assignment: []),
            ResponseFrame::consumerGroupHeartbeat(0, memberEpoch: -2, heartbeatIntervalMs: 0)
        );
        $this->connect($static);

        $instance = $this->membership(groupInstanceId: 't3-848-one');
        $instance->ensureActiveGroup([self::TOPIC], static fn(array $topics): array => []);
        $instance->leaveGroup();

        $staticLeave = $this->heartbeatOf($static->getReceivedFrames()[2]);

        self::assertTrue($instance->isStaticMember());
        self::assertSame(ConsumerGroupHeartbeatRequest::STATIC_LEAVE_MEMBER_EPOCH, $staticLeave['memberEpoch']);
        self::assertSame('t3-848-one', $staticLeave['instanceId']);
    }

    /**
     * A subscription that changed travels in an ordinary heartbeat: there is nothing to re-join for
     */
    public function testASubscriptionThatChangedIsAnOrdinaryHeartbeat(): void
    {
        $coordinator = new BrokerConnection(
            ResponseFrame::groupCoordinator(0, 0, 0, 'kafka-1', 9092),
            ResponseFrame::consumerGroupHeartbeat(0, assignment: []),
            ResponseFrame::consumerGroupHeartbeat(0, memberEpoch: 2)
        );
        $this->connect($coordinator);

        $membership = $this->membership();
        $membership->ensureActiveGroup([self::TOPIC], static fn(array $topics): array => []);
        $membership->requestRejoin('the subscription of the consumer changed');
        $membership->ensureActiveGroup([self::TOPIC, 'invoices'], static fn(array $topics): array => []);

        $frames = $coordinator->getReceivedFrames();

        self::assertCount(3, $frames, 'no second join was sent');

        $change = $this->heartbeatOf($frames[2]);

        self::assertSame(1, $change['memberEpoch'], 'the member kept its epoch and its identity');
        self::assertSame([self::TOPIC, 'invoices'], $change['subscribedTopicNames']);
        self::assertSame(2, $membership->getGenerationId(), 'and was answered a new one');
    }

    /**
     * The membership of a group whose coordinator is the scripted broker
     */
    private function membership(
        ?string $groupInstanceId = null,
        ?string $serverAssignor = null
    ): ConsumerGroupHeartbeatCoordinator {
        $configuration = [
            ClientConfig::BOOTSTRAP_SERVERS         => [self::BOOTSTRAP_ADDRESS],
            ClientConfig::CLIENT_ID                 => 't3-client',
            ClientConfig::REQUEST_TIMEOUT_MS        => 500,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 1000,
            ClientConfig::RETRY_BACKOFF_MS          => 1,
            ClientConfig::RETRIES                   => 0,
        ] + ConsumerConfig::getDefaultConfiguration();

        $cluster = Cluster::bootstrap($configuration);

        return new ConsumerGroupHeartbeatCoordinator(
            new Client($cluster, $configuration),
            $cluster,
            self::GROUP_ID,
            300000,
            1,
            $groupInstanceId,
            $serverAssignor
        );
    }

    private function connect(BrokerConnection $coordinator): void
    {
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::COORDINATOR_ADDRESS, $coordinator)
            ->install();
    }

    /**
     * A one-broker cluster whose only node is the scripted coordinator
     */
    private function clusterMetadata(): string
    {
        return ResponseFrame::metadata(0, [[0, 'kafka-1', 9092]], [self::TOPIC => [0 => 0, 1 => 0, 2 => 0]]);
    }

    /**
     * The 16 raw bytes the metadata fixture gives the topic of this class
     */
    private function topicId(): string
    {
        return ResponseFrame::topicIdOf(self::TOPIC);
    }

    /**
     * @return array<string, mixed>
     */
    private function heartbeatOf(string $frame): array
    {
        return MessageFields::of(
            ConsumerGroupHeartbeatRequest::unpack(new StringStream(pack('N', strlen($frame)) . $frame))
        );
    }

    private function apiKeyOf(string $frame): int
    {
        return (int) unpack('n', substr($frame, 0, 2))[1];
    }

    private function nowMs(): int
    {
        return (int) (microtime(true) * 1e3);
    }
}
