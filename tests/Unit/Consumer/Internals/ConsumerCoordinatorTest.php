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
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\Internals\ConsumerCoordinator;
use Protocol\Kafka\Consumer\MemberAssignment;
use Protocol\Kafka\Consumer\RangeAssignor;
use Protocol\Kafka\Consumer\Subscription;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Request\HeartbeatRequest;
use Protocol\Kafka\Protocol\Request\JoinGroupRequest;
use Protocol\Kafka\Protocol\Request\SyncGroupRequest;
use Protocol\Kafka\Tests\Compliance\MessageFields;
use Protocol\Kafka\Tests\Fixture\BrokerConnection;
use Protocol\Kafka\Tests\Fixture\ResponseFrame;
use Protocol\Kafka\Tests\Fixture\ScriptedConnections;

/**
 * The membership state machine of a consumer, driven against scripted broker answers.
 *
 * The one thing that needs a script rather than a broker is the **79** of KIP-394 (Kafka 2.2, JoinGroup v4): a
 * first join is refused with the member id the coordinator assigned, and the coordinator of this client has to
 * send the very same request again with that id, immediately and without counting the refusal as a failure.
 *
 * The second one is **static membership** (KIP-345, Kafka 2.3): a consumer that carries a `group.instance.id`
 * sends it in every request of the protocol and must not leave its group when it is closed, which is what keeps
 * its partitions across a restart.
 *
 * @see docs/protocol/2.8.md, sections "The member id of a first join (v4, KIP-394)" and
 *      "Static membership (KIP-345)"
 */
#[CoversClass(ConsumerCoordinator::class)]
final class ConsumerCoordinatorTest extends TestCase
{
    private const string BOOTSTRAP_ADDRESS = 'tcp://bootstrap:9092';

    private const string COORDINATOR_ADDRESS = 'tcp://kafka-1:9092';

    private const string TOPIC = 'orders';

    private const string MEMBER_ID = 't3-client-2b6f1e2a-0000-4000-8000-000000000001';

    /**
     * `group.instance.id` of the static member of this class
     */
    private const string INSTANCE_ID = 't3-instance-one';

    private ScriptedConnections $brokers;

    protected function setUp(): void
    {
        $this->brokers = new ScriptedConnections();
    }

    protected function tearDown(): void
    {
        ScriptedConnections::uninstall();
    }

    public function testAFirstJoinIsSentAgainWithTheMemberIdTheSeventyNineCarries(): void
    {
        $assignment   = new MemberAssignment([self::TOPIC => [0]])->pack();
        $subscription = new Subscription([self::TOPIC])->pack();
        $coordinator  = new BrokerConnection(
            ResponseFrame::groupCoordinator(0, 0, 0, 'kafka-1', 9092),
            // The first join is refused with the id the coordinator generated, the way a v4 broker answers it
            ResponseFrame::joinGroup(0, KafkaException::MEMBER_ID_REQUIRED, -1, '', '', self::MEMBER_ID),
            // The second one is accepted and makes this member the leader of the generation
            ResponseFrame::joinGroup(0, 0, 1, 'range', self::MEMBER_ID, self::MEMBER_ID, [self::MEMBER_ID => $subscription]),
            ResponseFrame::syncGroup(0, 0, $assignment)
        );
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::COORDINATOR_ADDRESS, $coordinator)
            ->install();

        $partitions = $this->coordinator()->ensureActiveGroup(
            [self::TOPIC],
            static fn(array $topics): array => [self::TOPIC => [0]]
        );

        self::assertSame([self::TOPIC => [0]], $partitions);

        $frames = $coordinator->getReceivedFrames();

        // The coordinator lookup, the refused join, the accepted join and the sync: four frames, no backoff
        self::assertCount(4, $frames);
        self::assertSame(ApiKeys::GROUP_COORDINATOR, $this->apiKeyOf($frames[0]));
        self::assertSame(ApiKeys::JOIN_GROUP, $this->apiKeyOf($frames[1]));
        self::assertSame(ApiKeys::JOIN_GROUP, $this->apiKeyOf($frames[2]));
        self::assertSame(ApiKeys::SYNC_GROUP, $this->apiKeyOf($frames[3]));

        $first  = MessageFields::of(JoinGroupRequest::unpack(new StringStream($this->framed($frames[1]))));
        $second = MessageFields::of(JoinGroupRequest::unpack(new StringStream($this->framed($frames[2]))));

        self::assertSame(6, $first['apiVersion'], 'the client sends JoinGroup v6');
        self::assertSame(JoinGroupRequest::DEFAULT_MEMBER_ID, $first['memberId']);
        self::assertSame(self::MEMBER_ID, $second['memberId'], 'the second join carries the assigned id');
        self::assertSame(
            substr(bin2hex($frames[1]), 0, 8),
            substr(bin2hex($frames[2]), 0, 8),
            'and is otherwise the very same request: the api key and the version did not change'
        );
    }

    public function testTheRefusedJoinIsNotCountedAsAFailedRebalanceAttempt(): void
    {
        $assignment   = new MemberAssignment([self::TOPIC => [0]])->pack();
        $subscription = new Subscription([self::TOPIC])->pack();
        $coordinator  = new BrokerConnection(
            ResponseFrame::groupCoordinator(0, 0, 0, 'kafka-1', 9092),
            // Four rebalances that are interrupted, and the fifth attempt succeeds - every one of them starting
            // with the 79 that a first join is answered with, which must not eat an attempt of its own
            ResponseFrame::joinGroup(0, KafkaException::MEMBER_ID_REQUIRED, -1, '', '', self::MEMBER_ID),
            ResponseFrame::joinGroup(0, 0, 1, 'range', self::MEMBER_ID, self::MEMBER_ID, [self::MEMBER_ID => $subscription]),
            ResponseFrame::syncGroup(0, KafkaException::REBALANCE_IN_PROGRESS, ''),
            ResponseFrame::joinGroup(0, 0, 2, 'range', self::MEMBER_ID, self::MEMBER_ID, [self::MEMBER_ID => $subscription]),
            ResponseFrame::syncGroup(0, 0, $assignment)
        );
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::COORDINATOR_ADDRESS, $coordinator)
            ->install();

        $membership = $this->coordinator();
        $partitions = $membership->ensureActiveGroup(
            [self::TOPIC],
            static fn(array $topics): array => [self::TOPIC => [0]]
        );

        self::assertSame([self::TOPIC => [0]], $partitions);
        self::assertSame(2, $membership->getGenerationId(), 'the second generation is the one that synced');
        self::assertSame(
            self::MEMBER_ID,
            $membership->getMemberId(),
            'the member kept the id of the 79 through the interrupted rebalance'
        );
    }

    /**
     * A 79 without a member id is a broken answer and is reported instead of being retried for ever
     */
    public function testASeventyNineWithoutAMemberIdIsReported(): void
    {
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::COORDINATOR_ADDRESS, new BrokerConnection(
                ResponseFrame::groupCoordinator(0, 0, 0, 'kafka-1', 9092),
                ...array_fill(0, 5, ResponseFrame::joinGroup(0, KafkaException::MEMBER_ID_REQUIRED, -1, '', '', ''))
            ))
            ->install();

        $this->expectException(\Protocol\Kafka\Common\Errors\MemberIdRequiredException::class);

        $this->coordinator()->ensureActiveGroup([self::TOPIC], static fn(array $topics): array => []);
    }

    /**
     * A static member names itself in every request of the protocol, and its first join is not refused
     */
    public function testAStaticMemberSendsItsInstanceIdInEveryRequestOfTheProtocol(): void
    {
        $assignment   = new MemberAssignment([self::TOPIC => [0]])->pack();
        $subscription = new Subscription([self::TOPIC])->pack();
        $coordinator  = new BrokerConnection(
            ResponseFrame::groupCoordinator(0, 0, 0, 'kafka-1', 9092),
            ResponseFrame::joinGroup(0, 0, 1, 'range', self::MEMBER_ID, self::MEMBER_ID, [self::MEMBER_ID => $subscription]),
            ResponseFrame::syncGroup(0, 0, $assignment),
            ResponseFrame::heartbeat(0, 0)
        );
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::COORDINATOR_ADDRESS, $coordinator)
            ->install();

        $membership = $this->coordinator(self::INSTANCE_ID);
        $membership->ensureActiveGroup([self::TOPIC], static fn(array $topics): array => [self::TOPIC => [0]]);

        self::assertTrue($membership->isStaticMember());
        self::assertSame(self::INSTANCE_ID, $membership->getGroupInstanceId());
        self::assertTrue($membership->maybeHeartbeat((int) (microtime(true) * 1e3) + 10000));

        $frames = $coordinator->getReceivedFrames();

        // The lookup, one join - no 79 exchange - the sync and the heartbeat
        self::assertCount(4, $frames);
        self::assertSame(ApiKeys::JOIN_GROUP, $this->apiKeyOf($frames[1]));

        $join = MessageFields::of(JoinGroupRequest::unpack(new StringStream($this->framed($frames[1]))));

        self::assertSame(self::INSTANCE_ID, $join['groupInstanceId'], 'the join names the instance');
        self::assertSame(
            self::INSTANCE_ID,
            MessageFields::of(SyncGroupRequest::unpack(new StringStream($this->framed($frames[2]))))['groupInstanceId'],
            'and so does the sync'
        );
        self::assertSame(
            self::INSTANCE_ID,
            MessageFields::of(HeartbeatRequest::unpack(new StringStream($this->framed($frames[3]))))['groupInstanceId'],
            'and the heartbeat'
        );
    }

    /**
     * A dynamic member writes the `null` of the field, which is what every consumer below Kafka 2.3 is
     */
    public function testADynamicMemberSendsNoInstanceIdAtAll(): void
    {
        $subscription = new Subscription([self::TOPIC])->pack();
        $coordinator  = new BrokerConnection(
            ResponseFrame::groupCoordinator(0, 0, 0, 'kafka-1', 9092),
            ResponseFrame::joinGroup(0, 0, 1, 'range', self::MEMBER_ID, self::MEMBER_ID, [self::MEMBER_ID => $subscription]),
            ResponseFrame::syncGroup(0, 0, new MemberAssignment([self::TOPIC => [0]])->pack())
        );
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::COORDINATOR_ADDRESS, $coordinator)
            ->install();

        $membership = $this->coordinator();
        $membership->ensureActiveGroup([self::TOPIC], static fn(array $topics): array => [self::TOPIC => [0]]);

        self::assertFalse($membership->isStaticMember());
        self::assertNull($membership->getGroupInstanceId());

        $frames = $coordinator->getReceivedFrames();
        $join   = MessageFields::of(JoinGroupRequest::unpack(new StringStream($this->framed($frames[1]))));

        self::assertNull($join['groupInstanceId']);
    }

    /**
     * A static member does not leave its group: the coordinator holds its partitions while it restarts
     */
    public function testAStaticMemberSendsNoLeaveGroup(): void
    {
        $subscription = new Subscription([self::TOPIC])->pack();
        $coordinator  = new BrokerConnection(
            ResponseFrame::groupCoordinator(0, 0, 0, 'kafka-1', 9092),
            ResponseFrame::joinGroup(0, 0, 1, 'range', self::MEMBER_ID, self::MEMBER_ID, [self::MEMBER_ID => $subscription]),
            ResponseFrame::syncGroup(0, 0, new MemberAssignment([self::TOPIC => [0]])->pack())
        );
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::COORDINATOR_ADDRESS, $coordinator)
            ->install();

        $membership = $this->coordinator(self::INSTANCE_ID);
        $membership->ensureActiveGroup([self::TOPIC], static fn(array $topics): array => [self::TOPIC => [0]]);
        $membership->leaveGroup();

        self::assertCount(3, $coordinator->getReceivedFrames(), 'no LeaveGroup was sent');
        self::assertFalse($membership->isMember(), 'the local membership is forgotten all the same');
        self::assertTrue($membership->needsRejoin(), 'so that the next poll() joins again under the same instance');
    }

    /**
     * The membership of a group whose coordinator is the scripted broker
     */
    private function coordinator(?string $groupInstanceId = null): ConsumerCoordinator
    {
        $configuration = [
            ClientConfig::BOOTSTRAP_SERVERS         => [self::BOOTSTRAP_ADDRESS],
            ClientConfig::CLIENT_ID                 => 't3-client',
            ClientConfig::REQUEST_TIMEOUT_MS        => 500,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 1000,
            ClientConfig::RETRY_BACKOFF_MS          => 1,
            ClientConfig::RETRIES                   => 0,
            ConsumerConfig::SESSION_TIMEOUT_MS      => 10000,
        ] + ConsumerConfig::getDefaultConfiguration();

        $client = new Client(Cluster::bootstrap($configuration), $configuration);

        return new ConsumerCoordinator($client, 't3-group', new RangeAssignor(), 3000, 1, null, $groupInstanceId);
    }

    /**
     * A one-broker cluster whose only node is the scripted coordinator
     */
    private function clusterMetadata(): string
    {
        return ResponseFrame::metadata(0, [[0, 'kafka-1', 9092]], [self::TOPIC => [0 => 0]]);
    }

    /**
     * Returns the api key of a request frame that a scripted broker received
     */
    private function apiKeyOf(string $frame): int
    {
        return (int) unpack('n', substr($frame, 0, 2))[1];
    }

    /**
     * Puts the Size field back in front of a received frame, so that a request class can decode it
     */
    private function framed(string $frame): string
    {
        return pack('N', strlen($frame)) . $frame;
    }
}
