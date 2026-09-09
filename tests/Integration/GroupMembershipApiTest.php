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
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\CoordinatorLookup;
use Protocol\Kafka\Common\Errors\IllegalGenerationException;
use Protocol\Kafka\Common\Errors\InvalidSessionTimeoutException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\UnknownMemberIdException;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\MemberAssignment;
use Protocol\Kafka\Consumer\Subscription;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Data\JoinGroupRequestProtocol;
use Protocol\Kafka\Protocol\Data\JoinGroupResponseMember;
use Protocol\Kafka\Protocol\Data\SyncGroupRequestMember;
use Protocol\Kafka\Protocol\Request\HeartbeatRequest;
use Protocol\Kafka\Protocol\Request\HeartbeatResponse;
use Protocol\Kafka\Protocol\Request\JoinGroupRequest;
use Protocol\Kafka\Protocol\Request\JoinGroupResponse;
use Protocol\Kafka\Protocol\Request\LeaveGroupRequest;
use Protocol\Kafka\Protocol\Request\LeaveGroupResponse;
use Protocol\Kafka\Protocol\Request\SyncGroupRequest;
use Protocol\Kafka\Protocol\Request\SyncGroupResponse;

/**
 * Verifies the four apis of the group membership protocol against a real Kafka 0.9.0.1 broker.
 *
 * Kafka 0.9 moved the coordination of a group out of ZooKeeper into the coordinator broker, and these tests drive
 * the whole cycle of a generation over raw requests: a member joins, the leader publishes an assignment, the
 * members heartbeat, a second member joins and forces a rebalance, and both leave. The payloads - the metadata of a
 * member and its assignment - are opaque byte arrays to these apis, so arbitrary bytes are used for them here; the
 * `consumer` structures that really go in there belong to another ticket.
 *
 * @see docs/protocol/0.11.0.md, sections "Group membership protocol (keys 11 to 14)", "JoinGroup API (key 11, v0 and v1)",
 *      "SyncGroup API (key 14, v0)", "Heartbeat API (key 12, v0)" and "LeaveGroup API (key 13, v0)"
 */
#[CoversClass(Client::class)]
#[CoversClass(JoinGroupRequest::class)]
#[CoversClass(JoinGroupResponse::class)]
#[CoversClass(JoinGroupRequestProtocol::class)]
#[CoversClass(JoinGroupResponseMember::class)]
#[CoversClass(SyncGroupRequest::class)]
#[CoversClass(SyncGroupResponse::class)]
#[CoversClass(SyncGroupRequestMember::class)]
#[CoversClass(HeartbeatRequest::class)]
#[CoversClass(HeartbeatResponse::class)]
#[CoversClass(LeaveGroupRequest::class)]
#[CoversClass(LeaveGroupResponse::class)]
final class GroupMembershipApiTest extends IntegrationTestCase
{
    /**
     * Protocol type of the consumer groups, the only one Kafka itself knows about
     */
    private const string PROTOCOL_TYPE = 'consumer';

    /**
     * Name of the protocol this test offers; its metadata is opaque to the coordinator
     */
    private const string PROTOCOL_NAME = 'range';

    /**
     * Session timeout of every member here: `group.min.session.timeout.ms` of the container is 1000
     */
    private const int SESSION_TIMEOUT_MS = 6000;

    /**
     * Rebalance timeout of every member here, the `rebalance_timeout` of the JoinGroup v1 request
     *
     * The coordinator waits this long - not the session timeout - for a member to rejoin a rebalance, so it bounds
     * every JoinGroup this class sends and has to stay below {@see self::REQUEST_TIMEOUT_MS}.
     */
    private const int REBALANCE_TIMEOUT_MS = 8000;

    /**
     * Read timeout of the connections, which has to cover a JoinGroup that waits for the whole rebalance
     */
    private const int REQUEST_TIMEOUT_MS = 20000;

    /**
     * How long a heartbeat is asked again for the answer a rebalance is expected to produce, in seconds
     */
    private const float REBALANCE_TIMEOUT = 15.0;

    /**
     * The cluster is resolved once: every test of this class talks to the same brokers
     */
    private static ?Cluster $sharedCluster = null;

    public function testAFreshGroupAssignsAMemberIdAndMakesTheFirstMemberItsLeader(): void
    {
        $groupId = self::uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);

        $join = $this->join($stream, $groupId, JoinGroupRequest::DEFAULT_MEMBER_ID, 'metadata of the first member');

        self::assertSame(KafkaException::NO_ERROR, $join->errorCode);
        self::assertSame(1, $join->generationId, 'the first generation of a group is 1');
        self::assertSame(self::PROTOCOL_NAME, $join->groupProtocol, 'the coordinator picked the offered protocol');
        self::assertSame($join->memberId, $join->leaderId, 'the only member of a group is its leader');
        self::assertMatchesRegularExpression(
            '/^' . preg_quote($this->clientId(), '/') . '-[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/',
            $join->memberId,
            'the coordinator builds the member id as "<client id>-<uuid>"'
        );
        self::assertSame(
            [$join->memberId => 'metadata of the first member'],
            array_map(static fn(JoinGroupResponseMember $member): string => $member->metadata, $join->members),
            'the answer of the leader carries the metadata of every member, indexed by the member id'
        );

        $this->leave($stream, $groupId, $join->memberId);
    }

    public function testTheLeaderPublishesTheAssignmentOfTheGenerationAndGetsItsOwnShareBack(): void
    {
        $groupId = self::uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);
        $join    = $this->join($stream, $groupId, JoinGroupRequest::DEFAULT_MEMBER_ID, 'metadata');

        $sync = $this->sync($stream, $groupId, $join->memberId, $join->generationId, [
            $join->memberId => "\x00assignment of the leader\xff",
        ]);

        self::assertSame(KafkaException::NO_ERROR, $sync->errorCode);
        self::assertSame(
            "\x00assignment of the leader\xff",
            $sync->memberAssignment,
            'the coordinator hands the bytes back untouched, it never parses them'
        );

        $heartbeat = $this->heartbeat($stream, $groupId, $join->memberId, $join->generationId);

        self::assertSame(KafkaException::NO_ERROR, $heartbeat->errorCode, 'the group is stable after the sync');

        $this->leave($stream, $groupId, $join->memberId);
    }

    /**
     * The whole rebalance: the second member joins, which the first one learns from its heartbeat; it rejoins, the
     * generation is incremented and the leader assigns a share to both of them.
     *
     * The JoinGroup of the second member is written but not read until the first member has rejoined, because the
     * coordinator holds that answer back for exactly that long - which is the blocking behaviour of the api.
     */
    public function testASecondMemberForcesARebalanceThatTheHeartbeatOfTheFirstOneReports(): void
    {
        $groupId     = self::uniqueGroupName();
        $firstStream = $this->coordinatorStream($groupId);
        $first       = $this->join($firstStream, $groupId, JoinGroupRequest::DEFAULT_MEMBER_ID, 'first');
        $this->sync($firstStream, $groupId, $first->memberId, $first->generationId, [
            $first->memberId => 'everything',
        ]);

        // A second connection, so that the join of the second member can be left unanswered while the first one acts
        $secondStream = $this->newCoordinatorStream($groupId);
        new JoinGroupRequest(
            $groupId,
            self::SESSION_TIMEOUT_MS,
            self::REBALANCE_TIMEOUT_MS,
            JoinGroupRequest::DEFAULT_MEMBER_ID,
            self::PROTOCOL_TYPE,
            [self::PROTOCOL_NAME => 'second'],
            $this->clientId(),
            201
        )->writeTo($secondStream);

        $rebalancing = $this->heartbeatUntil($firstStream, $groupId, $first, KafkaException::REBALANCE_IN_PROGRESS);

        self::assertSame(
            KafkaException::REBALANCE_IN_PROGRESS,
            $rebalancing,
            'a heartbeat is how a member learns that the group started to rebalance'
        );

        // The first member rejoins with the member id of the previous generation, which completes the rebalance
        $firstAgain = $this->join($firstStream, $groupId, $first->memberId, 'first');
        $second     = JoinGroupResponse::unpack($secondStream);

        self::assertSame(KafkaException::NO_ERROR, $second->errorCode);
        self::assertSame(2, $firstAgain->generationId, 'the generation is incremented by the rebalance');
        self::assertSame($firstAgain->generationId, $second->generationId, 'both members are in the same generation');
        self::assertSame($first->memberId, $firstAgain->memberId, 'a member keeps its id across a rebalance');
        self::assertSame($first->memberId, $firstAgain->leaderId, 'the first member stays the leader of the group');
        self::assertSame($firstAgain->leaderId, $second->leaderId);
        self::assertSame([], $second->members, 'only the leader receives the members of the group');
        self::assertEqualsCanonicalizing(
            [$first->memberId, $second->memberId],
            array_keys($firstAgain->members),
            'the leader receives every member of the new generation, itself included, in no particular order'
        );

        // The leader assigns a share to both members, and each of them reads its own out of its SyncGroup answer
        $leaderShare   = $this->sync($firstStream, $groupId, $firstAgain->memberId, $firstAgain->generationId, [
            $firstAgain->memberId => 'share of the leader',
            $second->memberId     => 'share of the follower',
        ]);
        $followerShare = $this->sync($secondStream, $groupId, $second->memberId, $second->generationId, []);

        self::assertSame('share of the leader', $leaderShare->memberAssignment);
        self::assertSame('share of the follower', $followerShare->memberAssignment);

        // Both members are alive in the stable group, and the group rebalances again when the follower leaves
        self::assertSame(
            KafkaException::NO_ERROR,
            $this->heartbeat($firstStream, $groupId, $firstAgain->memberId, $firstAgain->generationId)->errorCode
        );
        $this->leave($secondStream, $groupId, $second->memberId);

        self::assertSame(
            KafkaException::REBALANCE_IN_PROGRESS,
            $this->heartbeatUntil($firstStream, $groupId, $firstAgain, KafkaException::REBALANCE_IN_PROGRESS),
            'the leave of a member starts a rebalance right away, without waiting for its session timeout'
        );

        $this->leave($firstStream, $groupId, $firstAgain->memberId);
    }

    public function testASessionTimeoutOutsideTheRangeOfTheBrokerIsRefused(): void
    {
        $groupId = self::uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);

        // 1 ms is below group.min.session.timeout.ms of the broker, which is 1000 on this container
        new JoinGroupRequest(
            $groupId,
            1,
            self::REBALANCE_TIMEOUT_MS,
            JoinGroupRequest::DEFAULT_MEMBER_ID,
            self::PROTOCOL_TYPE,
            [self::PROTOCOL_NAME => 'metadata'],
            $this->clientId(),
            301
        )->writeTo($stream);
        $response = JoinGroupResponse::unpack($stream);

        self::assertSame(KafkaException::INVALID_SESSION_TIMEOUT, $response->errorCode);
        self::assertSame(
            0,
            $response->generationId,
            'GroupCoordinator.joinError builds an error answer with the generation 0, not with -1'
        );
        self::assertSame('', $response->groupProtocol);
        self::assertSame('', $response->leaderId);
        self::assertSame([], $response->members);
    }

    /**
     * The rebalance timeout of version 1 is not validated at all - only the session timeout is
     *
     * `group.max.session.timeout.ms` of the container is 60000, and `GroupCoordinator.handleJoinGroup` @ 0.10.2.2
     * checks nothing but the session timeout against it: a rebalance timeout far above that bound is accepted, and
     * so is a rebalance timeout of 0.
     */
    public function testTheRebalanceTimeoutOfVersionOneIsNotBoundedByTheBroker(): void
    {
        $groupId = self::uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);

        foreach ([300000, 0] as $index => $rebalanceTimeoutMs) {
            new JoinGroupRequest(
                $groupId,
                self::SESSION_TIMEOUT_MS,
                $rebalanceTimeoutMs,
                JoinGroupRequest::DEFAULT_MEMBER_ID,
                self::PROTOCOL_TYPE,
                [self::PROTOCOL_NAME => 'metadata'],
                $this->clientId(),
                310 + $index
            )->writeTo($stream);
            $response = JoinGroupResponse::unpack($stream);

            self::assertSame(
                KafkaException::NO_ERROR,
                $response->errorCode,
                "a rebalance timeout of {$rebalanceTimeoutMs} ms is accepted, however group.max.session.timeout.ms "
                . 'is configured'
            );
            $this->leave($stream, $groupId, $response->memberId);
        }
    }

    /**
     * The coordinator holds a JoinGroup for the REBALANCE timeout of the members, not for their session timeout
     *
     * Two connections, because a JoinGroup blocks the one it was sent on: the first member joins with a session
     * timeout far above its rebalance timeout and then goes silent, the second member joins and starts a rebalance
     * that the first one never rejoins. The answer of the second join is what measures the wait.
     */
    public function testTheCoordinatorWaitsTheRebalanceTimeoutForAMemberThatDoesNotRejoin(): void
    {
        $groupId       = self::uniqueGroupName();
        $sessionMs     = 30000;
        $rebalanceMs   = 3000;
        $firstStream   = $this->coordinatorStream($groupId);

        new JoinGroupRequest(
            $groupId,
            $sessionMs,
            $rebalanceMs,
            JoinGroupRequest::DEFAULT_MEMBER_ID,
            self::PROTOCOL_TYPE,
            [self::PROTOCOL_NAME => 'first'],
            $this->clientId(),
            320
        )->writeTo($firstStream);
        $first = JoinGroupResponse::unpack($firstStream);
        self::assertSame(KafkaException::NO_ERROR, $first->errorCode);
        $this->sync($firstStream, $groupId, $first->memberId, $first->generationId, [
            $first->memberId => 'everything',
        ]);

        // The first member now stops talking; the second one joins and waits for it to rejoin
        $secondStream = $this->newCoordinatorStream($groupId);
        $startedAt    = microtime(true);
        new JoinGroupRequest(
            $groupId,
            $sessionMs,
            $rebalanceMs,
            JoinGroupRequest::DEFAULT_MEMBER_ID,
            self::PROTOCOL_TYPE,
            [self::PROTOCOL_NAME => 'second'],
            $this->clientId(),
            321
        )->writeTo($secondStream);
        $second  = JoinGroupResponse::unpack($secondStream);
        $waitedS = microtime(true) - $startedAt;

        self::assertSame(KafkaException::NO_ERROR, $second->errorCode);
        self::assertSame(2, $second->generationId, 'the rebalance produced the next generation');
        self::assertGreaterThanOrEqual(
            $rebalanceMs / 1000 * 0.8,
            $waitedS,
            'the coordinator held the join until the rebalance timeout of the silent member had expired'
        );
        self::assertLessThan(
            $sessionMs / 1000,
            $waitedS,
            'and it did not wait for the session timeout, which is what a version 0 join would have cost'
        );

        $this->leave($secondStream, $groupId, $second->memberId);
    }

    public function testAMemberIdTheGroupDoesNotHaveIsRefusedByEveryApiOfTheProtocol(): void
    {
        $groupId = self::uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);
        $join    = $this->join($stream, $groupId, JoinGroupRequest::DEFAULT_MEMBER_ID, 'metadata');
        $this->sync($stream, $groupId, $join->memberId, $join->generationId, [$join->memberId => 'all']);

        new HeartbeatRequest($groupId, $join->generationId, 't3-not-a-member', $this->clientId(), 401)
            ->writeTo($stream);
        $heartbeat = HeartbeatResponse::unpack($stream);

        new SyncGroupRequest($groupId, $join->generationId, 't3-not-a-member', [], $this->clientId(), 402)
            ->writeTo($stream);
        $sync = SyncGroupResponse::unpack($stream);

        new LeaveGroupRequest($groupId, 't3-not-a-member', $this->clientId(), 403)->writeTo($stream);
        $leave = LeaveGroupResponse::unpack($stream);

        self::assertSame(KafkaException::UNKNOWN_MEMBER_ID, $heartbeat->errorCode);
        self::assertSame(KafkaException::UNKNOWN_MEMBER_ID, $sync->errorCode);
        self::assertSame('', $sync->memberAssignment, 'an error answer carries an empty assignment');
        self::assertSame(KafkaException::UNKNOWN_MEMBER_ID, $leave->errorCode);

        // A member that names a group the coordinator has never seen is refused the same way
        new HeartbeatRequest(self::uniqueGroupName(), 1, 't3-not-a-member', $this->clientId(), 404)
            ->writeTo($stream);

        self::assertSame(KafkaException::UNKNOWN_MEMBER_ID, HeartbeatResponse::unpack($stream)->errorCode);

        $this->leave($stream, $groupId, $join->memberId);
    }

    public function testAGenerationThatIsOverIsRefusedByHeartbeatAndSyncGroup(): void
    {
        $groupId = self::uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);
        $join    = $this->join($stream, $groupId, JoinGroupRequest::DEFAULT_MEMBER_ID, 'metadata');
        $this->sync($stream, $groupId, $join->memberId, $join->generationId, [$join->memberId => 'all']);

        new HeartbeatRequest($groupId, $join->generationId + 1, $join->memberId, $this->clientId(), 501)
            ->writeTo($stream);
        $heartbeat = HeartbeatResponse::unpack($stream);

        new SyncGroupRequest($groupId, $join->generationId + 1, $join->memberId, [], $this->clientId(), 502)
            ->writeTo($stream);
        $sync = SyncGroupResponse::unpack($stream);

        self::assertSame(KafkaException::ILLEGAL_GENERATION, $heartbeat->errorCode);
        self::assertSame(KafkaException::ILLEGAL_GENERATION, $sync->errorCode);

        $this->leave($stream, $groupId, $join->memberId);
    }

    /**
     * The same cycle through the client, which turns every error code into its exception
     */
    public function testClientDrivesTheWholeCycleOfAGeneration(): void
    {
        $groupId     = self::uniqueGroupName();
        $client      = new Client($this->cluster(), $this->configuration());
        $coordinator = $client->getGroupCoordinator($groupId);

        $join = $client->joinGroup(
            $coordinator,
            $groupId,
            JoinGroupRequest::DEFAULT_MEMBER_ID,
            self::PROTOCOL_TYPE,
            [self::PROTOCOL_NAME => 'metadata of the client']
        );

        self::assertSame(1, $join->generationId);
        self::assertSame($join->memberId, $join->leaderId);

        $sync = $client->syncGroup($coordinator, $groupId, $join->memberId, $join->generationId, [
            $join->memberId => 'everything',
        ]);

        self::assertSame('everything', $sync->memberAssignment);

        $client->heartbeat($coordinator, $groupId, $join->memberId, $join->generationId);
        $client->leaveGroup($coordinator, $groupId, $join->memberId);

        // The member is gone, so the very next heartbeat of it is refused
        $this->expectException(UnknownMemberIdException::class);
        $client->heartbeat($coordinator, $groupId, $join->memberId, $join->generationId);
    }

    public function testClientReportsASessionTimeoutTheBrokerDoesNotAllow(): void
    {
        $groupId = self::uniqueGroupName();
        $client  = new Client(
            $this->cluster(),
            [ConsumerConfig::SESSION_TIMEOUT_MS => 1] + $this->configuration()
        );

        $this->expectException(InvalidSessionTimeoutException::class);
        $client->joinGroup(
            $client->getGroupCoordinator($groupId),
            $groupId,
            JoinGroupRequest::DEFAULT_MEMBER_ID,
            self::PROTOCOL_TYPE,
            [self::PROTOCOL_NAME => 'metadata']
        );
    }

    public function testClientReportsAGenerationThatIsOver(): void
    {
        $groupId     = self::uniqueGroupName();
        $client      = new Client($this->cluster(), $this->configuration());
        $coordinator = $client->getGroupCoordinator($groupId);
        $join        = $client->joinGroup(
            $coordinator,
            $groupId,
            JoinGroupRequest::DEFAULT_MEMBER_ID,
            self::PROTOCOL_TYPE,
            [self::PROTOCOL_NAME => 'metadata']
        );
        $client->syncGroup($coordinator, $groupId, $join->memberId, $join->generationId, [
            $join->memberId => 'everything',
        ]);

        try {
            $client->heartbeat($coordinator, $groupId, $join->memberId, $join->generationId + 1);
            self::fail('A heartbeat of a generation that is over has to be reported');
        } catch (IllegalGenerationException $exception) {
            self::assertSame($groupId, $exception->getContext()['groupId']);
        } finally {
            $client->leaveGroup($coordinator, $groupId, $join->memberId);
        }
    }

    /**
     * The payloads of the `consumer` protocol travel through these apis untouched.
     *
     * The membership apis only ever see opaque bytes; what those bytes mean is decided by the `protocol_type`, and
     * this is the composition that the consumer of the next ticket builds on: a {@see Subscription} as the metadata
     * of the member and a {@see MemberAssignment} as its share of the generation.
     */
    public function testTheConsumerProtocolPayloadsSurviveTheRoundTripThroughTheseApis(): void
    {
        $groupId      = self::uniqueGroupName();
        $topic        = 't3-consumer-protocol';
        $client       = new Client($this->cluster(), $this->configuration());
        $coordinator  = $client->getGroupCoordinator($groupId);
        $subscription = new Subscription([$topic]);

        $join = $client->joinGroup(
            $coordinator,
            $groupId,
            JoinGroupRequest::DEFAULT_MEMBER_ID,
            self::PROTOCOL_TYPE,
            [self::PROTOCOL_NAME => $subscription->pack()]
        );

        self::assertEquals(
            $subscription,
            Subscription::unpack($join->members[$join->memberId]->metadata),
            'the leader reads the subscription of every member back out of its JoinGroup answer'
        );

        $assignment = new MemberAssignment([$topic => [0, 1, 2]]);
        $sync       = $client->syncGroup($coordinator, $groupId, $join->memberId, $join->generationId, [
            $join->memberId => $assignment->pack(),
        ]);

        self::assertEquals($assignment, MemberAssignment::unpack($sync->memberAssignment));
        self::assertSame([$topic => [0, 1, 2]], MemberAssignment::unpack($sync->memberAssignment)->partitions());

        $client->leaveGroup($coordinator, $groupId, $join->memberId);
    }

    /**
     * Sends a JoinGroup request and asserts that the coordinator accepted it
     */
    private function join(Stream $stream, string $groupId, string $memberId, string $metadata): JoinGroupResponse
    {
        new JoinGroupRequest(
            $groupId,
            self::SESSION_TIMEOUT_MS,
            self::REBALANCE_TIMEOUT_MS,
            $memberId,
            self::PROTOCOL_TYPE,
            [self::PROTOCOL_NAME => $metadata],
            $this->clientId(),
            101
        )->writeTo($stream);

        $response = JoinGroupResponse::unpack($stream);
        self::assertSame(KafkaException::NO_ERROR, $response->errorCode, 'The broker refused the join');

        return $response;
    }

    /**
     * Sends a SyncGroup request and asserts that the coordinator accepted it
     *
     * @param array<string, string> $assignments Assignment of every member, empty for a member that is not the leader
     */
    private function sync(
        Stream $stream,
        string $groupId,
        string $memberId,
        int $generationId,
        array $assignments
    ): SyncGroupResponse {
        new SyncGroupRequest($groupId, $generationId, $memberId, $assignments, $this->clientId(), 102)
            ->writeTo($stream);

        $response = SyncGroupResponse::unpack($stream);
        self::assertSame(KafkaException::NO_ERROR, $response->errorCode, 'The broker refused the sync');

        return $response;
    }

    /**
     * Sends one Heartbeat request and returns the answer, whatever it says
     */
    private function heartbeat(
        Stream $stream,
        string $groupId,
        string $memberId,
        int $generationId
    ): HeartbeatResponse {
        new HeartbeatRequest($groupId, $generationId, $memberId, $this->clientId(), 103)->writeTo($stream);

        return HeartbeatResponse::unpack($stream);
    }

    /**
     * Heartbeats until the coordinator answers the expected error code, or until the rebalance timeout elapses
     */
    private function heartbeatUntil(Stream $stream, string $groupId, JoinGroupResponse $member, int $errorCode): int
    {
        $deadline = microtime(true) + self::REBALANCE_TIMEOUT;

        do {
            $answer = $this->heartbeat($stream, $groupId, $member->memberId, $member->generationId)->errorCode;
            if ($answer === $errorCode) {
                return $answer;
            }
            usleep(200000);
        } while (microtime(true) < $deadline);

        return $answer;
    }

    /**
     * Sends a LeaveGroup request and asserts that the coordinator removed the member
     */
    private function leave(Stream $stream, string $groupId, string $memberId): void
    {
        new LeaveGroupRequest($groupId, $memberId, $this->clientId(), 104)->writeTo($stream);

        self::assertSame(
            KafkaException::NO_ERROR,
            LeaveGroupResponse::unpack($stream)->errorCode,
            'The broker refused to remove the member'
        );
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
     * Opens a second, uncached connection to the coordinator, for a member that acts in parallel with another one
     */
    private function newCoordinatorStream(string $groupId): Stream
    {
        $configuration = $this->configuration();
        $coordinator   = new CoordinatorLookup($this->cluster(), $configuration)->findCoordinator($groupId);

        // Not through ConnectionFactory, which would hand out the connection of the first member again
        return new SocketStream("tcp://{$coordinator->host}:{$coordinator->port}", $configuration, 5.0);
    }

    /**
     * Returns the cluster of the configured bootstrap servers
     */
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

    /**
     * Client id of this test class, which the coordinator uses as the prefix of every member id it assigns
     */
    private function clientId(): string
    {
        return 'kafka-client-t3';
    }

    /**
     * Builds a consumer group name that is unique for this test run
     */
    private static function uniqueGroupName(): string
    {
        return 't3-group-' . bin2hex(random_bytes(6));
    }
}
