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
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\CoordinatorLookup;
use Protocol\Kafka\Common\Errors\IllegalGenerationException;
use Protocol\Kafka\Common\Errors\InvalidSessionTimeoutException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\MemberIdRequiredException;
use Protocol\Kafka\Common\Errors\UnknownMemberIdException;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\Internals\ConsumerCoordinator;
use Protocol\Kafka\Consumer\MemberAssignment;
use Protocol\Kafka\Consumer\RangeAssignor;
use Protocol\Kafka\Consumer\Subscription;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Data\DescribeGroupResponseMetadata;
use Protocol\Kafka\Protocol\Data\JoinGroupRequestProtocol;
use Protocol\Kafka\Protocol\Data\JoinGroupResponseMember;
use Protocol\Kafka\Protocol\Data\SyncGroupRequestMember;
use Protocol\Kafka\Protocol\Request\HeartbeatRequest;
use Protocol\Kafka\Protocol\Request\HeartbeatResponse;
use Protocol\Kafka\Protocol\Request\JoinGroupRequest;
use Protocol\Kafka\Protocol\Request\JoinGroupRequestV3;
use Protocol\Kafka\Protocol\Request\JoinGroupResponse;
use Protocol\Kafka\Protocol\Request\JoinGroupResponseV3;
use Protocol\Kafka\Protocol\Request\LeaveGroupRequest;
use Protocol\Kafka\Protocol\Request\LeaveGroupResponse;
use Protocol\Kafka\Protocol\Request\SyncGroupRequest;
use Protocol\Kafka\Protocol\Request\SyncGroupResponse;

/**
 * Verifies the four apis of the group membership protocol against a real Kafka 2.8.2 broker.
 *
 * Kafka 0.9 moved the coordination of a group out of ZooKeeper into the coordinator broker, and these tests drive
 * the whole cycle of a generation over raw requests: a member joins, the leader publishes an assignment, the
 * members heartbeat, a second member joins and forces a rebalance, and both leave. The payloads - the metadata of a
 * member and its assignment - are opaque byte arrays to these **apis**, so arbitrary bytes are used for them here.
 *
 * They are not opaque to a **2.x coordinator**, though, and that is why the group protocol type of this class is
 * {@see self::PROTOCOL_TYPE} and not `consumer`: from Kafka 2.3 on `GroupMetadata.computeSubscribedTopics()` parses
 * the metadata of every member of a group whose protocol type *is* `consumer` as a `ConsumerProtocolSubscription`,
 * and bytes it cannot parse leave the group in `PreparingRebalance` for good - see "A `consumer` group whose member
 * metadata is not a Subscription never rebalances" in the "Broker quirks and observations" section of the protocol
 * document. {@see self::testTheConsumerProtocolPayloadsSurviveTheRoundTripThroughTheseApis} is the one test here
 * that uses the `consumer` protocol type, and it sends the real structures of that type.
 *
 * Every request goes out with the version this line sends, which Kafka 2.0 raised by one for all four apis without
 * changing a field (KIP-219): JoinGroup v3, SyncGroup v2, Heartbeat v2 and LeaveGroup v2.
 *
 * @see docs/protocol/2.8.md, sections "Group membership protocol (keys 11 to 14)", "JoinGroup API (key 11, v0 to v7)",
 *      "SyncGroup API (key 14, v0 to v5)", "Heartbeat API (key 12, v0 to v4)" and "LeaveGroup API (key 13, v0 to v4)"
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
     * Protocol type of the groups of this class, which is deliberately **not** `consumer`
     *
     * A group whose protocol type is `consumer` has its member metadata parsed by the coordinator itself from
     * Kafka 2.3 on; a protocol type of its own keeps the arbitrary bytes of these tests genuinely opaque, which is
     * what the four apis promise. {@see self::CONSUMER_PROTOCOL_TYPE} is used where the real structures are sent.
     */
    private const string PROTOCOL_TYPE = 't3-membership';

    /**
     * The protocol type of a real consumer group, the only one Kafka itself knows about
     */
    private const string CONSUMER_PROTOCOL_TYPE = 'consumer';

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

    /**
     * KIP-394 (Kafka 2.2, JoinGroup v4): a first join is refused once, with the member id it is to use
     *
     * Up to version 3 a join with an empty member id added the member to the group at once. From version 4 on the
     * coordinator answers **79** (`MemberIdRequired`) with the id it generated and adds nothing; the client sends
     * the same request again with that id and is then treated like any rejoining member.
     */
    public function testAFirstJoinOfVersionFourIsRefusedWithTheMemberIdTheCoordinatorAssigns(): void
    {
        $admin = new AdminClient($this->cluster(), $this->configuration());

        // The coordinator's `cleanupGroupMetadata` runs every `offsets.retention.check.interval.ms` (ten minutes
        // by default) and removes an Empty group that holds no offset - which is exactly what a group with nothing
        // but a pending member is. It is a rare neighbour of this exchange on the shared container, and the only
        // answer it can produce is the `Dead` of a group that is gone, so the exchange is simply done again
        for ($attempt = 1; ; ++$attempt) {
            $groupId = self::uniqueGroupName();
            $stream  = $this->coordinatorStream($groupId);

            $refused = $this->rawJoin($stream, $groupId, JoinGroupRequest::DEFAULT_MEMBER_ID, 'first join', 601);

            self::assertSame(KafkaException::MEMBER_ID_REQUIRED, $refused->errorCode);
            self::assertSame(-1, $refused->generationId, 'an error answer of a 2.x coordinator carries -1');
            self::assertNull(
                $refused->groupProtocol,
                'KIP-559 made the protocol name nullable in a version 7, where a version 6 carries the empty string'
            );
            self::assertNull($refused->protocolType, 'and the protocol type next to it is null for the same answer');
            self::assertSame('', $refused->leaderId);
            self::assertSame([], $refused->members);
            self::assertMatchesRegularExpression(
                '/^' . preg_quote($this->clientId(), '/') . '-[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/',
                $refused->memberId,
                'the answer carries the member id the coordinator generated, as "<client id>-<uuid>"'
            );

            // The group exists, but the pending member is not a member of it: it holds no rebalance up
            $description = $admin->describeGroup($groupId);

            if ($description->state !== DescribeGroupResponseMetadata::STATE_DEAD || $attempt === 3) {
                break;
            }
        }

        self::assertSame(DescribeGroupResponseMetadata::STATE_EMPTY, $description->state);
        self::assertSame([], $description->members, 'a pending member does not appear in DescribeGroups');

        // The same request again, with the id it was given, is accepted
        $joined = $this->rawJoin($stream, $groupId, $refused->memberId, 'first join', 602);

        self::assertSame(KafkaException::NO_ERROR, $joined->errorCode);
        self::assertSame(1, $joined->generationId, 'the first generation of the group');
        self::assertSame($refused->memberId, $joined->memberId, 'and the member kept the id it was handed');
        self::assertSame($joined->memberId, $joined->leaderId);
        self::assertSame([$joined->memberId], array_keys($joined->members));

        $this->leave($stream, $groupId, $joined->memberId);
    }

    /**
     * The version below it still adds an unidentified member to the group at once
     */
    public function testAFirstJoinOfVersionThreeIsAddedToTheGroupRightAway(): void
    {
        $groupId = self::uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);

        new JoinGroupRequestV3(
            $groupId,
            self::SESSION_TIMEOUT_MS,
            self::REBALANCE_TIMEOUT_MS,
            JoinGroupRequest::DEFAULT_MEMBER_ID,
            self::PROTOCOL_TYPE,
            [self::PROTOCOL_NAME => 'a version 3 first join'],
            $this->clientId(),
            603
        )->writeTo($stream);
        $joined = JoinGroupResponseV3::unpack($stream);

        self::assertSame(KafkaException::NO_ERROR, $joined->errorCode, 'no 79 below version 4');
        self::assertSame(1, $joined->generationId);
        self::assertNotSame('', $joined->memberId, 'the coordinator assigned an id and added the member');
        self::assertSame($joined->memberId, $joined->leaderId);

        $this->leave($stream, $groupId, $joined->memberId);
    }

    /**
     * The client answers the 79 itself, so a caller of the consumer never sees it
     */
    public function testTheClientCoordinatorJoinsWithTheAssignedMemberIdWithoutAnyHelp(): void
    {
        $groupId     = self::uniqueGroupName();
        $client      = new Client($this->cluster(), $this->configuration());
        $coordinator = new ConsumerCoordinator($client, $groupId, new RangeAssignor(), 3000);

        $assignment = $coordinator->ensureActiveGroup(['t3-membership-394'], static fn(array $topics): array => []);

        self::assertSame([], $assignment, 'no partitions, because the topic of the subscription has none here');
        self::assertTrue($coordinator->isMember(), 'the coordinator joined through the 79 of KIP-394');
        self::assertSame(1, $coordinator->getGenerationId());
        self::assertStringStartsWith($this->clientId(), $coordinator->getMemberId());

        $coordinator->leaveGroup();
    }

    /**
     * The low-level client reports the 79 with the assigned id in the context, and does NOT rejoin by itself
     */
    public function testTheLowLevelClientReportsTheAssignedMemberIdInTheExceptionContext(): void
    {
        $groupId     = self::uniqueGroupName();
        $client      = new Client($this->cluster(), $this->configuration());
        $coordinator = $client->getGroupCoordinator($groupId);

        try {
            $client->joinGroup(
                $coordinator,
                $groupId,
                JoinGroupRequest::DEFAULT_MEMBER_ID,
                self::PROTOCOL_TYPE,
                [self::PROTOCOL_NAME => 'metadata of the client']
            );
            self::fail('A first join of version 4 has to be refused with the error code 79');
        } catch (MemberIdRequiredException $exception) {
            $assigned = $exception->getContext()['assignedMemberId'] ?? null;

            self::assertIsString($assigned);
            self::assertStringStartsWith($this->clientId(), $assigned);
            self::assertSame('', $exception->getContext()['memberId'], 'the id that was SENT was the empty one');

            $joined = $client->joinGroup(
                $coordinator,
                $groupId,
                $assigned,
                self::PROTOCOL_TYPE,
                [self::PROTOCOL_NAME => 'metadata of the client']
            );

            self::assertSame(1, $joined->generationId);
            self::assertSame($assigned, $joined->memberId);
            $client->leaveGroup($coordinator, $groupId, $joined->memberId);
        }
    }

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
     * coordinator holds that answer back for exactly that long - which is the blocking behaviour of the api. Its
     * **first** join is a different matter: the 79 of KIP-394 is answered at once and starts no rebalance at all,
     * so it is sent and read before the blocking one goes out.
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
        $refused      = $this->rawJoin($secondStream, $groupId, JoinGroupRequest::DEFAULT_MEMBER_ID, 'second', 201);

        self::assertSame(
            KafkaException::MEMBER_ID_REQUIRED,
            $refused->errorCode,
            'the first join of a member is refused at once and leaves the stable group alone'
        );
        self::assertSame(
            KafkaException::NO_ERROR,
            $this->heartbeat($firstStream, $groupId, $first->memberId, $first->generationId)->errorCode,
            'a pending member starts no rebalance: the group is still stable for the first member'
        );

        // The real join of the second member, which the coordinator holds until the first one has rejoined
        new JoinGroupRequest(
            $groupId,
            self::SESSION_TIMEOUT_MS,
            self::REBALANCE_TIMEOUT_MS,
            $refused->memberId,
            self::PROTOCOL_TYPE,
            [self::PROTOCOL_NAME => 'second'],
            $this->clientId(),
            202
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
            -1,
            $response->generationId,
            'a 2.x error answer carries the UNKNOWN_GENERATION_ID -1, where a 0.11 or 1.1 broker sent 0'
        );
        self::assertNull($response->groupProtocol, 'null from version 7 on, the empty string below it (KIP-559)');
        self::assertSame('', $response->leaderId);
        self::assertSame([], $response->members);
    }

    /**
     * The rebalance timeout is not validated at all - only the session timeout is - but it is no longer harmless
     *
     * `group.max.session.timeout.ms` of the container is 60000, and `GroupCoordinator.handleJoinGroup` @ 2.8.2
     * checks nothing but the session timeout against it: a rebalance timeout far above that bound is accepted, and
     * so is a rebalance timeout of 0. What Kafka 2.5 added (KAFKA-9752, the pending-sync expiration of KIP-345) is
     * a **second** use of the field: `onCompleteJoin` schedules a `DelayedSync` with exactly this timeout and
     * `onExpirePendingSync` drops every member of the fresh generation that has not sent its SyncGroup by then.
     * With a rebalance timeout of 0 that expiry fires the moment the generation is formed, so the member is gone
     * before it can do anything with the answer it just received - which is what the LeaveGroup below measures.
     */
    public function testTheRebalanceTimeoutIsNotBoundedByTheBrokerButIsAlsoTheSyncDeadline(): void
    {
        $groupId = self::uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);

        $generous = $this->joinWithRebalanceTimeout($stream, $groupId, 300000, 310);

        self::assertSame(
            KafkaException::NO_ERROR,
            $generous->errorCode,
            'a rebalance timeout of 300000 ms is accepted, however group.max.session.timeout.ms is configured'
        );
        $this->leave($stream, $groupId, $generous->memberId);

        // The same join with a rebalance timeout of 0: accepted as well, and the member is dropped right away
        $immediate = self::uniqueGroupName();
        $zero      = $this->joinWithRebalanceTimeout($stream, $immediate, 0, 311);

        self::assertSame(KafkaException::NO_ERROR, $zero->errorCode, 'a rebalance timeout of 0 is accepted too');
        self::assertSame(1, $zero->generationId);

        new LeaveGroupRequest($immediate, $zero->memberId, $this->clientId(), 312)->writeTo($stream);
        $left = LeaveGroupResponse::unpack($stream);

        // From version 3 (KIP-345) the error of a member travels in its entry of the batch answer, and the
        // top-level code is about the request alone
        self::assertSame(KafkaException::NO_ERROR, $left->errorCode);
        self::assertSame(
            KafkaException::UNKNOWN_MEMBER_ID,
            $left->members[0]->errorCode,
            'the pending-sync expiration of the rebalance timeout 0 removed the member before it could sync'
        );
    }

    /**
     * The metadata of a group whose protocol type is not `consumer` is never parsed by the coordinator
     *
     * This is the counterpart of the quirk that the class docblock names: a 2.x coordinator parses the member
     * metadata of a `consumer` group and wedges the group when it cannot, but it only does that for that one
     * protocol type - `GroupMetadata.computeSubscribedTopics()` @ 2.8.2 matches
     * `Some(ConsumerProtocol.PROTOCOL_TYPE)` and answers `None` for everything else. Bytes that are not a
     * `ConsumerProtocolSubscription` - here a NUL byte, an invalid UTF-8 byte and a length prefix that promises far
     * more data than follows - therefore travel through JoinGroup and SyncGroup untouched.
     */
    public function testTheCoordinatorNeverParsesTheMetadataOfANonConsumerProtocolType(): void
    {
        $groupId    = self::uniqueGroupName();
        $stream     = $this->coordinatorStream($groupId);
        $metadata   = "\x00\xff\x7f\xff\xff\xffnot a subscription";
        $assignment = "\xff\xff\xff\xff\x00not an assignment";

        $join = $this->join($stream, $groupId, JoinGroupRequest::DEFAULT_MEMBER_ID, $metadata);

        self::assertSame(1, $join->generationId, 'the group rebalanced although the metadata is not parseable');
        self::assertSame(
            [$join->memberId => $metadata],
            array_map(static fn(JoinGroupResponseMember $member): string => $member->metadata, $join->members)
        );

        $sync = $this->sync($stream, $groupId, $join->memberId, $join->generationId, [
            $join->memberId => $assignment,
        ]);

        self::assertSame($assignment, $sync->memberAssignment);
        self::assertSame(
            KafkaException::NO_ERROR,
            $this->heartbeat($stream, $groupId, $join->memberId, $join->generationId)->errorCode,
            'the group is stable, which a `consumer` group with these bytes would never become'
        );

        $this->leave($stream, $groupId, $join->memberId);
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

        $first = $this->joinWithRebalanceTimeout($firstStream, $groupId, $rebalanceMs, 320, 'first', $sessionMs);
        self::assertSame(KafkaException::NO_ERROR, $first->errorCode);
        $this->sync($firstStream, $groupId, $first->memberId, $first->generationId, [
            $first->memberId => 'everything',
        ]);

        // The first member now stops talking; the second one joins and waits for it to rejoin. The 79 of KIP-394
        // is answered before the wait starts, so the id is fetched first and the clock starts with the real join
        $secondStream = $this->newCoordinatorStream($groupId);
        $refused      = $this->rawJoinWith($secondStream, $groupId, '', 'second', $sessionMs, $rebalanceMs, 321);
        self::assertSame(KafkaException::MEMBER_ID_REQUIRED, $refused->errorCode);

        $startedAt = microtime(true);
        $second    = $this->rawJoinWith(
            $secondStream,
            $groupId,
            $refused->memberId,
            'second',
            $sessionMs,
            $rebalanceMs,
            322
        );
        $waitedS   = microtime(true) - $startedAt;

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

        new SyncGroupRequest(
            $groupId,
            $join->generationId,
            't3-not-a-member',
            [],
            $this->clientId(),
            402,
            null,
            self::PROTOCOL_TYPE,
            self::PROTOCOL_NAME
        )->writeTo($stream);
        $sync = SyncGroupResponse::unpack($stream);

        new LeaveGroupRequest($groupId, 't3-not-a-member', $this->clientId(), 403)->writeTo($stream);
        $leave = LeaveGroupResponse::unpack($stream);

        self::assertSame(KafkaException::UNKNOWN_MEMBER_ID, $heartbeat->errorCode);
        self::assertSame(KafkaException::UNKNOWN_MEMBER_ID, $sync->errorCode);
        self::assertSame('', $sync->memberAssignment, 'an error answer carries an empty assignment');
        self::assertSame(
            KafkaException::NO_ERROR,
            $leave->errorCode,
            'the batch of version 3 (KIP-345) answers 0 at the top whatever became of its members'
        );
        self::assertSame(
            KafkaException::UNKNOWN_MEMBER_ID,
            $leave->members[0]->errorCode,
            'and the member id the group does not have is refused in its own entry'
        );
        self::assertSame('t3-not-a-member', $leave->members[0]->memberId, 'the entry echoes what was sent');

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

        new SyncGroupRequest(
            $groupId,
            $join->generationId + 1,
            $join->memberId,
            [],
            $this->clientId(),
            502,
            null,
            self::PROTOCOL_TYPE,
            self::PROTOCOL_NAME
        )->writeTo($stream);
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

        $join = $this->joinThroughClient($client, $coordinator, $groupId, [
            self::PROTOCOL_NAME => 'metadata of the client',
        ]);

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
        $join        = $this->joinThroughClient($client, $coordinator, $groupId, [
            self::PROTOCOL_NAME => 'metadata',
        ]);
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

        $join = $this->joinThroughClient(
            $client,
            $coordinator,
            $groupId,
            [self::PROTOCOL_NAME => $subscription->pack()],
            self::CONSUMER_PROTOCOL_TYPE
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
     * Joins through the low-level client, answering the 79 of KIP-394 with the member id it carries
     *
     * `Client::joinGroup()` is the api and not the state machine: it reports the 79 and leaves the second join to
     * its caller, which is what `Consumer\Internals\ConsumerCoordinator` does in the client itself.
     *
     * @param array<string, string> $protocols Metadata of every offered protocol, by protocol name
     */
    private function joinThroughClient(
        Client $client,
        Node $coordinator,
        string $groupId,
        array $protocols,
        string $protocolType = self::PROTOCOL_TYPE
    ): JoinGroupResponse {
        try {
            return $client->joinGroup(
                $coordinator,
                $groupId,
                JoinGroupRequest::DEFAULT_MEMBER_ID,
                $protocolType,
                $protocols
            );
        } catch (MemberIdRequiredException $exception) {
            return $client->joinGroup(
                $coordinator,
                $groupId,
                (string) $exception->getContext()['assignedMemberId'],
                $protocolType,
                $protocols
            );
        }
    }

    /**
     * Sends a JoinGroup request and asserts that the coordinator accepted it
     *
     * A first join - one with an empty member id - is refused once by the version 4 this client sends (KIP-394)
     * and has to be repeated with the member id the coordinator assigned, which is what this helper does.
     */
    private function join(Stream $stream, string $groupId, string $memberId, string $metadata): JoinGroupResponse
    {
        $response = $this->rawJoin($stream, $groupId, $memberId, $metadata, 101);
        if ($response->errorCode === KafkaException::MEMBER_ID_REQUIRED) {
            $response = $this->rawJoin($stream, $groupId, $response->memberId, $metadata, 102);
        }

        self::assertSame(KafkaException::NO_ERROR, $response->errorCode, 'The broker refused the join');

        return $response;
    }

    /**
     * Joins with a rebalance timeout of its own, answering the 79 of KIP-394 with the id it carries
     */
    private function joinWithRebalanceTimeout(
        Stream $stream,
        string $groupId,
        int $rebalanceTimeoutMs,
        int $correlationId,
        string $metadata = 'metadata',
        int $sessionTimeoutMs = self::SESSION_TIMEOUT_MS
    ): JoinGroupResponse {
        $response = $this->rawJoinWith(
            $stream,
            $groupId,
            JoinGroupRequest::DEFAULT_MEMBER_ID,
            $metadata,
            $sessionTimeoutMs,
            $rebalanceTimeoutMs,
            $correlationId
        );
        if ($response->errorCode !== KafkaException::MEMBER_ID_REQUIRED) {
            return $response;
        }

        return $this->rawJoinWith(
            $stream,
            $groupId,
            $response->memberId,
            $metadata,
            $sessionTimeoutMs,
            $rebalanceTimeoutMs,
            $correlationId + 1000
        );
    }

    /**
     * Sends one JoinGroup request with timeouts of its own and returns the answer, whatever it says
     */
    private function rawJoinWith(
        Stream $stream,
        string $groupId,
        string $memberId,
        string $metadata,
        int $sessionTimeoutMs,
        int $rebalanceTimeoutMs,
        int $correlationId
    ): JoinGroupResponse {
        new JoinGroupRequest(
            $groupId,
            $sessionTimeoutMs,
            $rebalanceTimeoutMs,
            $memberId,
            self::PROTOCOL_TYPE,
            [self::PROTOCOL_NAME => $metadata],
            $this->clientId(),
            $correlationId
        )->writeTo($stream);

        return JoinGroupResponse::unpack($stream);
    }

    /**
     * Sends one JoinGroup request of the version this client speaks and returns the answer, whatever it says
     */
    private function rawJoin(
        Stream $stream,
        string $groupId,
        string $memberId,
        string $metadata,
        int $correlationId
    ): JoinGroupResponse {
        new JoinGroupRequest(
            $groupId,
            self::SESSION_TIMEOUT_MS,
            self::REBALANCE_TIMEOUT_MS,
            $memberId,
            self::PROTOCOL_TYPE,
            [self::PROTOCOL_NAME => $metadata],
            $this->clientId(),
            $correlationId
        )->writeTo($stream);

        return JoinGroupResponse::unpack($stream);
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
        new SyncGroupRequest(
            $groupId,
            $generationId,
            $memberId,
            $assignments,
            $this->clientId(),
            102,
            null,
            self::PROTOCOL_TYPE,
            self::PROTOCOL_NAME
        )->writeTo($stream);

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
        $left = LeaveGroupResponse::unpack($stream);

        self::assertSame(KafkaException::NO_ERROR, $left->errorCode, 'The broker refused the request');
        self::assertSame(
            KafkaException::NO_ERROR,
            $left->members[0]->errorCode,
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
