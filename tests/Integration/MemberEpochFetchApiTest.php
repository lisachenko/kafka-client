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
use Protocol\Kafka\Common\Errors\StaleMemberEpochException;
use Protocol\Kafka\Common\Errors\UnknownMemberIdException;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\Internals\ConsumerGroupHeartbeatCoordinator;
use Protocol\Kafka\Consumer\MemberAssignment;
use Protocol\Kafka\Consumer\Subscription;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Data\OffsetFetchRequestGroup;
use Protocol\Kafka\Protocol\Data\OffsetFetchRequestGroupV8;
use Protocol\Kafka\Protocol\Request\JoinGroupRequest;
use Protocol\Kafka\Protocol\Request\JoinGroupResponse;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequest;
use Protocol\Kafka\Protocol\Request\OffsetCommitResponse;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequest;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequestV8;
use Protocol\Kafka\Protocol\Request\OffsetFetchResponse;
use Protocol\Kafka\Protocol\Request\OffsetFetchResponseV8;
use Protocol\Kafka\Protocol\Request\SyncGroupRequest;
use Protocol\Kafka\Protocol\Request\SyncGroupResponse;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * What Kafka 3.7 added to the group apis, against the 3.9.2 KRaft node: the OffsetFetch **v9** of KIP-848.
 *
 * The version adds two fields to every entry of the `groups` array of version 8 - a nullable `member_id` and an
 * `member_epoch` - and nothing else to either half of the api: "Those are filled in and validated when the new
 * consumer protocol is used" (`OffsetFetchRequest.json` @ 3.7.2). What the number buys is the promise of the
 * answer, "can return STALE_MEMBER_EPOCH and UNKNOWN_MEMBER_ID errors when the new consumer group protocol is
 * used", and both codes are **group-level** codes of the entry they belong to.
 *
 * Every rule of that validation is driven here against the node: the classic group that ignores the two fields,
 * the KIP-848 group that answers the 113 and the 25 with them, the defaults that skip the check altogether, the
 * one combination the node answers with the -1 of an `UnknownServerError`, and the group that does not exist,
 * which is answered the 0 of every version below and never the 69 that OffsetCommit v9 has.
 *
 * The KIP-848 group is created with a **ConsumerGroupHeartbeat** (key 68, `Client::joinConsumerGroup()` of the
 * KIP-848 wave, with the member id {@see ConsumerGroupHeartbeatCoordinator::newMemberId()} generates), exactly as
 * {@see MemberEpochCommitApiTest} of the 3.6 wave creates one.
 *
 * Every group and topic of this class carries the `t3-37-` prefix of the Kafka 3.7 wave and is removed again in
 * {@see self::tearDownAfterClass()} - every KIP-848 member with the leave heartbeat of the epoch -1 first, because
 * a group that still holds a member is not deletable.
 *
 * @see docs/protocol/3.9.md, section "The member id and epoch of KIP-848 (v9)"
 * @see docs/protocol/3.9.md, section "OffsetFetch API (key 9, v0 to v9)"
 */
#[CoversClass(Client::class)]
#[CoversClass(OffsetFetchRequest::class)]
#[CoversClass(OffsetFetchRequestV8::class)]
#[CoversClass(OffsetFetchRequestGroup::class)]
#[CoversClass(OffsetFetchRequestGroupV8::class)]
#[CoversClass(OffsetFetchResponse::class)]
#[CoversClass(OffsetFetchResponseV8::class)]
final class MemberEpochFetchApiTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t3-37';

    private const string TOPIC_PREFIX = 't3-37-fetch';

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
     * The offset every group of this class commits before it reads it back
     */
    private const int COMMITTED_OFFSET = 21;

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
     * The read of every client of this line is the version 9 with the two new fields at their defaults
     */
    public function testTheClientReadsCommittedOffsetsWithTheVersionNineOfKip848(): void
    {
        $topic   = $this->topic();
        $groupId = $this->uniqueGroupName();
        $this->classicGroupWithACommittedOffset($groupId, $topic);

        $offsets = new Client($this->cluster(), $this->configuration())
            ->fetchGroupOffsets($this->coordinator($groupId), $groupId, [$topic => [0]]);

        self::assertSame(9, OffsetFetchRequest::VERSION, 'the version this client sends since Kafka 3.7');
        self::assertSame(9, OffsetFetchResponse::VERSION);
        self::assertSame([$topic => [0 => self::COMMITTED_OFFSET]], $offsets);

        $entry = $this->fetch(
            $this->coordinatorStream($groupId),
            OffsetFetchRequest::class,
            OffsetFetchResponse::class,
            $groupId,
            [$topic => [0]],
            null,
            OffsetFetchRequestGroup::NO_MEMBER_EPOCH,
            3700
        )->groupOf($groupId);

        self::assertSame(KafkaException::NO_ERROR, $entry->errorCode);
        self::assertSame(self::COMMITTED_OFFSET, $entry->topics[$topic]->partitions[0]->offset);
    }

    /**
     * A classic group takes the member id and the epoch of version 9 and looks at neither of them
     */
    public function testAClassicGroupIgnoresTheMemberIdAndTheEpochOfVersionNine(): void
    {
        $topic   = $this->topic();
        $groupId = $this->uniqueGroupName();
        $this->classicGroupWithACommittedOffset($groupId, $topic);

        $offsets = new Client($this->cluster(), $this->configuration())->fetchGroupOffsetsAsMember(
            $this->coordinator($groupId),
            $groupId,
            [$topic => [0]],
            'a-member-this-group-never-had',
            7
        );

        self::assertSame(
            [$topic => [0 => self::COMMITTED_OFFSET]],
            $offsets,
            'ClassicGroup::validateOffsetFetch only refuses a group in the state Dead'
        );
    }

    /**
     * A member of a KIP-848 group reads its own offsets with its own member id and its current epoch
     */
    public function testAMemberOfAKip848GroupReadsItsOffsetsWithItsMemberEpoch(): void
    {
        $topic   = $this->topic();
        $groupId = $this->uniqueGroupName();
        [$memberId, $epoch] = $this->modernGroupWithACommittedOffset($groupId, $topic);

        $offsets = new Client($this->cluster(), $this->configuration())->fetchGroupOffsetsAsMember(
            $this->coordinator($groupId),
            $groupId,
            [$topic => [0]],
            $memberId,
            $epoch
        );

        self::assertSame([$topic => [0 => self::COMMITTED_OFFSET]], $offsets);

        // and the null topic array of "every partition of the group" is validated exactly the same way
        $all = new Client($this->cluster(), $this->configuration())->fetchGroupOffsetsAsMember(
            $this->coordinator($groupId),
            $groupId,
            null,
            $memberId,
            $epoch
        );

        self::assertSame([$topic => [0 => self::COMMITTED_OFFSET]], $all);
    }

    /**
     * An epoch that is not the one the coordinator holds is the 113, below and above it alike, and the -1 of the
     * default is one of them as soon as a member id stands next to it
     */
    public function testAnEpochThatIsNotTheCurrentOneIsTheOneHundredAndThirteen(): void
    {
        $topic   = $this->topic();
        $groupId = $this->uniqueGroupName();
        [$memberId, $epoch] = $this->modernGroupWithACommittedOffset($groupId, $topic);
        $stream  = $this->coordinatorStream($groupId);

        foreach ([$epoch - 1, $epoch + 5, OffsetFetchRequestGroup::NO_MEMBER_EPOCH] as $index => $stale) {
            $entry = $this->fetch(
                $stream,
                OffsetFetchRequest::class,
                OffsetFetchResponse::class,
                $groupId,
                [$topic => [0]],
                $memberId,
                $stale,
                3710 + $index
            )->groupOf($groupId);

            self::assertSame(
                KafkaException::STALE_MEMBER_EPOCH,
                $entry->errorCode,
                "the epoch {$stale} is not the {$epoch} the coordinator holds"
            );
            self::assertSame([], $entry->topics, 'a group-level error answers no topic at all');
        }
    }

    /**
     * And the client reports that code as its own exception
     */
    public function testTheClientReportsTheStaleMemberEpochAsItsOwnException(): void
    {
        $topic   = $this->topic();
        $groupId = $this->uniqueGroupName();
        [$memberId, $epoch] = $this->modernGroupWithACommittedOffset($groupId, $topic);

        $this->expectException(StaleMemberEpochException::class);

        new Client($this->cluster(), $this->configuration())->fetchGroupOffsetsAsMember(
            $this->coordinator($groupId),
            $groupId,
            [$topic => [0]],
            $memberId,
            $epoch - 1
        );
    }

    /**
     * A member id the KIP-848 group does not hold is the 25, and the empty string is a member id like any other
     */
    public function testAMemberIdTheKip848GroupDoesNotHoldIsTheTwentyFive(): void
    {
        $topic   = $this->topic();
        $groupId = $this->uniqueGroupName();
        [, $epoch] = $this->modernGroupWithACommittedOffset($groupId, $topic);
        $stream = $this->coordinatorStream($groupId);

        foreach (['not-a-member-of-this-group', ''] as $index => $unknown) {
            $entry = $this->fetch(
                $stream,
                OffsetFetchRequest::class,
                OffsetFetchResponse::class,
                $groupId,
                [$topic => [0]],
                $unknown,
                $epoch,
                3720 + $index
            )->groupOf($groupId);

            self::assertSame(KafkaException::UNKNOWN_MEMBER_ID, $entry->errorCode);
            self::assertSame([], $entry->topics);
        }

        $this->expectException(UnknownMemberIdException::class);

        new Client($this->cluster(), $this->configuration())->fetchGroupOffsetsAsMember(
            $this->coordinator($groupId),
            $groupId,
            [$topic => [0]],
            'not-a-member-of-this-group',
            $epoch
        );
    }

    /**
     * The defaults skip the member check for both kinds of group - and a null member id with an epoch that is NOT
     * negative is the one combination the node answers with the -1 of an `UnknownServerError`
     */
    public function testTheDefaultsSkipTheCheckAndANullMemberIdWithAnEpochIsTheUnknownServerError(): void
    {
        $topic   = $this->topic();
        $groupId = $this->uniqueGroupName();
        [, $epoch] = $this->modernGroupWithACommittedOffset($groupId, $topic);
        $stream = $this->coordinatorStream($groupId);

        $accepted = $this->fetch(
            $stream,
            OffsetFetchRequest::class,
            OffsetFetchResponse::class,
            $groupId,
            [$topic => [0]],
            null,
            OffsetFetchRequestGroup::NO_MEMBER_EPOCH,
            3730
        )->groupOf($groupId);

        self::assertSame(KafkaException::NO_ERROR, $accepted->errorCode, 'null and -1 is the admin client');
        self::assertSame(self::COMMITTED_OFFSET, $accepted->topics[$topic]->partitions[0]->offset);

        $broken = $this->fetch(
            $stream,
            OffsetFetchRequest::class,
            OffsetFetchResponse::class,
            $groupId,
            [$topic => [0]],
            null,
            $epoch,
            3731
        )->groupOf($groupId);

        self::assertSame(
            KafkaException::UNKNOWN,
            $broken->errorCode,
            'the member table of the coordinator dereferences the null key it is handed'
        );
        self::assertSame([], $broken->topics);

        // the connection is not stranded by it: the next request on it is answered normally
        $again = $this->fetch(
            $stream,
            OffsetFetchRequest::class,
            OffsetFetchResponse::class,
            $groupId,
            [$topic => [0]],
            null,
            OffsetFetchRequestGroup::NO_MEMBER_EPOCH,
            3732
        )->groupOf($groupId);

        self::assertSame(KafkaException::NO_ERROR, $again->errorCode);
    }

    /**
     * Reading is open where committing is closed: a member of a KIP-848 group may send a version 8 fetch
     */
    public function testAMemberOfAKip848GroupMayReadItsOffsetsWithTheVersionEight(): void
    {
        $topic   = $this->topic();
        $groupId = $this->uniqueGroupName();
        $this->modernGroupWithACommittedOffset($groupId, $topic);

        $entry = $this->fetch(
            $this->coordinatorStream($groupId),
            OffsetFetchRequestV8::class,
            OffsetFetchResponseV8::class,
            $groupId,
            [$topic => [0]],
            null,
            OffsetFetchRequestGroup::NO_MEMBER_EPOCH,
            3740
        )->groupOf($groupId);

        self::assertSame(
            KafkaException::NO_ERROR,
            $entry->errorCode,
            'validateOffsetFetch is never handed the api version, unlike validateOffsetCommit'
        );
        self::assertSame(self::COMMITTED_OFFSET, $entry->topics[$topic]->partitions[0]->offset);
    }

    /**
     * A group the coordinator does not know is the 0 of every version below, never the 69 of OffsetCommit v9
     */
    public function testAGroupThatDoesNotExistIsNotAnErrorAtEitherVersion(): void
    {
        $topic   = $this->topic();
        $groupId = $this->uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);

        foreach (
            [
                [OffsetFetchRequest::class, OffsetFetchResponse::class, 'a-member', 4, 3750],
                [OffsetFetchRequest::class, OffsetFetchResponse::class, null, -1, 3751],
                [OffsetFetchRequestV8::class, OffsetFetchResponseV8::class, null, -1, 3752],
            ] as [$request, $response, $memberId, $epoch, $correlationId]
        ) {
            $entry = $this->fetch(
                $stream,
                $request,
                $response,
                $groupId,
                [$topic => [0]],
                $memberId,
                $epoch,
                $correlationId
            )->groupOf($groupId);

            self::assertSame(KafkaException::NO_ERROR, $entry->errorCode, 'an unknown group is no error');
            self::assertSame(-1, $entry->topics[$topic]->partitions[0]->offset);
            self::assertSame(KafkaException::NO_ERROR, $entry->topics[$topic]->partitions[0]->errorCode);
        }
    }

    /**
     * The `require_stable` flag of KIP-447 and the member of KIP-848 are independent of each other
     */
    public function testTheRequireStableFlagAndTheMemberEpochAreIndependent(): void
    {
        $topic   = $this->topic();
        $groupId = $this->uniqueGroupName();
        [$memberId, $epoch] = $this->modernGroupWithACommittedOffset($groupId, $topic);
        $stream  = $this->coordinatorStream($groupId);

        $stable = $this->fetch(
            $stream,
            OffsetFetchRequest::class,
            OffsetFetchResponse::class,
            $groupId,
            [$topic => [0]],
            $memberId,
            $epoch,
            3760,
            true
        )->groupOf($groupId);

        self::assertSame(KafkaException::NO_ERROR, $stable->errorCode);
        self::assertSame(self::COMMITTED_OFFSET, $stable->topics[$topic]->partitions[0]->offset);

        $staleAndStable = $this->fetch(
            $stream,
            OffsetFetchRequest::class,
            OffsetFetchResponse::class,
            $groupId,
            [$topic => [0]],
            $memberId,
            $epoch - 1,
            3761,
            true
        )->groupOf($groupId);

        self::assertSame(
            KafkaException::STALE_MEMBER_EPOCH,
            $staleAndStable->errorCode,
            'the coordinator serves a stable read with a write operation and validates the member all the same'
        );
    }

    /**
     * The member is validated per entry of a batch: one bad entry never costs the other groups their answer
     */
    public function testTheMemberIsValidatedPerEntryOfABatch(): void
    {
        $topic    = $this->topic();
        $classic  = $this->uniqueGroupName();
        $modern   = $this->uniqueGroupName();
        $this->classicGroupWithACommittedOffset($classic, $topic);
        [$memberId, $epoch] = $this->modernGroupWithACommittedOffset($modern, $topic);

        $stream = $this->coordinatorStream($classic);

        OffsetFetchRequest::forGroups(
            [
                $classic => [$topic => [0]],
                $modern  => new OffsetFetchRequestGroup($modern, [$topic => [0]], $memberId, $epoch - 1),
            ],
            self::CLIENT_ID,
            3770
        )->writeTo($stream);
        $answer = OffsetFetchResponse::unpack($stream);

        self::assertSame(
            self::COMMITTED_OFFSET,
            $answer->groupOf($classic)->topics[$topic]->partitions[0]->offset
        );
        self::assertSame(KafkaException::NO_ERROR, $answer->groupOf($classic)->errorCode);
        self::assertSame(KafkaException::STALE_MEMBER_EPOCH, $answer->groupOf($modern)->errorCode);
        self::assertSame([], $answer->groupOf($modern)->topics);
    }

    /**
     * Sends one OffsetFetch of the given version with the two member fields of KIP-848 and reads its answer
     *
     * @param class-string<OffsetFetchRequest>  $requestClass
     * @param class-string<OffsetFetchResponse> $responseClass
     * @param array<string, list<int>>|null     $topicPartitions
     */
    private function fetch(
        Stream $stream,
        string $requestClass,
        string $responseClass,
        string $groupId,
        ?array $topicPartitions,
        ?string $memberId,
        int $memberEpoch,
        int $correlationId,
        bool $requireStable = false
    ): OffsetFetchResponse {
        $requestClass::forGroups(
            [$groupId => new OffsetFetchRequestGroup($groupId, $topicPartitions, $memberId, $memberEpoch)],
            self::CLIENT_ID,
            $correlationId,
            $requireStable
        )->writeTo($stream);

        return $responseClass::unpack($stream);
    }

    /**
     * Joins a fresh classic group with one member, publishes its assignment and commits one offset
     */
    private function classicGroupWithACommittedOffset(string $groupId, string $topic): void
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
            3690,
            null,
            'the t3-37 suite started'
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
            3691,
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
            3692,
            null,
            $joined->protocolType,
            $joined->groupProtocol
        )->writeTo($stream);
        $synced = SyncGroupResponse::unpack($stream);

        self::assertSame(KafkaException::NO_ERROR, $synced->errorCode, 'The node refused the sync');

        self::$members[] = [$groupId, $joined->memberId];

        $this->commit($stream, $groupId, $joined->generationId, $joined->memberId, $topic, 3693);
    }

    /**
     * Creates a group of the KIP-848 protocol with one member and commits one offset as that member
     *
     * @return array{string, int} The member id and the member epoch the coordinator answered with
     */
    private function modernGroupWithACommittedOffset(string $groupId, string $topic): array
    {
        $memberId = ConsumerGroupHeartbeatCoordinator::newMemberId();
        $answer   = new Client($this->cluster(), $this->configuration())->joinConsumerGroup(
            $this->coordinator($groupId),
            $groupId,
            $memberId,
            [$topic],
            self::REBALANCE_TIMEOUT_MS
        );
        $epoch    = $answer->memberEpoch;

        self::$modernMembers[] = [$groupId, $memberId];

        self::assertSame(KafkaException::NO_ERROR, $answer->errorCode, 'The node refused the heartbeat');
        self::assertGreaterThan(0, $epoch, 'a member that joined holds an epoch above zero');

        $this->commit($this->coordinatorStream($groupId), $groupId, $epoch, $memberId, $topic, 3695);

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
            'The node refused the commit this test reads back'
        );
    }

    /**
     * Creates the one topic of a test and waits until every partition of it has a leader
     */
    private function topic(): string
    {
        $topic = self::uniqueTopicName(self::TOPIC_PREFIX);

        new AdminClient($this->cluster(), $this->configuration())->createTopics([new NewTopic($topic, 1, 1)]);
        new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, self::TOPIC_PREFIX)
            ->awaitTopicWithLeaders($topic);

        return $topic;
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
        $groupId        = 't3-37-group-' . bin2hex(random_bytes(6));
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
                'the t3-37 suite is done'
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
            $configuration = self::cleanupConfiguration();
            $cluster       = Cluster::bootstrap($configuration);

            new Client($cluster, $configuration)->leaveConsumerGroup(
                new CoordinatorLookup($cluster, $configuration)->findCoordinator($groupId),
                $groupId,
                $memberId
            );
        } catch (KafkaException) {
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
