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
use Protocol\Kafka\Common\Errors\GroupIdNotFoundException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\MemberAssignment;
use Protocol\Kafka\Consumer\Subscription;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Request\JoinGroupRequest;
use Protocol\Kafka\Protocol\Request\JoinGroupResponse;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequest;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequestV8;
use Protocol\Kafka\Protocol\Request\OffsetCommitResponse;
use Protocol\Kafka\Protocol\Request\OffsetCommitResponseV8;
use Protocol\Kafka\Protocol\Request\SyncGroupRequest;
use Protocol\Kafka\Protocol\Request\SyncGroupResponse;
use Protocol\Kafka\Tests\Fixture\ConsumerGroupHeartbeatProbe;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * What Kafka 3.6 added to the group apis, against the 3.9.2 KRaft node: the OffsetCommit **v9** of KIP-848.
 *
 * The version adds no field to either half of the api - "the request is the same as version 8" - and what it
 * buys is a promise about the *answer*: a client that sends it is told the **69** `GroupIdNotFound` of a group
 * the coordinator does not know, where the versions below it are told the 22 of the backward compatibility, and
 * a member of a **KIP-848** group is told the **113** `StaleMemberEpoch` when the epoch it committed with is not
 * the one the coordinator holds for it. Every code of that promise is driven here against the node, at the
 * version 9 and at the version 8 next to it.
 *
 * The KIP-848 group of the last two tests is created with a hand-built **ConsumerGroupHeartbeat** (key 68,
 * {@see ConsumerGroupHeartbeatProbe}): the api itself is the last wave of this line and has no classes yet, but
 * without a group of the new protocol neither the 113 nor the 35 of a member that sends a version below 9 can be
 * produced at all.
 *
 * Every group and topic of this class carries the `t3-36-` prefix of the Kafka 3.6 wave and is removed again in
 * {@see self::tearDownAfterClass()}.
 *
 * @see docs/protocol/3.9.md, section "The member epoch of KIP-848 (v9)"
 * @see docs/protocol/3.9.md, section "OffsetCommit API (key 8, v0 to v9)"
 */
#[CoversClass(Client::class)]
#[CoversClass(OffsetCommitRequest::class)]
#[CoversClass(OffsetCommitRequestV8::class)]
#[CoversClass(OffsetCommitResponse::class)]
#[CoversClass(OffsetCommitResponseV8::class)]
final class MemberEpochCommitApiTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t3-36';

    private const string TOPIC_PREFIX = 't3-36-commit';

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
     * Takes every member of this class out of its group first: a group that still holds one is not deletable
     */
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
     * The commit of a classic member is the version 9 frame, and the node answers it exactly as it answers a v8
     */
    public function testAClassicMemberCommitsWithTheVersionNineOfKip848(): void
    {
        $topic   = $this->topic();
        $groupId = $this->uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);
        $joined  = $this->joinAndSync($stream, $groupId, $topic);

        $answer = $this->commit(
            $stream,
            OffsetCommitRequest::class,
            OffsetCommitResponse::class,
            $groupId,
            $joined->generationId,
            $joined->memberId,
            $topic,
            21,
            3601
        );

        self::assertSame(9, OffsetCommitRequest::VERSION, 'the version this client sends since Kafka 3.6');
        self::assertSame(KafkaException::NO_ERROR, $answer->topics[$topic]->partitions[0]->errorCode);
        self::assertSame(0, $answer->throttleTimeMs);

        // and the very same commit one version lower is accepted as well: a classic member may send either
        $lower = $this->commit(
            $stream,
            OffsetCommitRequestV8::class,
            OffsetCommitResponseV8::class,
            $groupId,
            $joined->generationId,
            $joined->memberId,
            $topic,
            22,
            3602
        );

        self::assertSame(KafkaException::NO_ERROR, $lower->topics[$topic]->partitions[0]->errorCode);

        $committed = new Client($this->cluster(), $this->configuration())->fetchGroupOffsets(
            new CoordinatorLookup($this->cluster(), $this->configuration())->findCoordinator($groupId),
            $groupId,
            [$topic => [0]]
        );

        self::assertSame(22, $committed[$topic][0], 'the second commit is the one that stands');
    }

    /**
     * A classic group answers a generation it does not have with the 22 at both versions, never with the 113
     */
    public function testAWrongGenerationOfAClassicGroupIsTheTwentyTwoAtBothVersions(): void
    {
        $topic   = $this->topic();
        $groupId = $this->uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);
        $joined  = $this->joinAndSync($stream, $groupId, $topic);

        foreach ([[OffsetCommitRequest::class, OffsetCommitResponse::class, 3603],
            [OffsetCommitRequestV8::class, OffsetCommitResponseV8::class, 3604]] as [$request, $response, $id]) {
            $answer = $this->commit(
                $stream,
                $request,
                $response,
                $groupId,
                $joined->generationId + 5,
                $joined->memberId,
                $topic,
                21,
                $id
            );

            self::assertSame(
                KafkaException::ILLEGAL_GENERATION,
                $answer->topics[$topic]->partitions[0]->errorCode,
                'The version ' . $request::VERSION . ' answered another code than the 22 of a classic group'
            );
        }
    }

    /**
     * A member id the classic group does not have is the 25 at both versions
     */
    public function testAMemberTheClassicGroupDoesNotHaveIsTheTwentyFiveAtBothVersions(): void
    {
        $topic   = $this->topic();
        $groupId = $this->uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);
        $joined  = $this->joinAndSync($stream, $groupId, $topic);

        foreach ([[OffsetCommitRequest::class, OffsetCommitResponse::class, 3605],
            [OffsetCommitRequestV8::class, OffsetCommitResponseV8::class, 3606]] as [$request, $response, $id]) {
            $answer = $this->commit(
                $stream,
                $request,
                $response,
                $groupId,
                $joined->generationId,
                't3-36-nobody',
                $topic,
                21,
                $id
            );

            self::assertSame(
                KafkaException::UNKNOWN_MEMBER_ID,
                $answer->topics[$topic]->partitions[0]->errorCode,
                'The version ' . $request::VERSION . ' answered another code than the 25'
            );
        }
    }

    /**
     * The one code the version 9 was added for on the classic side: the 69 of a group that does not exist
     */
    public function testAGroupTheCoordinatorDoesNotKnowIsSixtyNineAtVersionNineAndTwentyTwoBelowIt(): void
    {
        $topic   = $this->topic();
        $groupId = $this->uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);

        $version9 = $this->commit(
            $stream,
            OffsetCommitRequest::class,
            OffsetCommitResponse::class,
            $groupId,
            1,
            't3-36-nobody',
            $topic,
            21,
            3607
        );

        self::assertSame(
            KafkaException::GROUP_ID_NOT_FOUND,
            $version9->topics[$topic]->partitions[0]->errorCode,
            'the 69 of KIP-848, which the version 9 promises for both group protocols'
        );

        $version8 = $this->commit(
            $stream,
            OffsetCommitRequestV8::class,
            OffsetCommitResponseV8::class,
            $groupId,
            1,
            't3-36-nobody',
            $topic,
            21,
            3608
        );

        self::assertSame(
            KafkaException::ILLEGAL_GENERATION,
            $version8->topics[$topic]->partitions[0]->errorCode,
            'the same body one version lower is the 22 "to preserve the backward compatibility"'
        );
    }

    /**
     * The client maps that code, so a commit of a group that does not exist is a `GroupIdNotFoundException` now
     */
    public function testTheClientReportsTheSixtyNineOfAnUnknownGroupAsItsOwnException(): void
    {
        $topic   = $this->topic();
        $groupId = $this->uniqueGroupName();
        $client  = new Client($this->cluster(), $this->configuration());

        $this->expectException(GroupIdNotFoundException::class);

        $client->commitGroupOffsets(
            new CoordinatorLookup($this->cluster(), $this->configuration())->findCoordinator($groupId),
            $groupId,
            't3-36-nobody',
            1,
            [$topic => [0 => 21]],
            OffsetCommitRequest::DEFAULT_RETENTION_TIME
        );
    }

    /**
     * A commit that claims no membership is not refused at all: the coordinator creates a simple group for it
     */
    public function testACommitWithoutAMembershipCreatesASimpleGroupInsteadOfTheSixtyNine(): void
    {
        $topic   = $this->topic();
        $groupId = $this->uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);

        $answer = $this->commit(
            $stream,
            OffsetCommitRequest::class,
            OffsetCommitResponse::class,
            $groupId,
            OffsetCommitRequest::DEFAULT_GENERATION_ID,
            OffsetCommitRequest::DEFAULT_MEMBER_NAME,
            $topic,
            7,
            3609
        );

        self::assertSame(
            KafkaException::NO_ERROR,
            $answer->topics[$topic]->partitions[0]->errorCode,
            'the negative generation is checked before the group is looked up, so the 69 never happens here'
        );
    }

    /**
     * The 113 of KIP-848: a member of a consumer group of the new protocol commits with an epoch of its own
     */
    public function testAMemberOfAKip848GroupIsAnsweredTheStaleMemberEpoch(): void
    {
        $topic   = $this->topic();
        $groupId = $this->uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);
        $member  = 't3-36-' . bin2hex(random_bytes(8));
        $epoch   = $this->joinWithAHeartbeat($groupId, $member, $topic);

        $accepted = $this->commit(
            $stream,
            OffsetCommitRequest::class,
            OffsetCommitResponse::class,
            $groupId,
            $epoch,
            $member,
            $topic,
            11,
            3610
        );

        self::assertSame(
            KafkaException::NO_ERROR,
            $accepted->topics[$topic]->partitions[0]->errorCode,
            'the member epoch stands in the field a classic member writes its generation into'
        );

        foreach ([$epoch - 1 => 3611, $epoch + 1 => 3612] as $wrongEpoch => $correlationId) {
            $answer = $this->commit(
                $stream,
                OffsetCommitRequest::class,
                OffsetCommitResponse::class,
                $groupId,
                $wrongEpoch,
                $member,
                $topic,
                11,
                $correlationId
            );

            self::assertSame(
                KafkaException::STALE_MEMBER_EPOCH,
                $answer->topics[$topic]->partitions[0]->errorCode,
                "The epoch {$wrongEpoch} was not answered with the 113 of Kafka 3.6"
            );
        }

        $this->leaveWithAHeartbeat($groupId, $member);
    }

    /**
     * A member of a KIP-848 group may not send a version below 9, and the refusal is a per-partition 35
     */
    public function testAMemberOfAKip848GroupMayNotCommitWithTheVersionEight(): void
    {
        $topic   = $this->topic();
        $groupId = $this->uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);
        $member  = 't3-36-' . bin2hex(random_bytes(8));
        $epoch   = $this->joinWithAHeartbeat($groupId, $member, $topic);

        $answer = $this->commit(
            $stream,
            OffsetCommitRequestV8::class,
            OffsetCommitResponseV8::class,
            $groupId,
            $epoch,
            $member,
            $topic,
            11,
            3613
        );

        self::assertSame(
            KafkaException::UNSUPPORTED_VERSION,
            $answer->topics[$topic]->partitions[0]->errorCode,
            'the 35 of "OffsetCommit version 9 or above must be used by members using the modern group protocol"'
        );

        $this->leaveWithAHeartbeat($groupId, $member);
    }

    /**
     * Sends one OffsetCommit of the given version and returns the answer
     *
     * @param class-string<OffsetCommitRequest>  $requestClass
     * @param class-string<OffsetCommitResponse> $responseClass
     */
    private function commit(
        Stream $stream,
        string $requestClass,
        string $responseClass,
        string $groupId,
        int $generationId,
        string $memberId,
        string $topic,
        int $offset,
        int $correlationId
    ): OffsetCommitResponse {
        new $requestClass(
            $groupId,
            $generationId,
            $memberId,
            OffsetCommitRequest::DEFAULT_RETENTION_TIME,
            [$topic => [0 => $offset]],
            self::CLIENT_ID,
            $correlationId
        )->writeTo($stream);

        return $responseClass::unpack($stream);
    }

    /**
     * Joins a fresh classic group with one member and publishes its assignment, so that the group is `Stable`
     */
    private function joinAndSync(Stream $stream, string $groupId, string $topic): JoinGroupResponse
    {
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
            'the t3-36 suite started'
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

        return $joined;
    }

    /**
     * Creates a consumer group of the KIP-848 protocol with one member and returns the epoch of that member
     */
    private function joinWithAHeartbeat(string $groupId, string $memberId, string $topic): int
    {
        $epoch = new ConsumerGroupHeartbeatProbe(self::firstBootstrapServer())
            ->join($groupId, $memberId, [$topic], self::REBALANCE_TIMEOUT_MS, 3693);

        self::assertGreaterThan(0, $epoch, 'a member that joined holds an epoch above zero');

        return $epoch;
    }

    /**
     * Takes the member out of its KIP-848 group again, so that the group can be deleted
     */
    private function leaveWithAHeartbeat(string $groupId, string $memberId): void
    {
        $answer = new ConsumerGroupHeartbeatProbe(self::firstBootstrapServer())->leave($groupId, $memberId, 3694);

        self::assertSame(KafkaException::NO_ERROR, $answer['errorCode'], 'The node refused the leave');
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
        $groupId        = 't3-36-group-' . bin2hex(random_bytes(6));
        self::$groups[] = $groupId;

        return $groupId;
    }

    /**
     * Takes one member of this class out of its group, ignoring a member or a group that is gone already
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
                'the t3-36 suite is done'
            );
        } catch (KafkaException) {
            // A member the session timeout has already reaped, or a group that is gone, must not fail the suite
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
