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
use Protocol\Kafka\Admin\ConsumerGroupDescription;
use Protocol\Kafka\Admin\ConsumerGroupMemberDescription;
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\AclOperation;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\CoordinatorLookup;
use Protocol\Kafka\Common\Errors\GroupIdNotFoundException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Common\Security\SaslMechanism;
use Protocol\Kafka\Common\Security\SecurityProtocol;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\Internals\ConsumerGroupHeartbeatCoordinator;
use Protocol\Kafka\Consumer\MemberAssignment;
use Protocol\Kafka\Consumer\Subscription;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Data\ConsumerGroupDescribedGroup;
use Protocol\Kafka\Protocol\Data\DescribeGroupResponseMetadata;
use Protocol\Kafka\Protocol\Request\ConsumerGroupDescribeRequest;
use Protocol\Kafka\Protocol\Request\ConsumerGroupDescribeResponse;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * ConsumerGroupDescribe (key 69, Kafka 3.7, KIP-848) against the 3.9.2 KRaft node.
 *
 * The api is the DescribeGroups of the new consumer protocol and it does **not** overlap with key 15: this suite
 * drives both of them at the same group, in both directions, because the pair of answers is what decides how an
 * admin client has to route - and the two answers are not symmetric.
 *
 * Every group of this class carries the `t3-848-desc-` prefix and every member of it leaves with the epoch -1
 * before the group is deleted, in {@see self::tearDownAfterClass()}.
 *
 * @see docs/protocol/3.9.md, section "ConsumerGroupDescribe API (key 69, v0)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(ConsumerGroupDescribeRequest::class)]
#[CoversClass(ConsumerGroupDescribeResponse::class)]
#[CoversClass(ConsumerGroupDescription::class)]
#[CoversClass(ConsumerGroupMemberDescription::class)]
final class ConsumerGroupDescribeApiTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t3-848-desc';

    private const string TOPIC_PREFIX = 't3-848-desc';

    private const int REBALANCE_TIMEOUT_MS = 300000;

    private const int REQUEST_TIMEOUT_MS = 30000;

    private static ?Cluster $sharedCluster = null;

    /**
     * @var list<string>
     */
    private static array $groups = [];

    /**
     * @var list<array{string, string}>
     */
    private static array $members = [];

    /**
     * @var list<array{string, string}>
     */
    private static array $classicMembers = [];

    public static function tearDownAfterClass(): void
    {
        foreach (self::$members as [$groupId, $memberId]) {
            self::leaveQuietly($groupId, $memberId);
        }
        self::$members = [];

        foreach (self::$classicMembers as [$groupId, $memberId]) {
            self::leaveClassicQuietly($groupId, $memberId);
        }
        self::$classicMembers = [];

        foreach (self::$groups as $groupId) {
            self::deleteGroupQuietly($groupId);
        }
        self::$groups = [];

        parent::tearDownAfterClass();
    }

    /**
     * The answer of one settled member: the three epochs, the assignor and both assignments
     */
    public function testAGroupOfOneMemberIsDescribedWithItsEpochsAndBothAssignments(): void
    {
        $topic   = $this->topic(2);
        $groupId = $this->uniqueGroupName();
        [$memberId] = $this->joinedMember($groupId, [$topic]);

        $description = $this->admin()->describeConsumerGroup($groupId);

        self::assertSame($groupId, $description->groupId);
        self::assertSame(ConsumerGroupDescribedGroup::STATE_STABLE, $description->state);
        self::assertSame(
            $description->groupEpoch,
            $description->assignmentEpoch,
            'a settled group has computed its target assignment for the current group epoch'
        );
        self::assertSame(
            'uniform',
            $description->assignorName,
            'the first of group.consumer.assignors, which the coordinator picks when no member names one'
        );
        self::assertTrue($description->isStable());
        self::assertSame(
            AclOperation::NOT_REQUESTED,
            $description->authorizedOperations,
            'the request did not ask for them'
        );

        self::assertArrayHasKey($memberId, $description->members);
        $member = $description->members[$memberId];

        self::assertSame($description->groupEpoch, $member->memberEpoch, 'the member caught up with the group');
        self::assertSame(self::CLIENT_ID, $member->clientId);
        self::assertStringStartsWith('/', $member->clientHost, 'the address of the Java InetAddress');
        self::assertSame([$topic], $member->subscribedTopicNames, 'plain names, not the packed metadata of key 15');
        self::assertSame(
            '',
            $member->subscribedTopicRegex,
            'the node answers the EMPTY string where the specification declares a nullable field'
        );
        self::assertNull($member->instanceId, 'a dynamic member');
        self::assertNull($member->rackId);
        self::assertSame([$topic => [0, 1]], $member->assignment);
        self::assertSame([$topic => [0, 1]], $member->targetAssignment);
        self::assertTrue($member->isReconciled());
        self::assertFalse($member->isStatic());
        self::assertSame([$topic => [0, 1]], $description->assignment());
    }

    /**
     * Two members share the partitions, and the answer names both assignments of each of them
     */
    public function testAGroupOfTwoMembersReportsTheAssignmentOfEachOfThem(): void
    {
        $topic   = $this->topic(3);
        $topicId = self::topicIdOf($topic);
        $groupId = $this->uniqueGroupName();
        $node    = $this->coordinator($groupId);
        $client  = $this->client();

        [$first, $firstEpoch] = $this->joinedMember($groupId, [$topic]);
        [$second, $secondEpoch] = $this->joinedMember($groupId, [$topic]);

        // The first member releases what it lost, so that the group settles before it is described
        $revoked = $client->consumerGroupHeartbeat($node, $groupId, $first, $firstEpoch);
        $kept    = $revoked->assignment?->partitionsByTopicId()[$topicId] ?? [];
        $client->consumerGroupHeartbeat($node, $groupId, $first, $revoked->memberEpoch, null, [$topicId => $kept]);

        $handed = $client->consumerGroupHeartbeat($node, $groupId, $second, $secondEpoch);
        $taken  = $handed->assignment?->partitionsByTopicId()[$topicId] ?? [];
        $client->consumerGroupHeartbeat($node, $groupId, $second, $handed->memberEpoch, null, [$topicId => $taken]);

        $description = $this->admin()->describeConsumerGroup($groupId);

        self::assertCount(2, $description->members);
        self::assertSame([$topic => [0, 1, 2]], $description->assignment(), 'together they hold every partition');
        self::assertSame([$topic => $kept], $description->members[$first]->assignment);
        self::assertSame([$topic => $taken], $description->members[$second]->assignment);

        foreach ($description->members as $member) {
            self::assertTrue($member->isReconciled(), 'both members caught up with their target assignment');
        }
    }

    /**
     * The flag of KIP-430 is a field of the version 0, because the api was born long after that KIP
     */
    public function testTheAuthorizedOperationsAreReportedWhenTheRequestAsksForThem(): void
    {
        $topic   = $this->topic(1);
        $groupId = $this->uniqueGroupName();
        $this->joinedMember($groupId, [$topic]);

        $description = $this->admin()->describeConsumerGroup($groupId, true);

        self::assertNotSame(AclOperation::NOT_REQUESTED, $description->authorizedOperations);
        self::assertSame(
            [AclOperation::READ, AclOperation::DELETE, AclOperation::DESCRIBE],
            $description->authorizedOperations(),
            'the three operations the super user of this node holds on a group'
        );
    }

    /**
     * A group of the classic protocol is not described by this api at all
     */
    public function testAGroupOfTheClassicProtocolIsRefusedWithTheSixtyNine(): void
    {
        $topic   = $this->topic(1);
        $groupId = $this->uniqueGroupName();
        $this->classicGroup($groupId, $topic);

        $answer = $this->describeRaw([$groupId]);
        $entry  = $answer->groups[$groupId];

        self::assertSame(KafkaException::GROUP_ID_NOT_FOUND, $entry->errorCode);
        self::assertNull($entry->errorMessage, 'the refusal carries no sentence at all');
        self::assertSame('', $entry->groupState);
        self::assertSame([], $entry->members);

        $this->expectException(GroupIdNotFoundException::class);

        $this->admin()->describeConsumerGroups([$groupId]);
    }

    /**
     * And the other way round the classic api answers `Dead` rather than an error, which is the routing rule
     */
    public function testTheClassicDescribeAnswersAKip848GroupTheStateDead(): void
    {
        $topic   = $this->topic(1);
        $groupId = $this->uniqueGroupName();
        $this->joinedMember($groupId, [$topic]);

        $description = $this->admin()->describeGroup($groupId);

        self::assertSame(KafkaException::NO_ERROR, $description->errorCode, 'the classic api does not refuse it');
        self::assertSame(DescribeGroupResponseMetadata::STATE_DEAD, $description->state);
        self::assertSame('', $description->protocolType);
        self::assertSame([], $description->members, 'a live group looks like one that never existed');

        // which is why the routing has to follow the type of a ListGroups v5 answer
        $listed = $this->admin()->listAllGroups();

        self::assertArrayHasKey($groupId, $listed);
        self::assertSame('consumer', $listed[$groupId]->groupType);
    }

    /**
     * A group the coordinator has never heard of is the same 69, with the same null message
     */
    public function testAGroupThatDoesNotExistIsTheSameSixtyNine(): void
    {
        $groupId = 't3-848-desc-no-such-group-' . bin2hex(random_bytes(4));
        $entry   = $this->describeRaw([$groupId])->groups[$groupId];

        self::assertSame(KafkaException::GROUP_ID_NOT_FOUND, $entry->errorCode);
        self::assertNull($entry->errorMessage);
        self::assertSame(
            '',
            $entry->groupState,
            'the api does not tell a wrong protocol and a missing group apart'
        );
    }

    /**
     * The empty group id crashes the answer builder of the node, which answers the -1 of an unknown server error
     */
    public function testTheEmptyGroupIdIsAnsweredTheMinusOneOfTheAnswerBuilder(): void
    {
        $answer = $this->describeRaw(['']);

        self::assertCount(1, $answer->groups);

        $entry = reset($answer->groups);

        self::assertSame(
            KafkaException::UNKNOWN,
            $entry->errorCode,
            'the handler builds a refusal without a group id and cannot serialize it - see the node log'
        );
        self::assertSame('', $entry->groupId, 'and the id it is refusing is missing from the answer');
    }

    /**
     * The one principal outside `super.users` is refused per ENTRY, unlike the heartbeat api
     */
    public function testAnUnauthorizedPrincipalIsRefusedInsideTheEntry(): void
    {
        if (self::saslBootstrapServer() === '') {
            self::markTestSkipped(self::SASL_BOOTSTRAP_SERVERS_ENV . ' is not set, an acl needs a principal');
        }

        $topic   = $this->topic(1);
        $groupId = $this->uniqueGroupName();
        $this->joinedMember($groupId, [$topic]);

        $stream = new SocketStream(
            'tcp://' . self::saslBootstrapServer(),
            $this->unprivilegedConfiguration(),
            5.0
        );

        new ConsumerGroupDescribeRequest([$groupId], true, self::CLIENT_ID)->writeTo($stream);
        $answer = ConsumerGroupDescribeResponse::unpack($stream);
        $entry  = $answer->groups[$groupId];

        self::assertSame(KafkaException::GROUP_AUTHORIZATION_FAILED, $entry->errorCode);
        self::assertNull($entry->errorMessage);
        self::assertSame('', $entry->groupState);
        self::assertSame(
            AclOperation::NOT_REQUESTED,
            $entry->authorizedOperations,
            'even though the request asked for them'
        );
    }

    /**
     * Sends the api by hand, so that a refused entry can be read instead of thrown
     *
     * @param list<string> $groupIds
     */
    private function describeRaw(array $groupIds): ConsumerGroupDescribeResponse
    {
        $stream = $this->coordinatorStream($groupIds[0] === '' ? 't3-848-desc-any' : $groupIds[0]);

        new ConsumerGroupDescribeRequest($groupIds, false, self::CLIENT_ID)->writeTo($stream);

        return ConsumerGroupDescribeResponse::unpack($stream);
    }

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

        // a member that was given something acknowledges it, so that the group is Stable when it is described
        $assignment = $answer->assignment?->partitionsByTopicId() ?? [];
        if ($assignment !== []) {
            $this->client()->consumerGroupHeartbeat(
                $this->coordinator($groupId),
                $groupId,
                $member,
                $answer->memberEpoch,
                null,
                $assignment
            );
        }

        return [$member, $answer->memberEpoch];
    }

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

        self::$classicMembers[] = [$groupId, $join->memberId];
    }

    private function admin(): AdminClient
    {
        return new AdminClient($this->cluster(), $this->configuration());
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
            ClientConfig::SECURITY_PROTOCOL         => SecurityProtocol::SASL_PLAINTEXT,
            ClientConfig::SASL_MECHANISM            => SaslMechanism::PLAIN,
            ClientConfig::SASL_USERNAME             => 'acltest',
            ClientConfig::SASL_PASSWORD             => 'acltest-secret',
        ] + ConsumerConfig::getDefaultConfiguration();
    }

    private function uniqueGroupName(): string
    {
        $groupId        = 't3-848-desc-group-' . bin2hex(random_bytes(6));
        self::$groups[] = $groupId;

        return $groupId;
    }

    private static function leaveQuietly(string $groupId, string $memberId): void
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
            // A member the coordinator has already forgotten must not fail the suite
        }
    }

    private static function leaveClassicQuietly(string $groupId, string $memberId): void
    {
        try {
            $configuration = self::cleanupConfiguration();
            $cluster       = Cluster::bootstrap($configuration);

            new Client($cluster, $configuration)->leaveGroup(
                new CoordinatorLookup($cluster, $configuration)->findCoordinator($groupId),
                $groupId,
                $memberId,
                null,
                'the t3-848-desc suite is done'
            );
        } catch (KafkaException) {
            // The same
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
