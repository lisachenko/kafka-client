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
use Protocol\Kafka\Admin\AlterConfigOp;
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\CoordinatorLookup;
use Protocol\Kafka\Common\Errors\InvalidRequestException;
use Protocol\Kafka\Common\Errors\InvalidShareSessionEpochException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\ShareSessionNotFoundException;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\Internals\ConsumerGroupHeartbeatCoordinator;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Data\IncrementalAlterConfigsRequestResource;
use Protocol\Kafka\Protocol\Data\ShareAcknowledgementBatch;
use Protocol\Kafka\Protocol\Data\ShareFetchResponsePartition;
use Protocol\Kafka\Protocol\Request\IncrementalAlterConfigsRequest;
use Protocol\Kafka\Protocol\Request\IncrementalAlterConfigsResponse;
use Protocol\Kafka\Protocol\Request\ProduceRequestV12;
use Protocol\Kafka\Protocol\Request\ProduceResponseV12;
use Protocol\Kafka\Protocol\Request\ShareAcknowledgeRequest;
use Protocol\Kafka\Protocol\Request\ShareAcknowledgeResponse;
use Protocol\Kafka\Protocol\Request\ShareFetchRequest;
use Protocol\Kafka\Protocol\Request\ShareFetchResponse;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * ShareFetch (key 78) and ShareAcknowledge (key 79) of KIP-932 against the 4.3.1 KRaft node, at their version 1
 * (Kafka 4.1): the acquisition of records by a share member, their delivery count, the three acknowledgements
 * accept, release and reject, the redelivery of released records and the share session with its epochs and codes.
 *
 * Every test has a group and a topic of its own with one partition: the member joins, heartbeats until the
 * partition is assigned to it and reads with its own share session on the leader. A share group reads from
 * `latest` unless `share.auto.offset.reset` says otherwise, a *group* config (config resource type 32) that the
 * tests set to `earliest` before the join - except the one test that measures the default.
 *
 * Every group of this class carries the `t3-41-sf-` prefix; every member closes its share session (epoch -1) and
 * leaves (epoch -1) in {@see self::tearDownAfterClass()} at the latest, and every group is deleted there.
 *
 * @see docs/protocol/4.3.md, section "ShareFetch API (key 78, v1)"
 * @see docs/protocol/4.3.md, section "ShareAcknowledge API (key 79, v1)"
 */
#[CoversClass(Client::class)]
#[CoversClass(ShareFetchRequest::class)]
#[CoversClass(ShareFetchResponse::class)]
#[CoversClass(ShareAcknowledgeRequest::class)]
#[CoversClass(ShareAcknowledgeResponse::class)]
#[CoversClass(ShareFetchResponsePartition::class)]
final class ShareFetchApiTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t3-41-sf';

    private const string TOPIC_PREFIX = 't3-41-sf';

    private const int REQUEST_TIMEOUT_MS = 30000;

    /**
     * The group config resource of KIP-848 and KIP-932 (`ConfigResource.Type.GROUP`, id 32)
     */
    private const int GROUP_CONFIG_RESOURCE = 32;

    /**
     * How long a test waits for an assignment or for records, in seconds
     */
    private const float WAIT_TIMEOUT = 20.0;

    private const int MAX_WAIT_MS = 500;

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
     * The epoch 0 opens a share session and acquires the records, each range with its delivery count
     */
    public function testTheEpochZeroOpensASessionAndAcquiresTheRecordsForTheLockDuration(): void
    {
        [$groupId, $topicId, $leader] = $this->shareGroup(['v0', 'v1', 'v2', 'v3', 'v4', 'v5']);
        $member = $this->joinedMember($groupId, $topicId);

        [$answer] = $this->fetchUntilAcquired($leader, $groupId, $member, $topicId);

        self::assertSame(KafkaException::NO_ERROR, $answer->errorCode);
        self::assertNull($answer->errorMessage);
        self::assertSame(30000, $answer->acquisitionLockTimeoutMs, 'share.record.lock.duration.ms of the group');
        self::assertSame([], $answer->nodeEndpoints, 'the leader of every partition is the node that answered');

        $partition = $answer->partitionOf($topicId, 0);
        self::assertNotNull($partition);
        self::assertSame(KafkaException::NO_ERROR, $partition->errorCode);
        self::assertSame(KafkaException::NO_ERROR, $partition->acknowledgeErrorCode);
        self::assertSame(
            [0, 0],
            [$partition->currentLeader->leaderId, $partition->currentLeader->leaderEpoch],
            'the leader is filled in only for the codes 6 and 74, else it is the 0 0 of a field without default'
        );
        self::assertNotSame(0, $leader->nodeId, 'which is not the id of the leader that answered');
        self::assertCount(1, $partition->acquiredRecords, 'one range for the one batch that was produced');
        self::assertSame(0, $partition->acquiredRecords[0]->firstOffset);
        self::assertSame(5, $partition->acquiredRecords[0]->lastOffset);
        self::assertSame(1, $partition->acquiredRecords[0]->deliveryCount);
        self::assertSame(
            ['v0', 'v1', 'v2', 'v3', 'v4', 'v5'],
            array_map(static fn(array $acquired): ?string => $acquired['record']->value, array_values($partition->acquiredRecords())),
            'a fresh group with share.auto.offset.reset=earliest reads from the start of the partition'
        );
    }

    /**
     * Accept, release and reject in the next ShareFetch; the released records come back with a delivery count 2
     */
    public function testReleasedRecordsAreRedeliveredWithTheNextDeliveryCount(): void
    {
        [$groupId, $topicId, $leader] = $this->shareGroup(['v0', 'v1', 'v2', 'v3', 'v4', 'v5']);
        $member = $this->joinedMember($groupId, $topicId);
        [, $epoch] = $this->fetchUntilAcquired($leader, $groupId, $member, $topicId);

        // accept 0, release 1, reject 2 one type per offset, and release 3-5 with one type for the range
        $answer = $this->client()->shareFetch(
            $leader,
            $groupId,
            $member,
            $epoch,
            [$topicId => [0]],
            [$topicId => [0 => [
                new ShareAcknowledgementBatch(0, 2, [
                    ShareAcknowledgementBatch::ACCEPT,
                    ShareAcknowledgementBatch::RELEASE,
                    ShareAcknowledgementBatch::REJECT,
                ]),
                ShareAcknowledgementBatch::of(3, 5, ShareAcknowledgementBatch::RELEASE),
            ]]],
            self::MAX_WAIT_MS
        );

        $partition = $answer->partitionOf($topicId, 0);
        self::assertNotNull($partition);
        self::assertSame(KafkaException::NO_ERROR, $partition->acknowledgeErrorCode);
        self::assertSame(
            [1 => 2, 3 => 2, 4 => 2, 5 => 2],
            array_map(static fn(array $acquired): int => $acquired['deliveryCount'], $partition->acquiredRecords()),
            'the released 1 and 3-5 are acquired again with the delivery count 2, the accepted 0 and rejected 2 not'
        );
    }

    /**
     * ShareAcknowledge accepts in the session; the second acknowledgement of a record is the 121 of its partition
     */
    public function testAcknowledgingARecordTwiceIsTheInvalidRecordStateOfItsPartition(): void
    {
        [$groupId, $topicId, $leader] = $this->shareGroup(['v0', 'v1', 'v2', 'v3', 'v4', 'v5']);
        $member = $this->joinedMember($groupId, $topicId);
        [, $epoch] = $this->fetchUntilAcquired($leader, $groupId, $member, $topicId);
        $acceptThree = [$topicId => [0 => [ShareAcknowledgementBatch::of(3, 3, ShareAcknowledgementBatch::ACCEPT)]]];

        $first = $this->client()->shareAcknowledge($leader, $groupId, $member, $epoch++, $acceptThree);
        self::assertSame(KafkaException::NO_ERROR, $first->errorCode);
        self::assertSame(KafkaException::NO_ERROR, $first->partitionOf($topicId, 0)?->errorCode);

        $second    = $this->client()->shareAcknowledge($leader, $groupId, $member, $epoch++, $acceptThree);
        $partition = $second->partitionOf($topicId, 0);
        self::assertSame(KafkaException::NO_ERROR, $second->errorCode, 'the request itself is fine');
        self::assertNotNull($partition);
        self::assertSame(KafkaException::INVALID_RECORD_STATE, $partition->errorCode);
        self::assertSame('The offset cannot be acknowledged. The offset is not acquired.', $partition->errorMessage);

        // the same acknowledgement piggybacked on a ShareFetch is its acknowledge_error_code
        $fetch     = $this->client()->shareFetch($leader, $groupId, $member, $epoch++, [$topicId => [0]], $acceptThree, self::MAX_WAIT_MS);
        $partition = $fetch->partitionOf($topicId, 0);
        self::assertNotNull($partition);
        self::assertSame(KafkaException::NO_ERROR, $partition->errorCode);
        self::assertSame(KafkaException::INVALID_RECORD_STATE, $partition->acknowledgeErrorCode);

        // and an offset past everything acquired is the 42 of the partition
        $past      = $this->client()->shareAcknowledge($leader, $groupId, $member, $epoch++, [
            $topicId => [0 => [ShareAcknowledgementBatch::of(9, 9, ShareAcknowledgementBatch::ACCEPT)]],
        ]);
        $partition = $past->partitionOf($topicId, 0);
        self::assertNotNull($partition);
        self::assertSame(KafkaException::INVALID_REQUEST, $partition->errorCode);
        self::assertSame(
            'Batch record not found. The first offset in request is past acquired records.',
            $partition->errorMessage
        );
    }

    /**
     * A member acknowledging a record another member acquired is the 121 as well
     */
    public function testAMemberCannotAcknowledgeTheRecordsOfAnother(): void
    {
        [$groupId, $topicId, $leader] = $this->shareGroup(['v0', 'v1', 'v2']);
        $owner = $this->joinedMember($groupId, $topicId);
        $this->fetchUntilAcquired($leader, $groupId, $owner, $topicId);

        $other = $this->joinedMember($groupId, $topicId);
        $empty = $this->client()->shareFetch($leader, $groupId, $other, ShareFetchRequest::INITIAL_EPOCH, [$topicId => [0]], [], self::MAX_WAIT_MS);
        self::assertSame([], $empty->partitionOf($topicId, 0)?->acquiredRecords, 'the owner holds all of them');

        $answer    = $this->client()->shareAcknowledge($leader, $groupId, $other, 1, [
            $topicId => [0 => [ShareAcknowledgementBatch::of(1, 1, ShareAcknowledgementBatch::ACCEPT)]],
        ]);
        $partition = $answer->partitionOf($topicId, 0);
        self::assertNotNull($partition);
        self::assertSame(KafkaException::INVALID_RECORD_STATE, $partition->errorCode);
        self::assertSame('Member is not the owner of batch record', $partition->errorMessage);
    }

    /**
     * Closing the session (epoch -1) releases what the member holds: another member acquires it, delivery count 2
     */
    public function testClosingTheSessionReleasesTheRecordsToAnotherMember(): void
    {
        [$groupId, $topicId, $leader] = $this->shareGroup(['v0', 'v1', 'v2']);
        $first = $this->joinedMember($groupId, $topicId);
        $this->fetchUntilAcquired($leader, $groupId, $first, $topicId);

        $closed = $this->client()->shareAcknowledge($leader, $groupId, $first, ShareFetchRequest::FINAL_EPOCH);
        self::assertSame(KafkaException::NO_ERROR, $closed->errorCode);
        self::assertSame([], $closed->responses, 'a close that acknowledges nothing names no partition');

        $second = $this->joinedMember($groupId, $topicId);
        [$answer] = $this->fetchUntilAcquired($leader, $groupId, $second, $topicId);

        self::assertSame(
            [0 => 2, 1 => 2, 2 => 2],
            array_map(
                static fn(array $acquired): int => $acquired['deliveryCount'],
                $answer->partitionOf($topicId, 0)?->acquiredRecords() ?? []
            )
        );
    }

    /**
     * The share session: 42 for acknowledgements that open it, 123 for a wrong epoch, 122 once it is closed
     */
    public function testTheShareSessionRefusesAWrongEpochAndIsGoneOnceClosed(): void
    {
        [$groupId, $topicId, $leader] = $this->shareGroup(['v0']);
        $member = $this->joinedMember($groupId, $topicId);
        $accept = [$topicId => [0 => [ShareAcknowledgementBatch::of(0, 0, ShareAcknowledgementBatch::ACCEPT)]]];

        $this->expectExceptionOnce(InvalidRequestException::class, fn() => $this->client()->shareFetch(
            $leader,
            $groupId,
            $member,
            ShareFetchRequest::INITIAL_EPOCH,
            [$topicId => [0]],
            $accept,
            self::MAX_WAIT_MS
        ));

        [, $epoch] = $this->fetchUntilAcquired($leader, $groupId, $member, $topicId);

        $this->expectExceptionOnce(InvalidShareSessionEpochException::class, fn() => $this->client()->shareFetch(
            $leader,
            $groupId,
            $member,
            $epoch + 5,
            [$topicId => [0]],
            [],
            self::MAX_WAIT_MS
        ));
        $this->expectExceptionOnce(
            InvalidShareSessionEpochException::class,
            fn() => $this->client()->shareAcknowledge($leader, $groupId, $member, ShareFetchRequest::INITIAL_EPOCH, $accept)
        );

        // the refused epochs did not move the session: the expected one still works
        $accepted = $this->client()->shareAcknowledge($leader, $groupId, $member, $epoch++, $accept);
        self::assertSame(KafkaException::NO_ERROR, $accepted->partitionOf($topicId, 0)?->errorCode);

        $this->client()->shareAcknowledge($leader, $groupId, $member, ShareFetchRequest::FINAL_EPOCH);

        $this->expectExceptionOnce(ShareSessionNotFoundException::class, fn() => $this->client()->shareFetch(
            $leader,
            $groupId,
            $member,
            $epoch,
            [$topicId => [0]],
            [],
            self::MAX_WAIT_MS
        ));
        $this->expectExceptionOnce(
            ShareSessionNotFoundException::class,
            fn() => $this->client()->shareAcknowledge($leader, $groupId, $member, $epoch, $accept)
        );
    }

    /**
     * Without `share.auto.offset.reset` a fresh group reads from `latest`: what was there before is never delivered
     */
    public function testAFreshGroupReadsFromLatestByDefault(): void
    {
        [$groupId, $topicId, $leader, $topic] = $this->shareGroup(['old-0', 'old-1', 'old-2'], null);
        $member = $this->joinedMember($groupId, $topicId);

        $opened = $this->client()->shareFetch($leader, $groupId, $member, ShareFetchRequest::INITIAL_EPOCH, [$topicId => [0]], [], self::MAX_WAIT_MS);
        self::assertSame([], $opened->partitionOf($topicId, 0)?->acquiredRecords ?? [], 'nothing of what was there');

        $this->produce($topic, ['new-3', 'new-4']);
        [$answer] = $this->fetchUntilAcquired($leader, $groupId, $member, $topicId, 1);

        self::assertSame(
            [3 => 'new-3', 4 => 'new-4'],
            array_map(
                static fn(array $acquired): ?string => $acquired['record']->value,
                $answer->partitionOf($topicId, 0)?->acquiredRecords() ?? []
            )
        );
    }

    /**
     * Creates a topic of one partition with the given records and a group id for it, configured as asked
     *
     * @param list<string> $values          Records to produce before anybody joins
     * @param string|null  $autoOffsetReset `share.auto.offset.reset` of the group, null for the default
     *
     * @return array{string, string, Node, string} Group id, raw topic id, leader of the partition, topic name
     */
    private function shareGroup(array $values, ?string $autoOffsetReset = 'earliest'): array
    {
        $topic = self::uniqueTopicName(self::TOPIC_PREFIX);
        new AdminClient($this->cluster(), $this->configuration())->createTopics([new NewTopic($topic, 1, 1)]);
        new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, self::TOPIC_PREFIX)
            ->awaitTopicWithLeaders($topic);
        $this->produce($topic, $values);

        $groupId        = 't3-41-sf-group-' . bin2hex(random_bytes(6));
        self::$groups[] = $groupId;
        if ($autoOffsetReset !== null) {
            $this->setGroupConfig($groupId, 'share.auto.offset.reset', $autoOffsetReset);
        }

        $this->cluster()->reload([$topic]);

        return [$groupId, self::topicIdOf($topic), $this->cluster()->leaderFor($topic, 0), $topic];
    }

    /**
     * Joins a member and heartbeats until the one partition of the topic is assigned to it
     */
    private function joinedMember(string $groupId, string $topicId): string
    {
        $member = ConsumerGroupHeartbeatCoordinator::newMemberId();
        $topic  = $this->cluster()->topicNameById($topicId);
        self::assertNotNull($topic);

        $node   = $this->coordinator($groupId);
        $answer = $this->client()->joinShareGroup($node, $groupId, $member, [$topic]);
        self::$members[] = [$groupId, $member];

        $deadline = microtime(true) + self::WAIT_TIMEOUT;
        while (($answer->assignment?->partitionsByTopicId()[$topicId] ?? []) === [] && microtime(true) < $deadline) {
            usleep(250000);
            $answer = $this->client()->shareGroupHeartbeat($node, $groupId, $member, $answer->memberEpoch);
        }
        self::assertSame([0], $answer->assignment?->partitionsByTopicId()[$topicId] ?? [], 'the partition is assigned');

        return $member;
    }

    /**
     * Fetches in one share session until records are acquired, opening it unless an epoch is given
     *
     * @return array{ShareFetchResponse, int} The answer that acquired records, and the next epoch of the session
     */
    private function fetchUntilAcquired(
        Node $leader,
        string $groupId,
        string $member,
        string $topicId,
        int $epoch = ShareFetchRequest::INITIAL_EPOCH
    ): array {
        $deadline = microtime(true) + self::WAIT_TIMEOUT;
        while (true) {
            $answer = $this->client()->shareFetch($leader, $groupId, $member, $epoch, [$topicId => [0]], [], self::MAX_WAIT_MS);
            $epoch++;
            if (($answer->partitionOf($topicId, 0)?->acquiredRecords ?? []) !== [] || microtime(true) >= $deadline) {
                return [$answer, $epoch];
            }
        }
    }

    /**
     * @param list<string> $values
     */
    private function produce(string $topic, array $values): void
    {
        $records = [];
        foreach ($values as $value) {
            $records[] = new Record($value, null, 0, null, (int) (microtime(true) * 1000));
        }

        $stream = $this->connect();
        new ProduceRequestV12([$topic => [0 => RecordBatch::fromRecords($records)]], 1, 10000, self::CLIENT_ID)
            ->writeTo($stream);

        $errorCode = ProduceResponseV12::unpack($stream)->topics[$topic]->partitions[0]->errorCode;
        if ($errorCode !== KafkaException::NO_ERROR) {
            throw KafkaException::fromCode($errorCode, ['topic' => $topic]);
        }
    }

    /**
     * Sets a group config (KIP-848 and KIP-932) through IncrementalAlterConfigs and the config resource type 32
     */
    private function setGroupConfig(string $groupId, string $name, string $value): void
    {
        $stream = $this->connect();
        new IncrementalAlterConfigsRequest(
            [new IncrementalAlterConfigsRequestResource(self::GROUP_CONFIG_RESOURCE, $groupId, [AlterConfigOp::set($name, $value)])],
            false,
            self::CLIENT_ID
        )->writeTo($stream);

        $answer = IncrementalAlterConfigsResponse::unpack($stream);
        self::assertSame(KafkaException::NO_ERROR, $answer->responses[0]->errorCode ?? null, "{$name} of {$groupId}");
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
     * Closes the share session of a member of this class and takes it out of its group, both with the epoch -1
     */
    private static function leaveQuietly(string $groupId, string $memberId): void
    {
        $configuration = self::cleanupConfiguration();

        try {
            $cluster = Cluster::bootstrap($configuration);
            $client  = new Client($cluster, $configuration);
            foreach ($cluster->nodes() as $node) {
                try {
                    $client->shareAcknowledge($node, $groupId, $memberId, ShareFetchRequest::FINAL_EPOCH);
                } catch (KafkaException) {
                    // No session of the member on that node: its connection is gone, and the session with it
                }
            }
            $client->leaveShareGroup(
                new CoordinatorLookup($cluster, $configuration)->findCoordinator($groupId),
                $groupId,
                $memberId
            );
        } catch (KafkaException) {
            // A member that left in its test, or a group that is gone, must not fail the suite
        }
    }

    private static function deleteGroupQuietly(string $groupId): void
    {
        try {
            $configuration = self::cleanupConfiguration();

            new AdminClient(Cluster::bootstrap($configuration), $configuration)->deleteConsumerGroups([$groupId]);
        } catch (KafkaException) {
            // A group that is gone already must not fail the suite
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
