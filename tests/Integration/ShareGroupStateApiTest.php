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
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Common\Security\SaslMechanism;
use Protocol\Kafka\Common\Security\SecurityProtocol;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\Internals\ConsumerGroupHeartbeatCoordinator;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Data\DeleteShareGroupStateRequestTopic;
use Protocol\Kafka\Protocol\Data\IncrementalAlterConfigsRequestResource;
use Protocol\Kafka\Protocol\Data\InitializeShareGroupStateRequestTopic;
use Protocol\Kafka\Protocol\Data\ReadShareGroupStateRequestPartition;
use Protocol\Kafka\Protocol\Data\ReadShareGroupStateRequestTopic;
use Protocol\Kafka\Protocol\Data\ReadShareGroupStateSummaryResponsePartition;
use Protocol\Kafka\Protocol\Data\ShareAcknowledgementBatch;
use Protocol\Kafka\Protocol\Data\ShareGroupStateBatch;
use Protocol\Kafka\Protocol\Data\WriteShareGroupStateRequestPartition;
use Protocol\Kafka\Protocol\Data\WriteShareGroupStateRequestTopic;
use Protocol\Kafka\Protocol\Request\DeleteShareGroupStateRequest;
use Protocol\Kafka\Protocol\Request\DeleteShareGroupStateResponse;
use Protocol\Kafka\Protocol\Request\IncrementalAlterConfigsRequest;
use Protocol\Kafka\Protocol\Request\IncrementalAlterConfigsResponse;
use Protocol\Kafka\Protocol\Request\InitializeShareGroupStateRequest;
use Protocol\Kafka\Protocol\Request\InitializeShareGroupStateResponse;
use Protocol\Kafka\Protocol\Request\ProduceRequestV12;
use Protocol\Kafka\Protocol\Request\ProduceResponseV12;
use Protocol\Kafka\Protocol\Request\ReadShareGroupStateRequest;
use Protocol\Kafka\Protocol\Request\ReadShareGroupStateResponse;
use Protocol\Kafka\Protocol\Request\ReadShareGroupStateSummaryRequest;
use Protocol\Kafka\Protocol\Request\ReadShareGroupStateSummaryRequestV0;
use Protocol\Kafka\Protocol\Request\ReadShareGroupStateSummaryResponse;
use Protocol\Kafka\Protocol\Request\ReadShareGroupStateSummaryResponseV0;
use Protocol\Kafka\Protocol\Request\ShareFetchRequest;
use Protocol\Kafka\Protocol\Request\WriteShareGroupStateRequest;
use Protocol\Kafka\Protocol\Request\WriteShareGroupStateResponse;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * Probes the five share-group state apis of KIP-932 (keys 83 to 87) on the 4.3.1 node: their v0 of Kafka 4.1, and
 * the v1 of WriteShareGroupState and ReadShareGroupStateSummary that Kafka 4.2 added (KIP-1226).
 *
 * They are the apis of the **share coordinator**, which a partition leader and a group coordinator send it, and
 * they are **wire only** on this line. This class therefore writes no state through them: the three writing apis
 * (83, 85 and 86) carry a topic without a partition, which the share coordinator answers with an empty result
 * before it looks up a key, or - the write of v1 - a partition of a share group that does not exist, which it
 * refuses with the 42; and the two reading apis (84 and 87) name the one partition of this class's own topic,
 * with the leader epoch -1, which a read never writes back. The one share group this class creates is its own
 * (`t1-42-share-state-group-…`): the traffic of a real ShareFetch and ShareAcknowledge, whose summary carries the
 * `DeliveryCompleteCount` of KIP-1226. Its member leaves and the group is deleted in
 * {@see self::tearDownAfterClass()}.
 *
 * @see docs/protocol/4.3.md, section "The share-group state apis (keys 83 to 87) — wire only"
 */
#[CoversClass(InitializeShareGroupStateRequest::class)]
#[CoversClass(InitializeShareGroupStateResponse::class)]
#[CoversClass(ReadShareGroupStateRequest::class)]
#[CoversClass(ReadShareGroupStateResponse::class)]
#[CoversClass(WriteShareGroupStateRequest::class)]
#[CoversClass(WriteShareGroupStateResponse::class)]
#[CoversClass(DeleteShareGroupStateRequest::class)]
#[CoversClass(DeleteShareGroupStateResponse::class)]
#[CoversClass(ReadShareGroupStateSummaryRequest::class)]
#[CoversClass(ReadShareGroupStateSummaryResponse::class)]
#[CoversClass(ReadShareGroupStateSummaryRequestV0::class)]
#[CoversClass(ReadShareGroupStateSummaryResponseV0::class)]
final class ShareGroupStateApiTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t1-41-share-state';

    /**
     * A share group no suite of this repository creates
     */
    private const string ABSENT_GROUP = 't1-41-no-such-share-group';

    /**
     * The group config resource of KIP-848 and KIP-932 (`ConfigResource.Type.GROUP`, id 32)
     */
    private const int GROUP_CONFIG_RESOURCE = 32;

    /**
     * How long the traffic test waits for an assignment, records or a written state, in seconds
     */
    private const float WAIT_TIMEOUT = 20.0;

    private static ?string $topicId = null;

    /**
     * The name of the topic of this class
     */
    private static ?string $topic = null;

    /**
     * Every share group this class created
     *
     * @var list<string>
     */
    private static array $groups = [];

    /**
     * Every member this class joined to a group, as `[group id, member id]`
     *
     * @var list<array{string, string}>
     */
    private static array $members = [];

    protected function setUp(): void
    {
        parent::setUp();

        if (self::$topicId === null) {
            $configuration = [
                ClientConfig::BOOTSTRAP_SERVERS  => ['tcp://' . self::firstBootstrapServer()],
                ClientConfig::CLIENT_ID          => self::CLIENT_ID,
                ClientConfig::REQUEST_TIMEOUT_MS => 30000,
            ];
            $topic = self::uniqueTopicName('t1-41-share-state');
            new AdminClient(Cluster::bootstrap($configuration), $configuration)
                ->createTopics([new NewTopic($topic, 1, 1)]);
            // A KRaft node publishes a fresh topic before the broker has applied it, and the share coordinator
            // answers a read of it with the 3 in the meantime (seen once under the load of a full gate): wait until
            // the leader of the partition answers a ListOffsets
            new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, self::CLIENT_ID)
                ->awaitTopicWithLeaders($topic);
            self::$topicId = self::topicIdOf($topic);
            self::$topic   = $topic;
        }
    }

    public static function tearDownAfterClass(): void
    {
        foreach (self::$members as [$groupId, $memberId]) {
            self::leaveQuietly($groupId, $memberId);
        }
        self::$members = [];
        foreach (self::$groups as $groupId) {
            try {
                $configuration = self::cleanupConfiguration();
                new AdminClient(Cluster::bootstrap($configuration), $configuration)->deleteConsumerGroups([$groupId]);
            } catch (KafkaException) {
                // A group that is gone already must not fail the suite
            }
        }
        self::$groups  = [];
        self::$topicId = null;
        self::$topic   = null;

        parent::tearDownAfterClass();
    }

    /**
     * A partition that was never initialized cannot be read: the 42 of the share coordinator, and nothing written
     */
    public function testAReadOfAnUninitializedSharePartitionIsInvalidRequest(): void
    {
        $stream = $this->connect();
        new ReadShareGroupStateRequest(self::ABSENT_GROUP, $this->onePartition(), self::CLIENT_ID, 4801)
            ->writeTo($stream);
        $answer = ReadShareGroupStateResponse::unpack($stream);

        self::assertCount(1, $answer->results);
        self::assertSame(self::$topicId, $answer->results[0]->topicId);

        $partition = $answer->results[0]->partitions[0];

        self::assertSame(KafkaException::INVALID_REQUEST, $partition->errorCode);
        self::assertSame('Read operation on uninitialized share partition not allowed.', $partition->errorMessage);
        self::assertSame(0, $partition->stateEpoch);
        self::assertSame(0, $partition->startOffset, 'the default of the field, not the -1 of "not initialized"');
        self::assertSame([], $partition->stateBatches);
    }

    /**
     * The summary of the same partition is no error: the state of a partition the coordinator has never seen
     */
    public function testTheSummaryOfAnUninitializedSharePartitionIsItsInitialState(): void
    {
        $stream = $this->connect();
        new ReadShareGroupStateSummaryRequest(self::ABSENT_GROUP, $this->onePartition(), self::CLIENT_ID, 4802)
            ->writeTo($stream);
        $answer = ReadShareGroupStateSummaryResponse::unpack($stream);

        $partition = $answer->results[0]->partitions[0];

        self::assertSame(KafkaException::NO_ERROR, $partition->errorCode);
        self::assertNull($partition->errorMessage);
        self::assertSame(-1, $partition->startOffset, '`PartitionFactory.UNINITIALIZED_START_OFFSET` @ 4.3.1');
        self::assertSame(0, $partition->stateEpoch);
        self::assertSame(0, $partition->leaderEpoch);
        self::assertSame(-1, $partition->deliveryCompleteCount, 'v1 (KIP-1226): `UNINITIALIZED_DELIVERY_COMPLETE_COUNT`');

        new ReadShareGroupStateSummaryRequest('', $this->onePartition(), self::CLIENT_ID, 4803)->writeTo($stream);

        self::assertSame(
            [],
            ReadShareGroupStateSummaryResponse::unpack($stream)->results,
            'an empty group id is answered with nothing at all'
        );
    }

    /**
     * The three writing apis answer a topic without a partition with an empty result, before any key is looked at
     */
    public function testTheWritingApisAnswerATopicWithoutAPartitionWithNothing(): void
    {
        $stream  = $this->connect();
        $topicId = (string) self::$topicId;

        new InitializeShareGroupStateRequest(
            self::ABSENT_GROUP,
            [new InitializeShareGroupStateRequestTopic($topicId)],
            self::CLIENT_ID,
            4804
        )->writeTo($stream);
        $initialized = InitializeShareGroupStateResponse::unpack($stream);

        new WriteShareGroupStateRequest(
            self::ABSENT_GROUP,
            [new WriteShareGroupStateRequestTopic($topicId)],
            self::CLIENT_ID,
            4805
        )->writeTo($stream);
        $written = WriteShareGroupStateResponse::unpack($stream);

        new DeleteShareGroupStateRequest(
            self::ABSENT_GROUP,
            [new DeleteShareGroupStateRequestTopic($topicId)],
            self::CLIENT_ID,
            4806
        )->writeTo($stream);
        $deleted = DeleteShareGroupStateResponse::unpack($stream);

        foreach ([$initialized, $written, $deleted] as $answer) {
            self::assertSame([], $answer->results);
            self::assertSame(
                7,
                $answer->getMessageSize(),
                'the correlation id, the tag buffer of the header, the empty compact array and the tag buffer'
            );
        }
    }

    /**
     * The keep-behind v0 of the summary is answered without the count, which keeps the -1 of its default
     */
    public function testTheSummaryOfVersionZeroCarriesNoCount(): void
    {
        $stream = $this->connect();
        new ReadShareGroupStateSummaryRequestV0(self::ABSENT_GROUP, $this->onePartition(), self::CLIENT_ID, 4808)
            ->writeTo($stream);
        $answer = ReadShareGroupStateSummaryResponseV0::unpack($stream);

        $partition = $answer->results[0]->partitions[0];

        self::assertSame(KafkaException::NO_ERROR, $partition->errorCode);
        self::assertSame(-1, $partition->startOffset);
        self::assertSame(
            49,
            $answer->getMessageSize(),
            'the frame of v0 ends the partition with the start offset: four bytes shorter than the 53 of v1'
        );
    }

    /**
     * A write of v1 (KIP-1226) of a partition the share coordinator has no state for is the 42, and writes nothing
     *
     * The partition carries the `DeliveryCompleteCount` of Kafka 4.2 between its start offset and its batches; the
     * coordinator reads it and refuses the key before it would append a record - a share partition is initialized
     * by its group coordinator (83) first. The summary of the same key afterwards is still the initial state.
     */
    public function testAWriteOfVersionOneOfAnUninitializedSharePartitionIsInvalidRequest(): void
    {
        $stream = $this->connect();
        new WriteShareGroupStateRequest(
            self::ABSENT_GROUP,
            [new WriteShareGroupStateRequestTopic((string) self::$topicId, [
                new WriteShareGroupStateRequestPartition(0, 0, 0, 0, [new ShareGroupStateBatch(0, 1, 2, 1)], 2),
            ])],
            self::CLIENT_ID,
            4809
        )->writeTo($stream);
        $answer = WriteShareGroupStateResponse::unpack($stream);

        $partition = $answer->results[0]->partitions[0];

        self::assertSame(self::$topicId, $answer->results[0]->topicId);
        self::assertSame(KafkaException::INVALID_REQUEST, $partition->errorCode);
        self::assertSame('Write operation on uninitialized share partition not allowed.', $partition->errorMessage);

        $summary = $this->summaryOf(self::ABSENT_GROUP);

        self::assertSame([-1, -1], [$summary->startOffset, $summary->deliveryCompleteCount], 'nothing was written');
    }

    /**
     * The summary of v1 counts the records whose delivery is complete, after a real ShareFetch and acknowledgement
     *
     * Six records, a share group of this class that reads from `earliest`, one member that acquires the six and
     * acknowledges them: the offset 1 accepted, the offset 2 rejected, the offsets 0 and 3 to 5 released. The
     * partition leader writes the state with its `DeliveryCompleteCount` (WriteShareGroupState v1), and the summary
     * answers the start offset **0** - the released offset 0 is available again - and the count **2**, the accepted
     * and the archived record. Before the first ShareFetch the summary of the same key is the start offset -1 and
     * the count -1 of a share partition no consumer has read yet.
     */
    public function testTheSummaryOfVersionOneCountsTheRecordsWhoseDeliveryIsComplete(): void
    {
        $topicId = (string) self::$topicId;
        $topic   = (string) self::$topic;
        $this->produce($topic, ['v0', 'v1', 'v2', 'v3', 'v4', 'v5']);

        $groupId        = 't1-42-share-state-group-' . bin2hex(random_bytes(6));
        self::$groups[] = $groupId;
        $this->setGroupConfig($groupId, 'share.auto.offset.reset', 'earliest');

        $configuration = self::cleanupConfiguration();
        $cluster       = Cluster::bootstrap($configuration);
        $cluster->reload([$topic]);
        $client      = new Client($cluster, $configuration);
        $coordinator = new CoordinatorLookup($cluster, $configuration)->findCoordinator($groupId);
        $leader      = $cluster->leaderFor($topic, 0);
        $member      = ConsumerGroupHeartbeatCoordinator::newMemberId();

        $heartbeat       = $client->joinShareGroup($coordinator, $groupId, $member, [$topic]);
        self::$members[] = [$groupId, $member];
        $deadline        = microtime(true) + self::WAIT_TIMEOUT;
        while (($heartbeat->assignment?->partitionsByTopicId()[$topicId] ?? []) === [] && microtime(true) < $deadline) {
            usleep(250000);
            $heartbeat = $client->shareGroupHeartbeat($coordinator, $groupId, $member, $heartbeat->memberEpoch);
        }
        self::assertSame([0], $heartbeat->assignment?->partitionsByTopicId()[$topicId] ?? [], 'the partition is assigned');

        $initial = $this->summaryOf($groupId);
        self::assertSame([-1, -1], [$initial->startOffset, $initial->deliveryCompleteCount], 'nobody has read yet');

        $epoch    = ShareFetchRequest::INITIAL_EPOCH;
        $deadline = microtime(true) + self::WAIT_TIMEOUT;
        do {
            $fetched  = $client->shareFetch($leader, $groupId, $member, $epoch++, [$topicId => [0]], [], 500);
            $acquired = $fetched->partitionOf($topicId, 0)?->acquiredRecords ?? [];
        } while ($acquired === [] && microtime(true) < $deadline);
        self::assertSame(
            [[0, 5]],
            array_map(static fn($range): array => [$range->firstOffset, $range->lastOffset], $acquired),
            'the six records in one range'
        );

        $acknowledged = $client->shareAcknowledge($leader, $groupId, $member, $epoch, [$topicId => [0 => [
            ShareAcknowledgementBatch::of(0, 0, ShareAcknowledgementBatch::RELEASE),
            new ShareAcknowledgementBatch(1, 2, [ShareAcknowledgementBatch::ACCEPT, ShareAcknowledgementBatch::REJECT]),
            ShareAcknowledgementBatch::of(3, 5, ShareAcknowledgementBatch::RELEASE),
        ]]]);
        self::assertSame(KafkaException::NO_ERROR, $acknowledged->errorCode);

        // The partition leader writes the state on its own time: poll the read-back
        $deadline = microtime(true) + self::WAIT_TIMEOUT;
        $summary  = $this->summaryOf($groupId);
        while ($summary->deliveryCompleteCount !== 2 && microtime(true) < $deadline) {
            usleep(250000);
            $summary = $this->summaryOf($groupId);
        }

        self::assertSame(KafkaException::NO_ERROR, $summary->errorCode);
        self::assertSame(0, $summary->startOffset, 'the released offset 0 is available again');
        self::assertSame(2, $summary->deliveryCompleteCount, 'the accepted offset 1 and the rejected offset 2');
    }

    /**
     * A principal that may not act as a broker is refused per partition with the 31, and the message says so
     */
    public function testAPrincipalWithoutClusterActionIsRefusedPerPartition(): void
    {
        self::assertNotSame('', self::saslBootstrapServer(), 'the SASL_PLAINTEXT listener of the node');

        $configuration = [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::saslBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ClientConfig::REQUEST_TIMEOUT_MS        => 30000,
            ClientConfig::SECURITY_PROTOCOL         => SecurityProtocol::SASL_PLAINTEXT,
            ClientConfig::SASL_MECHANISM            => SaslMechanism::PLAIN,
            ClientConfig::SASL_USERNAME             => 'acltest',
            ClientConfig::SASL_PASSWORD             => 'acltest-secret',
        ];
        $nodes  = Cluster::bootstrap($configuration)->nodes();
        $stream = reset($nodes)->getConnection($configuration);
        new ReadShareGroupStateRequest(self::ABSENT_GROUP, $this->onePartition(), self::CLIENT_ID, 4807)
            ->writeTo($stream);
        $partition = ReadShareGroupStateResponse::unpack($stream)->results[0]->partitions[0];

        self::assertSame(KafkaException::CLUSTER_AUTHORIZATION_FAILED, $partition->errorCode);
        self::assertSame('Cluster authorization failed.', $partition->errorMessage);
    }

    /**
     * The summary of the partition 0 of the topic of this class for a share group, at version 1
     */
    private function summaryOf(string $groupId): ReadShareGroupStateSummaryResponsePartition
    {
        $stream = $this->connect();
        new ReadShareGroupStateSummaryRequest($groupId, $this->onePartition(), self::CLIENT_ID, 4811)->writeTo($stream);

        return ReadShareGroupStateSummaryResponse::unpack($stream)->results[0]->partitions[0];
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
        self::assertSame(KafkaException::NO_ERROR, $errorCode, "the records of {$topic}");
    }

    /**
     * Sets a group config (KIP-932) through IncrementalAlterConfigs and the config resource type 32
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
     * Closes the share sessions of a member of this class and takes it out of its group, both with the epoch -1
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
            // A member that is gone already must not fail the suite
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
            ClientConfig::RETRY_BACKOFF_MS          => 250,
            ClientConfig::RETRIES                   => 2,
            ClientConfig::REQUEST_TIMEOUT_MS        => 30000,
        ] + ConsumerConfig::getDefaultConfiguration();
    }

    /**
     * The partition 0 of the topic of this class, with the leader epoch -1 that a read never writes back
     *
     * @return list<ReadShareGroupStateRequestTopic>
     */
    private function onePartition(): array
    {
        return [
            new ReadShareGroupStateRequestTopic(
                (string) self::$topicId,
                [new ReadShareGroupStateRequestPartition(0, -1)]
            ),
        ];
    }
}
