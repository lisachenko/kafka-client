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
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\NetworkException;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Common\Record\MemoryRecords;
use Protocol\Kafka\Consumer\OffsetAndMetadata;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\GroupCoordinatorResponseMetadata;
use Protocol\Kafka\Protocol\Data\OffsetCommitRequestPartition;
use Protocol\Kafka\Protocol\Data\OffsetCommitRequestPartitionV1;
use Protocol\Kafka\Protocol\Data\OffsetCommitRequestTopic;
use Protocol\Kafka\Protocol\Data\OffsetCommitRequestTopicV1;
use Protocol\Kafka\Protocol\Data\OffsetFetchResponsePartition;
use Protocol\Kafka\Protocol\Data\OffsetFetchResponseTopic;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\FetchResponse;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorRequest;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorResponse;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\MetadataResponse;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequest;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequestV0;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequestV1;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequestV4;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequestV5;
use Protocol\Kafka\Protocol\Request\OffsetCommitResponse;
use Protocol\Kafka\Protocol\Request\OffsetCommitResponseV0;
use Protocol\Kafka\Protocol\Request\OffsetCommitResponseV1;
use Protocol\Kafka\Protocol\Request\OffsetCommitResponseV4;
use Protocol\Kafka\Protocol\Request\OffsetCommitResponseV5;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequest;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequestV0;
use Protocol\Kafka\Protocol\Request\OffsetFetchResponse;
use Protocol\Kafka\Protocol\Request\OffsetFetchResponseV0;
use Protocol\Kafka\Tests\Fixture\RawApiProbe;

/**
 * Verifies the GroupCoordinator, OffsetCommit and OffsetFetch APIs against a real Kafka 0.9.0.1 broker.
 *
 * The versions of the OffsetCommit API address two different storages: version 0 keeps the offsets in ZooKeeper as
 * Kafka 0.8.1 did, versions 1 and 2 keep them in the internal `__consumer_offsets` topic of the cluster. All of them
 * are exercised here, because version 0 and version 2 are reachable through the `offsets.storage` option of the
 * client and version 1 is the version a 0.8 broker expects.
 *
 * @see docs/protocol/2.8.md, sections "GroupCoordinator API (key 10, v0 to v3)",
 *      "OffsetCommit API (key 8, v0 to v8)" and "OffsetFetch API (key 9, v0 to v7)"
 */
#[CoversClass(Client::class)]
#[CoversClass(CoordinatorLookup::class)]
#[CoversClass(GroupCoordinatorRequest::class)]
#[CoversClass(GroupCoordinatorResponse::class)]
#[CoversClass(GroupCoordinatorResponseMetadata::class)]
#[CoversClass(OffsetCommitRequest::class)]
#[CoversClass(OffsetCommitRequestV0::class)]
#[CoversClass(OffsetCommitRequestV1::class)]
#[CoversClass(OffsetCommitRequestPartition::class)]
#[CoversClass(OffsetCommitRequestPartitionV1::class)]
#[CoversClass(OffsetCommitRequestTopic::class)]
#[CoversClass(OffsetCommitRequestTopicV1::class)]
#[CoversClass(OffsetCommitResponse::class)]
#[CoversClass(OffsetCommitResponseV0::class)]
#[CoversClass(OffsetCommitResponseV1::class)]
#[CoversClass(OffsetFetchRequest::class)]
#[CoversClass(OffsetFetchRequestV0::class)]
#[CoversClass(OffsetFetchResponse::class)]
#[CoversClass(OffsetFetchResponseV0::class)]
#[CoversClass(OffsetFetchResponseTopic::class)]
#[CoversClass(OffsetFetchResponsePartition::class)]
final class OffsetsCoordinatorTest extends IntegrationTestCase
{
    /**
     * `offset.metadata.max.bytes` of a 0.9.0.1 broker; a longer metadata string is answered with the error code 12
     */
    private const int OFFSET_METADATA_MAX_BYTES = 4096;

    /**
     * `offsets.topic.num.partitions` of the test broker: the partition of a group is one of these
     */
    private const int OFFSETS_TOPIC_PARTITIONS = 5;

    /**
     * Internal topic the coordinator appends every committed offset to
     */
    private const string OFFSETS_TOPIC = '__consumer_offsets';

    /**
     * The cluster is resolved once: every test of this class talks to the same brokers
     */
    private static ?Cluster $sharedCluster = null;

    public function testCoordinatorOfAGroupIsDiscovered(): void
    {
        $groupId = self::uniqueGroupName();

        $coordinator = new CoordinatorLookup($this->cluster(), $this->configuration())->findCoordinator($groupId);

        self::assertInstanceOf(Node::class, $coordinator);
        self::assertContains(
            $coordinator->host . ':' . $coordinator->port,
            array_map(static fn($broker): string => $broker->host . ':' . $broker->port, self::clusterBrokers())
        );
    }

    public function testCoordinatorLookupRetriesTheLazyCreationOfTheOffsetsTopic(): void
    {
        // A single request may still be answered with 15 while the broker creates __consumer_offsets, the retrying
        // lookup must not be: whatever the very first answer is, it ends up with a coordinator
        $groupId  = self::uniqueGroupName();
        $stream   = $this->connect();
        new GroupCoordinatorRequest(
            $groupId,
            GroupCoordinatorRequest::COORDINATOR_TYPE_GROUP,
            'kafka-client-t6',
            1
        )->writeTo($stream);
        $rawFirst = GroupCoordinatorResponse::unpack($stream);

        self::assertContains(
            $rawFirst->errorCode,
            [
                KafkaException::NO_ERROR,
                KafkaException::GROUP_COORDINATOR_NOT_AVAILABLE,
                KafkaException::GROUP_LOAD_IN_PROGRESS,
            ],
            'a bare coordinator request either succeeds or reports that the coordinator is not ready yet'
        );

        $coordinator = new CoordinatorLookup($this->cluster(), $this->configuration())->findCoordinator($groupId);

        self::assertGreaterThanOrEqual(0, $coordinator->nodeId);
    }

    public function testOffsetsCommittedInKafkaStorageAreFetchedBack(): void
    {
        $groupId = self::uniqueGroupName();
        $topic   = $this->createTopic();
        $stream  = $this->coordinatorStream($groupId);

        $this->commitInKafka($stream, $groupId, [$topic => [0 => 21, 1 => 42]]);

        $offsets = $this->fetchInKafka($stream, $groupId, [$topic => [0, 1]]);

        self::assertSame(21, $offsets[$topic]->partitions[0]->offset);
        self::assertSame(0, $offsets[$topic]->partitions[0]->errorCode);
        self::assertSame(42, $offsets[$topic]->partitions[1]->offset);
        self::assertSame(0, $offsets[$topic]->partitions[1]->errorCode);
    }

    /**
     * The nullable topic array of version 2 (Kafka 0.10.2, KIP-88): null is every topic, [] is none
     */
    public function testVersionTwoAnswersEveryCommittedTopicOfTheGroupForANullTopicArray(): void
    {
        $groupId = self::uniqueGroupName();
        $topic   = $this->createTopic();
        $stream  = $this->coordinatorStream($groupId);

        $this->commitInKafka($stream, $groupId, [$topic => [0 => 11, 2 => 33]]);

        $allTopics = $this->fetchInKafka($stream, $groupId, null);

        $partitions = array_keys($allTopics[$topic]->partitions);
        sort($partitions);

        self::assertSame([$topic], array_keys($allTopics), 'the answer names the topics the group committed');
        self::assertSame(
            [0, 2],
            $partitions,
            'and only the committed partitions - in the order of the internal map of the coordinator, which a '
            . '2.8.2 broker no longer walks in partition order'
        );
        self::assertSame(11, $allTopics[$topic]->partitions[0]->offset);
        self::assertSame(33, $allTopics[$topic]->partitions[2]->offset);
    }

    public function testAnEmptyTopicArrayOfVersionTwoIsNotTheNullOne(): void
    {
        $groupId = self::uniqueGroupName();
        $topic   = $this->createTopic();
        $stream  = $this->coordinatorStream($groupId);

        $this->commitInKafka($stream, $groupId, [$topic => [0 => 17]]);

        self::assertSame(
            [],
            $this->fetchInKafka($stream, $groupId, []),
            'an empty topic array names no topic at all, although the group has a committed offset'
        );
        self::assertNotSame([], $this->fetchInKafka($stream, $groupId, null), 'while a null array names all of them');
    }

    /**
     * A group the coordinator never heard of is not an error, it is a group without offsets
     *
     * `GroupCoordinator.handleFetchOffsets` @ 0.10.2.2 "returns offsets blindly regardless the current group state",
     * so an unknown group answers an empty topics array with the group-level error code 0. A client cannot tell an
     * unknown group from a group without offsets through this api - DescribeGroups is where the state of a group is.
     */
    public function testAnUnknownGroupAnswersNoTopicAndNoGroupError(): void
    {
        $groupId = self::uniqueGroupName();
        $stream  = $this->coordinatorStream($groupId);

        $response = $this->fetchInKafkaResponse($stream, $groupId, null);

        self::assertSame([], $response->topics);
        self::assertSame(KafkaException::NO_ERROR, $response->errorCode);
    }

    /**
     * The nullable topic array is version 2 only: the broker closes the connection on a `-1` array of version 1
     *
     * The request class of this client refuses to build that frame ({@see OffsetFetchRequest::__construct()}), so
     * the probe hand-builds it - and the broker answers the way it answers every frame it cannot parse on this line.
     */
    public function testVersionOneCanNotSendTheNullTopicArray(): void
    {
        $groupId = self::uniqueGroupName();
        $probe   = new RawApiProbe(self::firstBootstrapServer());

        try {
            $nullTopicArray = pack('n', strlen($groupId)) . $groupId . pack('N', -1);
            $versionOne     = $probe->send(ApiKeys::OFFSET_FETCH, 1, $nullTopicArray, 33);

            self::assertSame(
                RawApiProbe::CLOSED,
                $versionOne['status'],
                'a -1 topic array is a SchemaException for version 1, and a 0.10 broker closes the socket for it'
            );
        } finally {
            $probe->close();
        }
    }

    /**
     * The same frame with the version 2 in its header is a perfectly ordinary request
     */
    public function testTheSameFrameIsAcceptedByVersionTwo(): void
    {
        $groupId = self::uniqueGroupName();
        $probe   = new RawApiProbe(self::firstBootstrapServer());

        try {
            $nullTopicArray = pack('n', strlen($groupId)) . $groupId . pack('N', -1);
            $answer         = $probe->send(ApiKeys::OFFSET_FETCH, 2, $nullTopicArray, 34);

            self::assertSame(RawApiProbe::ANSWERED, $answer['status']);
            self::assertSame(34, $answer['correlationId']);
        } finally {
            $probe->close();
        }
    }

    public function testOffsetsCommittedInZooKeeperStorageAreFetchedBackFromAnyBroker(): void
    {
        $groupId = self::uniqueGroupName();
        $topic   = $this->createTopic();
        // Version 0 does not go through the coordinator at all: the bootstrap connection is enough
        $stream  = $this->connect();

        new OffsetCommitRequestV0($groupId, [$topic => [0 => 7]], 'kafka-client-t6', 11)->writeTo($stream);
        $commitResponse = OffsetCommitResponseV0::unpack($stream);

        self::assertSame(11, $commitResponse->getCorrelationId());
        self::assertSame(0, $commitResponse->topics[$topic]->partitions[0]->errorCode);

        new OffsetFetchRequestV0($groupId, [$topic => [0]], 'kafka-client-t6', 12)->writeTo($stream);
        $fetchResponse = OffsetFetchResponseV0::unpack($stream);

        self::assertSame(12, $fetchResponse->getCorrelationId());
        self::assertSame(7, $fetchResponse->topics[$topic]->partitions[0]->offset);
        self::assertSame(0, $fetchResponse->topics[$topic]->partitions[0]->errorCode);
    }

    public function testTheTwoStoragesKeepTheirOffsetsApart(): void
    {
        $groupId = self::uniqueGroupName();
        $topic   = $this->createTopic();
        $stream  = $this->coordinatorStream($groupId);

        $this->commitInKafka($stream, $groupId, [$topic => [0 => 1000]]);
        new OffsetCommitRequestV0($groupId, [$topic => [0 => 5]], 'kafka-client-t6', 21)->writeTo($stream);
        OffsetCommitResponseV0::unpack($stream);

        $fromKafka = $this->fetchInKafka($stream, $groupId, [$topic => [0]]);
        new OffsetFetchRequestV0($groupId, [$topic => [0]], 'kafka-client-t6', 22)->writeTo($stream);
        $fromZooKeeper = OffsetFetchResponseV0::unpack($stream)->topics;

        self::assertSame(1000, $fromKafka[$topic]->partitions[0]->offset);
        self::assertSame(5, $fromZooKeeper[$topic]->partitions[0]->offset);
    }

    public function testUncommittedPartitionIsReportedDifferentlyByTheTwoVersions(): void
    {
        $groupId = self::uniqueGroupName();
        $topic   = $this->createTopic();
        $stream  = $this->coordinatorStream($groupId);

        $fromKafka = $this->fetchInKafka($stream, $groupId, [$topic => [0]]);

        self::assertSame(-1, $fromKafka[$topic]->partitions[0]->offset);
        self::assertSame(
            KafkaException::NO_ERROR,
            $fromKafka[$topic]->partitions[0]->errorCode,
            'reading a partition of __consumer_offsets that was never written is not an error'
        );

        new OffsetFetchRequestV0($groupId, [$topic => [0]], 'kafka-client-t6', 31)->writeTo($stream);
        $fromZooKeeper = OffsetFetchResponseV0::unpack($stream)->topics;

        self::assertSame(-1, $fromZooKeeper[$topic]->partitions[0]->offset);
        self::assertSame(
            KafkaException::UNKNOWN_TOPIC_OR_PARTITION,
            $fromZooKeeper[$topic]->partitions[0]->errorCode,
            'a missing ZooKeeper node is reported as UnknownTopicOrPartition'
        );
    }

    /**
     * A partition that the cluster does not host is the one answer the two versions stopped sharing in Kafka 0.9.
     *
     * Version 1 of 0.8.2.2 filtered the requested topic-partitions against the metadata cache and reported the
     * unknown ones with the error code 3. Version 1 of 0.9.0.1 does not: KafkaApis hands the whole list to the
     * group coordinator and notes that "we do not need to filter the partitions in the metadata cache as the topic
     * partitions will be filtered in coordinator's offset manager through the offset cache" - and a partition the
     * offset cache does not know is simply an uncommitted one, i.e. the offset -1 with the error code 0. Version 0
     * still reads a ZooKeeper node that is not there and keeps reporting the error code 3.
     */
    public function testPartitionThatTheClusterDoesNotHostIsUncommittedToVersionOneAndUnknownToVersionZero(): void
    {
        $groupId          = self::uniqueGroupName();
        $topic            = $this->createTopic();
        $missingPartition = 4242;
        $stream           = $this->coordinatorStream($groupId);

        $fromKafka = $this->fetchInKafka($stream, $groupId, [$topic => [$missingPartition]]);

        // A 0.9.0.1 broker answers "never committed" here, where 0.8.2.2 answered 3 (UnknownTopicOrPartition):
        // `KafkaApis.handleOffsetFetchRequest` @ 0.9.0.1 hands the version 1 request straight to the coordinator
        // and no longer checks the metadata cache ("we do not need to filter the partitions in the metadata cache").
        self::assertSame(
            KafkaException::NO_ERROR,
            $fromKafka[$topic]->partitions[$missingPartition]->errorCode,
            'version 1 of a 0.9 broker does not check the requested partition against the metadata cache any more'
        );
        self::assertSame(-1, $fromKafka[$topic]->partitions[$missingPartition]->offset);

        new OffsetFetchRequestV0($groupId, [$topic => [$missingPartition]], 'kafka-client-t6', 32)
            ->writeTo($stream);
        $fromZooKeeper = OffsetFetchResponseV0::unpack($stream)->topics;

        self::assertSame(
            KafkaException::UNKNOWN_TOPIC_OR_PARTITION,
            $fromZooKeeper[$topic]->partitions[$missingPartition]->errorCode,
            'version 0 reads a ZooKeeper node that does not exist'
        );
        self::assertSame(-1, $fromZooKeeper[$topic]->partitions[$missingPartition]->offset);
    }

    public function testMetadataOfACommittedOffsetSurvivesTheRoundTrip(): void
    {
        $groupId  = self::uniqueGroupName();
        $topic    = $this->createTopic();
        $stream   = $this->coordinatorStream($groupId);
        $metadata = 'committed by ' . __FUNCTION__;

        $this->commitInKafka($stream, $groupId, [$topic => [0 => new OffsetAndMetadata(64, $metadata)]]);
        $offsets = $this->fetchInKafka($stream, $groupId, [$topic => [0]]);

        self::assertSame(64, $offsets[$topic]->partitions[0]->offset);
        self::assertSame($metadata, $offsets[$topic]->partitions[0]->metadata);
    }

    public function testOversizedMetadataIsRejectedWithOffsetMetadataTooLarge(): void
    {
        $groupId  = self::uniqueGroupName();
        $topic    = $this->createTopic();
        $stream   = $this->coordinatorStream($groupId);
        $metadata = str_repeat('m', self::OFFSET_METADATA_MAX_BYTES + 1);

        // The Kafka storage filters the entry out of the append to __consumer_offsets and reports it as an error.
        // A 0.9.0.1 broker filters it before the cache as well, so the offset stays uncommitted, see the protocol
        // document; only the error code is a contract of the protocol, so only the error code is asserted here.
        $inKafka = $this->commitInKafka($stream, $groupId, [$topic => [0 => new OffsetAndMetadata(1, $metadata)]], false);

        self::assertSame(
            KafkaException::OFFSET_METADATA_TOO_LARGE,
            $inKafka->topics[$topic]->partitions[0]->errorCode
        );

        // Version 0 checks the very same limit before it writes the offset to ZooKeeper
        new OffsetCommitRequestV0(
            $groupId,
            [$topic => [0 => new OffsetAndMetadata(1, $metadata)]],
            'kafka-client-t6',
            41
        )->writeTo($stream);
        $inZooKeeper = OffsetCommitResponseV0::unpack($stream);

        self::assertSame(
            KafkaException::OFFSET_METADATA_TOO_LARGE,
            $inZooKeeper->topics[$topic]->partitions[0]->errorCode
        );
    }

    public function testMetadataOfTheMaximumLengthIsAccepted(): void
    {
        $groupId  = self::uniqueGroupName();
        $topic    = $this->createTopic();
        $stream   = $this->coordinatorStream($groupId);
        $metadata = str_repeat('m', self::OFFSET_METADATA_MAX_BYTES);

        $this->commitInKafka($stream, $groupId, [$topic => [0 => new OffsetAndMetadata(2, $metadata)]]);
        $offsets = $this->fetchInKafka($stream, $groupId, [$topic => [0]]);

        self::assertSame(2, $offsets[$topic]->partitions[0]->offset);
        self::assertSame($metadata, $offsets[$topic]->partitions[0]->metadata);
    }

    public function testVersion1CommitIsStillAcceptedAndCarriesItsOwnTimestamp(): void
    {
        $groupId = self::uniqueGroupName();
        $topic   = $this->createTopic();
        $stream  = $this->coordinatorStream($groupId);

        // The commit timestamp of version 1 is the point the retention is counted from; -1 asks for the receive time
        new OffsetCommitRequestV1(
            $groupId,
            OffsetCommitRequest::DEFAULT_GENERATION_ID,
            OffsetCommitRequest::DEFAULT_MEMBER_NAME,
            [$topic => [0 => new OffsetCommitRequestPartitionV1(0, 77, 'by version 1')]],
            'kafka-client-t6',
            1
        )->writeTo($stream);
        $response = OffsetCommitResponseV1::unpack($stream);

        self::assertSame(KafkaException::NO_ERROR, $response->topics[$topic]->partitions[0]->errorCode);

        $offsets = $this->fetchInKafka($stream, $groupId, [$topic => [0]]);

        self::assertSame(77, $offsets[$topic]->partitions[0]->offset);
        self::assertSame('by version 1', $offsets[$topic]->partitions[0]->metadata);
    }

    /**
     * What the `retention_time` of a v2 to v4 commit still does on a 2.8.2 broker, and what KIP-211 changed
     *
     * The field is on the wire up to version 4 and gone from version 5 on, and it is what decides the **value
     * schema** the coordinator writes the offset with: `GroupMetadataManager.offsetCommitValue` @ 2.8.2 picks the
     * schema **v1** - the only one that has an `expire_timestamp` - as soon as the request brought a retention of
     * its own, and the schema **v3** otherwise, because `inter.broker.protocol.version` of the container is above
     * `2.1-IV1`. A commit that leaves the field at -1 therefore carries no expiry at all any more: KIP-211 expires
     * the offsets of a group `offsets.retention.minutes` after the group itself became empty, not a fixed time
     * after the commit, which is what a 0.11 or 1.1 broker stored here.
     */
    public function testRetentionTimeOfVersion2DecidesTheValueSchemaOfTheStoredOffset(): void
    {
        $groupId       = self::uniqueGroupName();
        $topic         = $this->createTopic();
        $stream        = $this->coordinatorStream($groupId);
        $retentionTime = 60000;

        // The first commit asks for the retention of the broker, the second one brings its own - and has to be a
        // version 4 frame to do so at all, because version 5 has no such field any more (KIP-211)
        $this->commitInKafka($stream, $groupId, [$topic => [0 => 5]]);
        new OffsetCommitRequestV4(
            $groupId,
            OffsetCommitRequest::DEFAULT_GENERATION_ID,
            OffsetCommitRequest::DEFAULT_MEMBER_NAME,
            $retentionTime,
            [$topic => [0 => 6]],
            'kafka-client-t6',
            10
        )->writeTo($stream);

        self::assertSame(
            KafkaException::NO_ERROR,
            OffsetCommitResponseV4::unpack($stream)->topics[$topic]->partitions[0]->errorCode
        );

        $stored = $this->readStoredOffsets($groupId, $topic, 0);

        self::assertNotSame([], $stored, 'the commits have to be readable back out of __consumer_offsets');

        [$offsetOfDefault, , $commitOfDefault, $expiryOfDefault, $schemaOfDefault, $epochOfDefault] = $stored[0];
        [$offsetOfExplicit, , $commitOfExplicit, $expiryOfExplicit, $schemaOfExplicit] = $stored[1];

        self::assertSame(5, $offsetOfDefault);
        self::assertSame(6, $offsetOfExplicit);
        self::assertSame(3, $schemaOfDefault, 'retention_time = -1 is written with the value schema v3 (KIP-211)');
        self::assertNull($expiryOfDefault, 'the value schema v3 has no expire_timestamp at all');
        self::assertSame(-1, $epochOfDefault, 'and its committed leader epoch is the -1 of a commit without one');
        self::assertSame(
            1,
            $schemaOfExplicit,
            'an explicit retention_time falls back to the value schema v1, the only one with an expire_timestamp'
        );
        self::assertSame(
            $retentionTime,
            $expiryOfExplicit - $commitOfExplicit,
            'and it is still honoured: the expiry is the receive time of the commit plus the retention'
        );
    }

    /**
     * KIP-211 (Kafka 2.1) took `retention_time` out of the frame at version 5, and the stored offset follows
     *
     * The field has the versions `2-4` in `OffsetCommitRequest.json` @ 2.8.2 - it is not sent as -1 - so a v5
     * request cannot ask for a retention of its own at all, whatever a caller passes. The offset is then written
     * with the `__consumer_offsets` value schema **v3**, which has no `expire_timestamp` field: the offsets of the
     * group expire `offsets.retention.minutes` after the GROUP became empty, not a fixed time after this commit.
     */
    public function testVersion5SendsNoRetentionTimeAndTheOffsetIsStoredWithoutAnExpiry(): void
    {
        $groupId = self::uniqueGroupName();
        $topic   = $this->createTopic();
        $stream  = $this->coordinatorStream($groupId);

        // An hour of retention, which a v4 frame would honour and a v5 frame has no field for
        new OffsetCommitRequestV5(
            $groupId,
            OffsetCommitRequest::DEFAULT_GENERATION_ID,
            OffsetCommitRequest::DEFAULT_MEMBER_NAME,
            3600000,
            [$topic => [0 => 9]],
            'kafka-client-t6',
            11
        )->writeTo($stream);
        $response = OffsetCommitResponseV5::unpack($stream);

        self::assertSame(KafkaException::NO_ERROR, $response->topics[$topic]->partitions[0]->errorCode);
        self::assertSame(
            ['messageSize', 'apiKey', 'apiVersion', 'correlationId', 'clientId', 'consumerGroup', 'generationId',
                'memberName', 'topicPartitions'],
            array_keys(OffsetCommitRequestV5::getScheme()),
            'the frame of version 5 has no retention time to send'
        );

        $stored = $this->readStoredOffsets($groupId, $topic, 0);

        self::assertNotSame([], $stored, 'the commit has to be readable back out of __consumer_offsets');

        [$offset, , , $expiry, $valueSchema, $leaderEpoch] = $stored[0];

        self::assertSame(9, $offset);
        self::assertSame(3, $valueSchema, 'a commit without a retention is written with the value schema v3');
        self::assertNull($expiry, 'and the value schema v3 has no expire_timestamp at all');
        self::assertSame(-1, $leaderEpoch, 'a v5 frame has no leader epoch either, so the stored one is -1');
    }

    /**
     * The `committed_leader_epoch` of OffsetCommit v6 and OffsetFetch v5 (KIP-320, Kafka 2.1)
     *
     * The epoch travels through the coordinator untouched: it is stored in the `leaderEpoch` field of the
     * `__consumer_offsets` value schema v3 and handed back by every OffsetFetch from version 5 on. An offset
     * committed with a frame below v6 has no epoch, and the answer reports the -1 of "not known" for it.
     */
    public function testTheLeaderEpochOfACommittedOffsetSurvivesTheRoundTrip(): void
    {
        $groupId = self::uniqueGroupName();
        $topic   = $this->createTopic();
        $stream  = $this->coordinatorStream($groupId);

        // The leader of a partition that was elected once and never moved is at the epoch 0
        $this->commitInKafka($stream, $groupId, [$topic => [0 => new OffsetAndMetadata(21, 'with an epoch', 0)]]);

        $withEpoch = $this->fetchInKafka($stream, $groupId, [$topic => [0]])[$topic]->partitions[0];

        self::assertSame(21, $withEpoch->offset);
        self::assertSame(0, $withEpoch->leaderEpoch, 'the epoch of the commit comes back in the version 5 answer');
        self::assertSame('with an epoch', $withEpoch->metadata);
        self::assertSame(0, $withEpoch->toOffsetAndMetadata()->leaderEpoch);

        [, , , , $valueSchema, $storedEpoch] = $this->readStoredOffsets($groupId, $topic, 0)[0];

        self::assertSame(3, $valueSchema, 'the value schema v3 is the one with a leaderEpoch field');
        self::assertSame(0, $storedEpoch, 'and the coordinator stored the epoch of the commit in it');

        // The same partition committed with a version 4 frame, which has no field for the epoch
        new OffsetCommitRequestV4(
            $groupId,
            OffsetCommitRequest::DEFAULT_GENERATION_ID,
            OffsetCommitRequest::DEFAULT_MEMBER_NAME,
            OffsetCommitRequest::DEFAULT_RETENTION_TIME,
            [$topic => [0 => 22]],
            'kafka-client-t6',
            12
        )->writeTo($stream);

        self::assertSame(
            KafkaException::NO_ERROR,
            OffsetCommitResponseV4::unpack($stream)->topics[$topic]->partitions[0]->errorCode
        );

        $withoutEpoch = $this->fetchInKafka($stream, $groupId, [$topic => [0]])[$topic]->partitions[0];

        self::assertSame(22, $withoutEpoch->offset);
        self::assertSame(
            OffsetFetchResponsePartition::UNKNOWN_LEADER_EPOCH,
            $withoutEpoch->leaderEpoch,
            'an offset committed below version 6 has no epoch, and the answer says so with -1'
        );
        self::assertNull(
            $withoutEpoch->toOffsetAndMetadata()->leaderEpoch,
            'which the value object reports as the empty Optional of the Java client'
        );
    }

    /**
     * The coordinator does not validate the epoch of a commit: the check of KIP-320 is on the fetch side
     */
    public function testAnEpochTheLeaderNeverHadIsStoredAllTheSame(): void
    {
        $groupId = self::uniqueGroupName();
        $topic   = $this->createTopic();
        $stream  = $this->coordinatorStream($groupId);

        $this->commitInKafka($stream, $groupId, [$topic => [0 => new OffsetAndMetadata(3, null, 4242)]]);

        self::assertSame(
            4242,
            $this->fetchInKafka($stream, $groupId, [$topic => [0]])[$topic]->partitions[0]->leaderEpoch,
            'the coordinator stores whatever epoch it is given - 74 and 75 are answered by the fetch path'
        );
    }

    public function testVersionNineOfTheOffsetCommitApiClosesTheConnection(): void
    {
        // A 2.8.2 broker serves the versions 0 to 8 of this api - v4 is the KIP-219 bump this client sends and v8
        // is the flexible one - and closes the connection on anything above, exactly as a 0.11 or 1.1 broker did
        // for v4: `SocketServer.processCompletedReceives` catches the `InvalidRequestException` of the parser and
        // CLOSES the channel, see "An api the broker does not serve closes the connection" in the protocol
        // document. The client has no class for the version 9, so the frame is built by hand here; its body is the
        // v2/v3/v4 one, which the broker never gets far enough to read.
        $groupId = self::uniqueGroupName();
        $topic   = $this->createTopic();
        $body    = pack('n', 8) . pack('n', 9) . pack('N', 91) . pack('n', 0)
            . pack('n', strlen($groupId)) . $groupId
            . pack('N', -1) . pack('n', 0) . pack('J', -1) . pack('N', 0);
        $stream  = $this->connect([ClientConfig::REQUEST_TIMEOUT_MS => 1000]);
        $stream->write('N', strlen($body));
        $stream->writeBuffer($body);

        try {
            OffsetCommitResponse::unpack($stream);
            self::fail('The broker cannot parse an OffsetCommit v9 and must not answer it');
        } catch (NetworkException $exception) {
            self::assertStringContainsString('stream', strtolower($exception->getMessage()));
        }

        // The connection is gone with the frame, so the commit that follows needs a new one - which is answered
        // normally, the broker itself is unaffected
        $accepted = $this->commitInKafka(
            $this->connect([ClientConfig::REQUEST_TIMEOUT_MS => 1000]),
            $groupId,
            [$topic => [0 => 3]]
        );

        self::assertSame(KafkaException::NO_ERROR, $accepted->topics[$topic]->partitions[0]->errorCode);
    }

    public function testClientRoundTripsOffsetsThroughTheKafkaStorage(): void
    {
        $groupId       = self::uniqueGroupName();
        $topic         = $this->createTopic();
        $configuration = [ClientConfig::OFFSETS_STORAGE => ClientConfig::OFFSETS_STORAGE_KAFKA]
            + $this->configuration();
        $client        = new Client($this->cluster(), $configuration);

        $coordinator = $client->getGroupCoordinator($groupId);
        $client->commitGroupOffsets(
            $coordinator,
            $groupId,
            OffsetCommitRequest::DEFAULT_MEMBER_NAME,
            OffsetCommitRequest::DEFAULT_GENERATION_ID,
            [$topic => [0 => new OffsetAndMetadata(17, 'by client')]],
            OffsetCommitRequest::DEFAULT_RETENTION_TIME
        );

        self::assertSame([$topic => [0 => 17]], $client->fetchGroupOffsets($coordinator, $groupId, [$topic => [0]]));
    }

    public function testClientRoundTripsOffsetsThroughTheZooKeeperStorage(): void
    {
        $groupId       = self::uniqueGroupName();
        $topic         = $this->createTopic();
        $configuration = [ClientConfig::OFFSETS_STORAGE => ClientConfig::OFFSETS_STORAGE_ZOOKEEPER]
            + $this->configuration();
        $client        = new Client($this->cluster(), $configuration);

        // Version 0 is answered by any broker, the coordinator is just a convenient node to talk to
        $anyNode = $client->getGroupCoordinator($groupId);
        $client->commitGroupOffsets(
            $anyNode,
            $groupId,
            OffsetCommitRequest::DEFAULT_MEMBER_NAME,
            OffsetCommitRequest::DEFAULT_GENERATION_ID,
            [$topic => [0 => 19]],
            OffsetCommitRequest::DEFAULT_RETENTION_TIME
        );

        self::assertSame([$topic => [0 => 19]], $client->fetchGroupOffsets($anyNode, $groupId, [$topic => [0]]));
    }

    public function testClientSilencesTheUnknownPartitionOfAnUncommittedZooKeeperOffset(): void
    {
        $groupId       = self::uniqueGroupName();
        $topic         = $this->createTopic();
        $configuration = [ClientConfig::OFFSETS_STORAGE => ClientConfig::OFFSETS_STORAGE_ZOOKEEPER]
            + $this->configuration();
        $client        = new Client($this->cluster(), $configuration);

        $offsets = $client->fetchGroupOffsets($client->getGroupCoordinator($groupId), $groupId, [$topic => [0]]);

        self::assertSame([$topic => [0 => -1]], $offsets, 'the error code 3 is silenced into the offset -1');
    }

    /**
     * Commits offsets into the Kafka storage with a version 2 request and asserts that the broker accepted them
     *
     * @param array<string, array<int, int|OffsetAndMetadata>> $topicPartitionOffsets Offsets to commit
     */
    private function commitInKafka(
        Stream $stream,
        string $groupId,
        array $topicPartitionOffsets,
        bool $expectSuccess = true,
        int $retentionTime = OffsetCommitRequest::DEFAULT_RETENTION_TIME
    ): OffsetCommitResponse {
        new OffsetCommitRequest(
            $groupId,
            OffsetCommitRequest::DEFAULT_GENERATION_ID,
            OffsetCommitRequest::DEFAULT_MEMBER_NAME,
            $retentionTime,
            $topicPartitionOffsets,
            'kafka-client-t6',
            1
        )->writeTo($stream);

        $response = OffsetCommitResponse::unpack($stream);
        if ($expectSuccess) {
            foreach ($response->topics as $topic => $topicResponse) {
                foreach ($topicResponse->partitions as $partitionId => $partition) {
                    self::assertSame(
                        0,
                        $partition->errorCode,
                        "The broker refused the commit for {$topic}-{$partitionId}"
                    );
                }
            }
        }

        return $response;
    }

    /**
     * Fetches the offsets that live in `__consumer_offsets` with a version 2 request
     *
     * @param array<string, list<int>>|null $topicPartitions Partitions to fetch, per topic, or null for all topics
     *
     * @return array<string, OffsetFetchResponseTopic>
     */
    private function fetchInKafka(Stream $stream, string $groupId, ?array $topicPartitions): array
    {
        $response = $this->fetchInKafkaResponse($stream, $groupId, $topicPartitions);

        self::assertSame(
            KafkaException::NO_ERROR,
            $response->errorCode,
            'the group-level error code of the version 2 answer'
        );

        return $response->topics;
    }

    /**
     * The whole version 2 answer, group-level error code included
     *
     * @param array<string, list<int>>|null $topicPartitions Partitions to fetch, per topic, or null for all topics
     */
    private function fetchInKafkaResponse(
        Stream $stream,
        string $groupId,
        ?array $topicPartitions
    ): OffsetFetchResponse {
        new OffsetFetchRequest($groupId, $topicPartitions, 'kafka-client-t6', 2)->writeTo($stream);

        return OffsetFetchResponse::unpack($stream);
    }

    /**
     * Reads every offset entry of one group and topic-partition back out of the `__consumer_offsets` topic.
     *
     * The internal topic is a plain log of key/value messages; its schemes belong to the coordinator and not to the
     * client protocol, so they are decoded here by hand. Only that log carries the commit and expiry timestamps that
     * `retention_time` decides - the OffsetFetch API never reports them.
     *
     * @return list<array{int, ?string, int, ?int}> offset, metadata, commit timestamp, expire timestamp, in the
     *         order the entries were appended
     */
    private function readStoredOffsets(string $groupId, string $topic, int $partition): array
    {
        $configuration = $this->configuration();
        $fetchOffsets  = array_fill(0, self::OFFSETS_TOPIC_PARTITIONS, 0);
        $stream        = $this->connect();

        new FetchRequest(
            [self::OFFSETS_TOPIC => $fetchOffsets],
            100,
            1,
            1048576,
            -1,
            $configuration[ClientConfig::CLIENT_ID],
            3
        )->writeTo($stream);
        $response = FetchResponse::unpack($stream);

        $entries = [];
        foreach ($response->topics[self::OFFSETS_TOPIC]->partitions ?? [] as $responsePartition) {
            // The internal topic is written in the `log.message.format.version` of the broker, 0.11.0 here, so
            // its entries are record batches: the reader has to be the one that takes any of the three formats
            foreach (MemoryRecords::fromBuffer((string) $responsePartition->messageSet, false)->getRecords() as $record) {
                $entry = self::decodeOffsetEntry((string) $record->key, $record->value);
                if ($entry !== null && $entry[0] === $groupId && $entry[1] === $topic && $entry[2] === $partition) {
                    $entries[] = $entry[3];
                }
            }
        }

        return $entries;
    }

    /**
     * Decodes one message of `__consumer_offsets` into the group, topic and partition it belongs to and its value.
     *
     * Key (version 0 and 1) => version int16 group string topic string partition int32
     * Value                 => version int16 offset int64 [leaderEpoch int32, version 3 only] metadata string
     *                          commitTimestamp int64 [expireTimestamp int64, version 1 only]
     *
     * `OffsetCommitValue.json` @ 2.8.2 has the four versions 0 to 3: version 1 is the only one with an
     * `expire_timestamp` and version 3 the only one with a `leaderEpoch`, and which of them the coordinator writes
     * follows `inter.broker.protocol.version` and the `retention_time` of the request, see
     * {@see self::testRetentionTimeOfVersion2DecidesTheValueSchemaOfTheStoredOffset}.
     *
     * Everything else - the group metadata messages of the membership protocol, whose key version is 2 - is skipped.
     *
     * @return array{string, string, int, array{int, ?string, int, ?int, int, ?int}}|null
     */
    private static function decodeOffsetEntry(string $key, ?string $value): ?array
    {
        $keyVersion = unpack('nversion', $key)['version'];
        if ($keyVersion > 1 || $value === null) {
            return null;
        }

        $offsetInKey  = 2;
        $groupLength  = unpack('nlength', substr($key, $offsetInKey, 2))['length'];
        $group        = substr($key, $offsetInKey + 2, $groupLength);
        $offsetInKey += 2 + $groupLength;
        $topicLength  = unpack('nlength', substr($key, $offsetInKey, 2))['length'];
        $topic        = substr($key, $offsetInKey + 2, $topicLength);
        $offsetInKey += 2 + $topicLength;
        $partition    = unpack('Npartition', substr($key, $offsetInKey, 4))['partition'];

        $valueVersion   = unpack('nversion', $value)['version'];
        $offsetInValue  = 2;
        $offset         = unpack('Joffset', substr($value, $offsetInValue, 8))['offset'];
        $offsetInValue += 8;
        $leaderEpoch    = null;
        if ($valueVersion >= 3) {
            $leaderEpoch    = unpack('lepoch', strrev(substr($value, $offsetInValue, 4)))['epoch'];
            $offsetInValue += 4;
        }
        $metadataLength = unpack('nlength', substr($value, $offsetInValue, 2))['length'];
        $metadata       = $metadataLength === 0xFFFF ? null : substr($value, $offsetInValue + 2, $metadataLength);
        $offsetInValue += 2 + ($metadataLength === 0xFFFF ? 0 : $metadataLength);
        $commit         = unpack('Jtimestamp', substr($value, $offsetInValue, 8))['timestamp'];
        $offsetInValue += 8;
        $expiry         = $valueVersion === 1
            ? unpack('Jtimestamp', substr($value, $offsetInValue, 8))['timestamp']
            : null;

        return [$group, $topic, $partition, [$offset, $metadata, $commit, $expiry, $valueVersion, $leaderEpoch]];
    }

    /**
     * Opens a connection to the coordinator of the given group, which is where the version 1 requests belong
     */
    private function coordinatorStream(string $groupId): Stream
    {
        $configuration = $this->configuration();
        $coordinator   = new CoordinatorLookup($this->cluster(), $configuration)->findCoordinator($groupId);

        return $coordinator->getConnection($configuration);
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
            ClientConfig::CLIENT_ID                 => 'kafka-client-t6',
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ClientConfig::RETRY_BACKOFF_MS          => 250,
            ClientConfig::REQUEST_TIMEOUT_MS        => 10000,
        ] + ClientConfig::getDefaultConfiguration();
    }

    /**
     * Creates a topic for this test run and waits until the cluster has elected a leader for its partitions
     */
    private function createTopic(int $partitions = 1): string
    {
        $topic    = self::uniqueTopicName('t6-offsets');
        $deadline = microtime(true) + 30.0;
        $attempt  = 0;

        do {
            $stream = $this->connect();
            new MetadataRequest([$topic], true, 'kafka-client-t6', ++$attempt)->writeTo($stream);
            $metadata = MetadataResponse::unpack($stream)->topics[$topic] ?? null;

            $isReady = $metadata !== null
                && count($metadata->partitions) >= $partitions
                && array_filter($metadata->partitions, static fn($info): bool => $info->leader < 0) === [];
            if ($isReady) {
                return $topic;
            }
            usleep(250000);
        } while (microtime(true) < $deadline);

        self::fail("The broker did not create the topic {$topic} within 30 seconds");
    }

    /**
     * Builds a consumer group name that is unique for this test run
     */
    private static function uniqueGroupName(): string
    {
        return 't6-group-' . bin2hex(random_bytes(6));
    }
}
