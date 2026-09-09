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
use Protocol\Kafka\Common\Record\MessageSet;
use Protocol\Kafka\Consumer\OffsetAndMetadata;
use Protocol\Kafka\IO\Stream;
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
use Protocol\Kafka\Protocol\Request\OffsetCommitResponse;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequest;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequestV0;
use Protocol\Kafka\Protocol\Request\OffsetFetchResponse;

/**
 * Verifies the GroupCoordinator, OffsetCommit and OffsetFetch APIs against a real Kafka 0.9.0.1 broker.
 *
 * The versions of the OffsetCommit API address two different storages: version 0 keeps the offsets in ZooKeeper as
 * Kafka 0.8.1 did, versions 1 and 2 keep them in the internal `__consumer_offsets` topic of the cluster. All of them
 * are exercised here, because version 0 and version 2 are reachable through the `offsets.storage` option of the
 * client and version 1 is the version a 0.8 broker expects.
 *
 * @see docs/protocol/0.10.2.md, sections "GroupCoordinator API (key 10, v0)",
 *      "OffsetCommit API (key 8, v0, v1 and v2)" and "OffsetFetch API (key 9, v0 and v1)"
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
#[CoversClass(OffsetFetchRequest::class)]
#[CoversClass(OffsetFetchRequestV0::class)]
#[CoversClass(OffsetFetchResponse::class)]
#[CoversClass(OffsetFetchResponseTopic::class)]
#[CoversClass(OffsetFetchResponsePartition::class)]
final class OffsetsCoordinatorTest extends IntegrationTestCase
{
    /**
     * `offset.metadata.max.bytes` of a 0.9.0.1 broker; a longer metadata string is answered with the error code 12
     */
    private const int OFFSET_METADATA_MAX_BYTES = 4096;

    /**
     * `offsets.retention.minutes` of the broker, in milliseconds: what a `retention_time` of -1 asks for
     */
    private const int DEFAULT_OFFSET_RETENTION_MS = 24 * 60 * 60 * 1000;

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
        new GroupCoordinatorRequest($groupId, 'kafka-client-t6', 1)->writeTo($stream);
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

        $offsets = $this->fetchV1($stream, $groupId, [$topic => [0, 1]]);

        self::assertSame(21, $offsets[$topic]->partitions[0]->offset);
        self::assertSame(0, $offsets[$topic]->partitions[0]->errorCode);
        self::assertSame(42, $offsets[$topic]->partitions[1]->offset);
        self::assertSame(0, $offsets[$topic]->partitions[1]->errorCode);
    }

    public function testOffsetsCommittedInZooKeeperStorageAreFetchedBackFromAnyBroker(): void
    {
        $groupId = self::uniqueGroupName();
        $topic   = $this->createTopic();
        // Version 0 does not go through the coordinator at all: the bootstrap connection is enough
        $stream  = $this->connect();

        new OffsetCommitRequestV0($groupId, [$topic => [0 => 7]], 'kafka-client-t6', 11)->writeTo($stream);
        $commitResponse = OffsetCommitResponse::unpack($stream);

        self::assertSame(11, $commitResponse->getCorrelationId());
        self::assertSame(0, $commitResponse->topics[$topic]->partitions[0]->errorCode);

        new OffsetFetchRequestV0($groupId, [$topic => [0]], 'kafka-client-t6', 12)->writeTo($stream);
        $fetchResponse = OffsetFetchResponse::unpack($stream);

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
        OffsetCommitResponse::unpack($stream);

        $fromKafka = $this->fetchV1($stream, $groupId, [$topic => [0]]);
        new OffsetFetchRequestV0($groupId, [$topic => [0]], 'kafka-client-t6', 22)->writeTo($stream);
        $fromZooKeeper = OffsetFetchResponse::unpack($stream)->topics;

        self::assertSame(1000, $fromKafka[$topic]->partitions[0]->offset);
        self::assertSame(5, $fromZooKeeper[$topic]->partitions[0]->offset);
    }

    public function testUncommittedPartitionIsReportedDifferentlyByTheTwoVersions(): void
    {
        $groupId = self::uniqueGroupName();
        $topic   = $this->createTopic();
        $stream  = $this->coordinatorStream($groupId);

        $fromKafka = $this->fetchV1($stream, $groupId, [$topic => [0]]);

        self::assertSame(-1, $fromKafka[$topic]->partitions[0]->offset);
        self::assertSame(
            KafkaException::NO_ERROR,
            $fromKafka[$topic]->partitions[0]->errorCode,
            'reading a partition of __consumer_offsets that was never written is not an error'
        );

        new OffsetFetchRequestV0($groupId, [$topic => [0]], 'kafka-client-t6', 31)->writeTo($stream);
        $fromZooKeeper = OffsetFetchResponse::unpack($stream)->topics;

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

        $fromKafka = $this->fetchV1($stream, $groupId, [$topic => [$missingPartition]]);

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
        $fromZooKeeper = OffsetFetchResponse::unpack($stream)->topics;

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
        $offsets = $this->fetchV1($stream, $groupId, [$topic => [0]]);

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
        $inZooKeeper = OffsetCommitResponse::unpack($stream);

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
        $offsets = $this->fetchV1($stream, $groupId, [$topic => [0]]);

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
        $response = OffsetCommitResponse::unpack($stream);

        self::assertSame(KafkaException::NO_ERROR, $response->topics[$topic]->partitions[0]->errorCode);

        $offsets = $this->fetchV1($stream, $groupId, [$topic => [0]]);

        self::assertSame(77, $offsets[$topic]->partitions[0]->offset);
        self::assertSame('by version 1', $offsets[$topic]->partitions[0]->metadata);
    }

    public function testRetentionTimeOfVersion2ReplacesTheRetentionOfTheBroker(): void
    {
        $groupId       = self::uniqueGroupName();
        $topic         = $this->createTopic();
        $stream        = $this->coordinatorStream($groupId);
        $retentionTime = 60000;

        // Partition 0 asks for the retention of the broker, partition 1 brings its own
        $this->commitInKafka($stream, $groupId, [$topic => [0 => 5]]);
        $this->commitInKafka($stream, $groupId, [$topic => [0 => 6]], true, $retentionTime);

        $stored = $this->readStoredOffsets($groupId, $topic, 0);

        self::assertNotSame([], $stored, 'the commits have to be readable back out of __consumer_offsets');

        [$offsetOfDefault, , $commitOfDefault, $expiryOfDefault] = $stored[0];
        [$offsetOfExplicit, , $commitOfExplicit, $expiryOfExplicit] = $stored[1];

        self::assertSame(5, $offsetOfDefault);
        self::assertSame(6, $offsetOfExplicit);
        self::assertSame(
            self::DEFAULT_OFFSET_RETENTION_MS,
            $expiryOfDefault - $commitOfDefault,
            'retention_time = -1 asks for offsets.retention.minutes of the broker'
        );
        self::assertSame(
            $retentionTime,
            $expiryOfExplicit - $commitOfExplicit,
            'an explicit retention_time replaces it for this commit alone'
        );
    }

    public function testVersion3OfTheOffsetCommitApiIsDroppedWithoutAnAnswer(): void
    {
        // `OffsetCommitRequest.readFrom` @ 0.9.0.1 asserts that the version is 0, 1 or 2. A frame it cannot parse is
        // not refused, it is silently dropped: the socket stays open and the request is simply never answered, see
        // "An api the broker does not serve is dropped, not refused" in the protocol document. The client has no
        // class for the version, so the frame is built by hand here.
        $groupId = self::uniqueGroupName();
        $topic   = $this->createTopic();
        $body    = pack('n', 8) . pack('n', 3) . pack('N', 91) . pack('n', 0)
            . pack('n', strlen($groupId)) . $groupId
            . pack('N', -1) . pack('n', 0) . pack('J', -1) . pack('N', 0);
        $stream  = $this->connect([ClientConfig::REQUEST_TIMEOUT_MS => 1000]);
        $stream->write('N', strlen($body));
        $stream->writeBuffer($body);

        try {
            OffsetCommitResponse::unpack($stream);
            self::fail('The broker cannot parse an OffsetCommit v3 and must not answer it');
        } catch (NetworkException $exception) {
            self::assertStringContainsString('stream', strtolower($exception->getMessage()));
        }

        // The connection is still perfectly usable: the next well-formed request on it is answered normally
        $accepted = $this->commitInKafka($stream, $groupId, [$topic => [0 => 3]]);

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
     * Fetches offsets with a version 1 request
     *
     * @param array<string, list<int>> $topicPartitions Partitions to fetch, per topic
     *
     * @return array<string, OffsetFetchResponseTopic>
     */
    private function fetchV1(Stream $stream, string $groupId, array $topicPartitions): array
    {
        new OffsetFetchRequest($groupId, $topicPartitions, 'kafka-client-t6', 2)->writeTo($stream);

        return OffsetFetchResponse::unpack($stream)->topics;
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
            foreach (MessageSet::fromBuffer((string) $responsePartition->messageSet, false)->getRecords() as $record) {
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
     * Key (version 0 and 1)   => version int16 group string topic string partition int32
     * Value (version 0 and 1) => version int16 offset int64 metadata string commitTimestamp int64
     *                            [expireTimestamp int64, version 1 only]
     *
     * Everything else - the group metadata messages of the membership protocol, whose key version is 2 - is skipped.
     *
     * @return array{string, string, int, array{int, ?string, int, ?int}}|null
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
        $metadataLength = unpack('nlength', substr($value, $offsetInValue, 2))['length'];
        $metadata       = $metadataLength === 0xFFFF ? null : substr($value, $offsetInValue + 2, $metadataLength);
        $offsetInValue += 2 + ($metadataLength === 0xFFFF ? 0 : $metadataLength);
        $commit         = unpack('Jtimestamp', substr($value, $offsetInValue, 8))['timestamp'];
        $offsetInValue += 8;
        $expiry         = $valueVersion >= 1
            ? unpack('Jtimestamp', substr($value, $offsetInValue, 8))['timestamp']
            : null;

        return [$group, $topic, $partition, [$offset, $metadata, $commit, $expiry]];
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
            new MetadataRequest([$topic], 'kafka-client-t6', ++$attempt)->writeTo($stream);
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
