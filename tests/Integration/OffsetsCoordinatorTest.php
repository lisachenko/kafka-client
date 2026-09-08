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
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Consumer\OffsetAndMetadata;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Data\GroupCoordinatorResponseMetadata;
use Protocol\Kafka\Protocol\Data\OffsetCommitRequestPartition;
use Protocol\Kafka\Protocol\Data\OffsetCommitRequestTopic;
use Protocol\Kafka\Protocol\Data\OffsetFetchResponsePartition;
use Protocol\Kafka\Protocol\Data\OffsetFetchResponseTopic;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorRequest;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorResponse;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\MetadataResponse;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequest;
use Protocol\Kafka\Protocol\Request\OffsetCommitRequestV0;
use Protocol\Kafka\Protocol\Request\OffsetCommitResponse;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequest;
use Protocol\Kafka\Protocol\Request\OffsetFetchRequestV0;
use Protocol\Kafka\Protocol\Request\OffsetFetchResponse;

/**
 * Verifies the GroupCoordinator, OffsetCommit and OffsetFetch APIs against a real Kafka 0.9.0.1 broker.
 *
 * The two versions of the offset APIs address two different storages: version 0 keeps the offsets in ZooKeeper as
 * Kafka 0.8.1 did, version 1 keeps them in the internal `__consumer_offsets` topic of the cluster. Both are exercised
 * here, because both are reachable through the `offsets.storage` option of the client.
 *
 * @see docs/protocol/0.9.0.md, sections "GroupCoordinator API (key 10, v0)", "OffsetCommit API (key 8, v0 and v1)"
 *      and "OffsetFetch API (key 9, v0 and v1)"
 */
#[CoversClass(Client::class)]
#[CoversClass(CoordinatorLookup::class)]
#[CoversClass(GroupCoordinatorRequest::class)]
#[CoversClass(GroupCoordinatorResponse::class)]
#[CoversClass(GroupCoordinatorResponseMetadata::class)]
#[CoversClass(OffsetCommitRequest::class)]
#[CoversClass(OffsetCommitRequestV0::class)]
#[CoversClass(OffsetCommitRequestPartition::class)]
#[CoversClass(OffsetCommitRequestTopic::class)]
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

        $this->commitV1($stream, $groupId, [$topic => [0 => 21, 1 => 42]]);

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

        $this->commitV1($stream, $groupId, [$topic => [0 => 1000]]);
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

        $this->commitV1($stream, $groupId, [$topic => [0 => new OffsetAndMetadata(64, $metadata)]]);
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

        // Version 1: the entry is filtered out of the append to __consumer_offsets and reported as an error. Note
        // that KafkaApis still hands the whole request to OffsetManager.putOffsets(), so the rejected offset does
        // reach the in-memory cache of this coordinator - it just never becomes durable. Only the error code is a
        // contract of the protocol, so only the error code is asserted here.
        $inKafka = $this->commitV1($stream, $groupId, [$topic => [0 => new OffsetAndMetadata(1, $metadata)]], false);

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

        $this->commitV1($stream, $groupId, [$topic => [0 => new OffsetAndMetadata(2, $metadata)]]);
        $offsets = $this->fetchV1($stream, $groupId, [$topic => [0]]);

        self::assertSame(2, $offsets[$topic]->partitions[0]->offset);
        self::assertSame($metadata, $offsets[$topic]->partitions[0]->metadata);
    }

    public function testClientRoundTripsOffsetsThroughTheKafkaStorage(): void
    {
        $groupId       = self::uniqueGroupName();
        $topic         = $this->createTopic();
        $configuration = [ClientConfig::OFFSETS_STORAGE => ClientConfig::OFFSETS_STORAGE_KAFKA]
            + $this->configuration();
        $client        = new Client($this->cluster(), $configuration);

        $coordinator = $client->getGroupCoordinator($groupId);
        $client->commitGroupOffsets($coordinator, $groupId, [$topic => [0 => new OffsetAndMetadata(17, 'by client')]]);

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
        $client->commitGroupOffsets($anyNode, $groupId, [$topic => [0 => 19]]);

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
     * Commits offsets with a version 1 request and asserts that the broker accepted them
     *
     * @param array<string, array<int, int|OffsetAndMetadata>> $topicPartitionOffsets Offsets to commit
     */
    private function commitV1(
        Stream $stream,
        string $groupId,
        array $topicPartitionOffsets,
        bool $expectSuccess = true
    ): OffsetCommitResponse {
        new OffsetCommitRequest(
            $groupId,
            OffsetCommitRequest::DEFAULT_GENERATION_ID,
            OffsetCommitRequest::DEFAULT_MEMBER_NAME,
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
