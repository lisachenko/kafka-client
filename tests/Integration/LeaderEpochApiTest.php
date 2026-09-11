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
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\UnknownLeaderEpochException;
use Protocol\Kafka\Common\PartitionMetadata;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Data\FetchRequestTopicPartition;
use Protocol\Kafka\Protocol\Data\OffsetForLeaderEpochRequestPartition;
use Protocol\Kafka\Protocol\Data\OffsetForLeaderEpochResponsePartition;
use Protocol\Kafka\Protocol\Data\OffsetsRequestPartition;
use Protocol\Kafka\Protocol\Data\OffsetsResponsePartition;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\FetchRequestV8;
use Protocol\Kafka\Protocol\Request\FetchRequestV9;
use Protocol\Kafka\Protocol\Request\FetchResponse;
use Protocol\Kafka\Protocol\Request\FetchResponseV8;
use Protocol\Kafka\Protocol\Request\FetchResponseV9;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\MetadataRequestV6;
use Protocol\Kafka\Protocol\Request\MetadataResponse;
use Protocol\Kafka\Protocol\Request\MetadataResponseV6;
use Protocol\Kafka\Protocol\Request\OffsetForLeaderEpochRequest;
use Protocol\Kafka\Protocol\Request\OffsetForLeaderEpochRequestV1;
use Protocol\Kafka\Protocol\Request\OffsetForLeaderEpochResponse;
use Protocol\Kafka\Protocol\Request\OffsetForLeaderEpochResponseV1;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;
use Protocol\Kafka\Protocol\Request\OffsetsResponse;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceResponse;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * **KIP-320** (Kafka 2.1) on the wire: the leader epoch in the four apis a consumer speaks.
 *
 * A leader epoch is a counter the controller raises with every leader election of a partition, and Kafka 2.1 put
 * it into the Metadata answer (v7), into every partition entry of a Fetch request (v9), into both halves of
 * ListOffsets (v4) and into the OffsetForLeaderEpoch request (v2). This class measures all four against the
 * container: the value the broker reports, the fencing of an epoch the leader is not on, and the fields the lower
 * version of each api does not have.
 *
 * @see docs/protocol/2.8.md, sections "The leader epoch (KIP-320)" and "Metadata API (key 3, v0 to v7)"
 */
#[CoversClass(FetchRequest::class)]
#[CoversClass(FetchRequestTopicPartition::class)]
#[CoversClass(MetadataResponse::class)]
#[CoversClass(PartitionMetadata::class)]
#[CoversClass(OffsetsRequest::class)]
#[CoversClass(OffsetsRequestPartition::class)]
#[CoversClass(OffsetsResponsePartition::class)]
#[CoversClass(OffsetForLeaderEpochRequest::class)]
#[CoversClass(OffsetForLeaderEpochRequestPartition::class)]
#[CoversClass(OffsetForLeaderEpochResponse::class)]
final class LeaderEpochApiTest extends IntegrationTestCase
{
    /**
     * Client id that identifies the requests of this test in the logs of the broker
     */
    private const string CLIENT_ID = 'kafka-client-t2-leader-epoch';

    private const int PRODUCE_TIMEOUT_MS = 5000;

    private const int FETCH_MAX_WAIT_MS = 500;

    /**
     * Topic of the current test, one partition on one broker
     */
    private string $topic;

    protected function setUp(): void
    {
        parent::setUp();

        $this->topic = self::uniqueTopicName('t2-21-epoch');
        self::createTopic($this->topic);
        new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, self::CLIENT_ID)
            ->awaitTopicWithLeaders($this->topic);

        $this->produce('epoch-value');
    }

    public function testMetadataVersionSevenReportsTheLeaderEpochOfEveryPartition(): void
    {
        $stream = $this->connect();
        new MetadataRequest([$this->topic], false, self::CLIENT_ID, 900)->writeTo($stream);
        $partition = MetadataResponse::unpack($stream)->topics[$this->topic]->partitions[0];

        // A partition that has been led by the same broker since it was created is on the epoch 0, and every
        // election raises it by one; -1 is what a client reads when the answer does not carry the field
        self::assertSame(KafkaException::NO_ERROR, $partition->partitionErrorCode);
        self::assertGreaterThanOrEqual(0, $partition->leaderEpoch);

        $stream = $this->connect();
        new MetadataRequestV6([$this->topic], false, self::CLIENT_ID, 901)->writeTo($stream);
        $belowSeven = MetadataResponseV6::unpack($stream)->topics[$this->topic]->partitions[0];

        self::assertSame(
            PartitionMetadata::UNKNOWN_LEADER_EPOCH,
            $belowSeven->leaderEpoch,
            'a version 6 answer does not carry the field at all'
        );
        self::assertSame($partition->leader, $belowSeven->leader, 'and describes the same leadership otherwise');
    }

    public function testFetchVersionNineSendsTheCurrentLeaderEpochAndIsFencedWithSeventyFive(): void
    {
        $epoch = $this->leaderEpoch();

        foreach ([$epoch, FetchRequestTopicPartition::UNKNOWN_LEADER_EPOCH] as $accepted) {
            $partition = $this->fetch(FetchRequestV9::class, FetchResponseV9::class, 910, $accepted);

            self::assertSame(
                KafkaException::NO_ERROR,
                $partition->errorCode,
                "the epoch {$accepted} is the one the leader is on, or no belief at all"
            );
            self::assertSame(['epoch-value'], $this->valuesOf($partition->getRecords()->getRecords()));
        }

        // One above the epoch the partition really is led with: a belief from the future, which the leader refuses
        $fenced = $this->fetch(FetchRequestV9::class, FetchResponseV9::class, 911, $epoch + 1);

        self::assertSame(KafkaException::UNKNOWN_LEADER_EPOCH, $fenced->errorCode);
        self::assertSame(75, KafkaException::UNKNOWN_LEADER_EPOCH);
        self::assertInstanceOf(
            UnknownLeaderEpochException::class,
            KafkaException::fromCode($fenced->errorCode, ['topic' => $this->topic])
        );
        self::assertSame(-1, $fenced->highWaterMarkOffset, 'the refused partition has no high water mark');
        self::assertSame([], $fenced->getRecords()->getRecords(), 'and no records');
    }

    public function testFetchVersionTenSendsTheSameBodyAsVersionNine(): void
    {
        $epoch     = $this->leaderEpoch();
        $partition = $this->fetch(FetchRequest::class, FetchResponse::class, 920, $epoch);

        self::assertSame(KafkaException::NO_ERROR, $partition->errorCode);
        self::assertSame(['epoch-value'], $this->valuesOf($partition->getRecords()->getRecords()));

        // The bodies of a version 9 and a version 10 request are the same bytes, the api version apart
        $ten  = (string) $this->fetchRequest(FetchRequest::class, 930, $epoch);
        $nine = (string) $this->fetchRequest(FetchRequestV9::class, 930, $epoch);

        self::assertSame(substr($ten, 8), substr($nine, 8));
        self::assertNotSame($ten, $nine, 'only the api version of the header differs');

        // And a version 8 request has no room for the epoch at all
        $eight = (string) $this->fetchRequest(FetchRequestV8::class, 930, $epoch);
        self::assertSame(strlen($nine) - 4, strlen($eight));

        $stream = $this->connect();
        $this->fetchRequest(FetchRequestV8::class, 931, $epoch)->writeTo($stream);
        self::assertSame(
            KafkaException::NO_ERROR,
            FetchResponseV8::unpack($stream)->topics[$this->topic]->partitions[0]->errorCode,
            'a version 8 fetch is never fenced, because it states no belief about the leadership'
        );
    }

    public function testListOffsetsVersionFourCarriesTheEpochOnBothSides(): void
    {
        $epoch = $this->leaderEpoch();

        $stream = $this->connect();
        new OffsetsRequest(
            [$this->topic => [0 => [OffsetsRequest::LATEST, $epoch]]],
            OffsetsRequest::CONSUMER_REPLICA_ID,
            FetchRequest::READ_UNCOMMITTED,
            self::CLIENT_ID,
            940
        )->writeTo($stream);
        $partition = OffsetsResponse::unpack($stream)->topics[$this->topic]->partitions[0];

        self::assertSame(KafkaException::NO_ERROR, $partition->errorCode);
        self::assertSame(1, $partition->offset, 'the log end offset of the one record of this topic');
        self::assertSame($epoch, $partition->leaderEpoch, 'the epoch the leader resolved the offset in');

        // The same fencing as a fetch: an epoch above the one the leader is on is refused
        $stream = $this->connect();
        new OffsetsRequest(
            [$this->topic => [0 => [OffsetsRequest::LATEST, $epoch + 1]]],
            OffsetsRequest::CONSUMER_REPLICA_ID,
            FetchRequest::READ_UNCOMMITTED,
            self::CLIENT_ID,
            941
        )->writeTo($stream);
        $fenced = OffsetsResponse::unpack($stream)->topics[$this->topic]->partitions[0];

        self::assertSame(KafkaException::UNKNOWN_LEADER_EPOCH, $fenced->errorCode);
        self::assertSame(OffsetsResponsePartition::UNKNOWN_OFFSET, $fenced->offset);
    }

    public function testOffsetForLeaderEpochVersionTwoAsksWhereAnEpochEnded(): void
    {
        $epoch = $this->leaderEpoch();

        $stream = $this->connect();
        new OffsetForLeaderEpochRequest(
            [$this->topic => [0 => [$epoch, $epoch]]],
            self::CLIENT_ID,
            950
        )->writeTo($stream);
        $response  = OffsetForLeaderEpochResponse::unpack($stream);
        $partition = $response->topics[$this->topic]->partitions[0];

        // The throttle time that version 2 put at the head of the frame: the api became one a client quota
        // applies to, because from Kafka 2.1 on an ordinary consumer sends it
        self::assertSame(0, $response->throttleTimeMs);
        self::assertSame(KafkaException::NO_ERROR, $partition->errorCode);
        self::assertSame($epoch, $partition->leaderEpoch);
        self::assertSame(1, $partition->endOffset, 'the epoch the leader is still on ends at the log end offset');

        // And the fencing of version 2: a current_leader_epoch above the one the leader is on
        $stream = $this->connect();
        new OffsetForLeaderEpochRequest(
            [$this->topic => [0 => [$epoch, $epoch + 1]]],
            self::CLIENT_ID,
            951
        )->writeTo($stream);
        $fenced = OffsetForLeaderEpochResponse::unpack($stream)->topics[$this->topic]->partitions[0];

        self::assertSame(KafkaException::UNKNOWN_LEADER_EPOCH, $fenced->errorCode);
        self::assertSame(OffsetForLeaderEpochResponsePartition::UNDEFINED_EPOCH, $fenced->leaderEpoch);
        self::assertSame(OffsetForLeaderEpochResponsePartition::UNDEFINED_EPOCH_OFFSET, $fenced->endOffset);
    }

    public function testTheVersionOneOfTheSameQuestionHasNeitherFieldAndIsNeverFenced(): void
    {
        $epoch = $this->leaderEpoch();

        $stream = $this->connect();
        new OffsetForLeaderEpochRequestV1([$this->topic => [0 => $epoch]], self::CLIENT_ID, 960)->writeTo($stream);
        $response = OffsetForLeaderEpochResponseV1::unpack($stream);

        self::assertSame(
            0,
            $response->throttleTimeMs,
            'the property of a version 1 answer keeps its default, because the frame has no such field'
        );
        self::assertSame(1, $response->topics[$this->topic]->partitions[0]->endOffset);

        // The two frames differ in the four bytes of the epoch of the request and the four of the throttle time
        $two = (string) new OffsetForLeaderEpochRequest([$this->topic => [0 => [$epoch, $epoch]]], self::CLIENT_ID, 1);
        $one = (string) new OffsetForLeaderEpochRequestV1([$this->topic => [0 => $epoch]], self::CLIENT_ID, 1);

        self::assertSame(strlen($one) + 4, strlen($two));
    }

    /**
     * Returns the leader epoch the Metadata v7 answer reports for the partition 0 of the topic of this test
     */
    private function leaderEpoch(): int
    {
        $stream = $this->connect();
        new MetadataRequest([$this->topic], false, self::CLIENT_ID, 890)->writeTo($stream);

        return MetadataResponse::unpack($stream)->topics[$this->topic]->partitions[0]->leaderEpoch;
    }

    /**
     * @param class-string<FetchRequest> $requestClass
     */
    private function fetchRequest(string $requestClass, int $correlationId, int $epoch): FetchRequest
    {
        return new $requestClass(
            [$this->topic => [0 => [0, $epoch]]],
            self::FETCH_MAX_WAIT_MS,
            1,
            65536,
            -1,
            self::CLIENT_ID,
            $correlationId
        );
    }

    /**
     * @param class-string<FetchRequest>  $requestClass
     * @param class-string<FetchResponse> $responseClass
     */
    private function fetch(
        string $requestClass,
        string $responseClass,
        int $correlationId,
        int $epoch
    ): \Protocol\Kafka\Protocol\Data\FetchResponsePartition {
        $stream = $this->connect();
        $this->fetchRequest($requestClass, $correlationId, $epoch)->writeTo($stream);

        return $responseClass::unpack($stream)->topics[$this->topic]->partitions[0];
    }

    /**
     * @param list<Record> $records
     *
     * @return list<string|null>
     */
    private function valuesOf(array $records): array
    {
        return array_map(static fn(Record $record): ?string => $record->value, $records);
    }

    /**
     * Produces one record of the message format v2 to the partition 0 of the topic of this test
     */
    private function produce(string $value): void
    {
        $stream = $this->connect();
        new ProduceRequest(
            [$this->topic => [0 => RecordBatch::fromRecords(
                [new Record($value, null, 0, null, (int) round(microtime(true) * 1000))]
            )]],
            1,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            880
        )->writeTo($stream);

        self::assertSame(
            KafkaException::NO_ERROR,
            ProduceResponse::unpack($stream)->topics[$this->topic]->partitions[0]->errorCode
        );
    }

    /**
     * Creates a topic with the given topic-level options through the `kafka-topics.sh` of the container
     *
     * @param array<string, string> $configuration Topic-level options, as `name => value`
     */
    private static function createTopic(string $topic, array $configuration = []): void
    {
        $options = '';
        foreach ($configuration as $name => $value) {
            $options .= ' --config ' . escapeshellarg("{$name}={$value}");
        }

        $container = getenv('KAFKA_CONTAINER');
        $container = $container === false || trim($container) === '' ? 'kafka-2-8-2' : trim($container);
        $command   = sprintf(
            'docker exec %s /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create'
            . ' --if-not-exists --topic %s --partitions 1 --replication-factor 1%s 2>&1',
            escapeshellarg($container),
            escapeshellarg($topic),
            $options
        );

        $output   = [];
        $exitCode = 0;
        exec($command, $output, $exitCode);

        if ($exitCode !== 0) {
            self::fail("Can not create the topic {$topic}: " . implode("\n", $output));
        }
    }
}
