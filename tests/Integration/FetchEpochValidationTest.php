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
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Data\FetchRequestTopicPartition;
use Protocol\Kafka\Protocol\Data\FetchResponseDivergingEpoch;
use Protocol\Kafka\Protocol\Data\FetchResponsePartition;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\FetchRequestV11;
use Protocol\Kafka\Protocol\Request\FetchResponse;
use Protocol\Kafka\Protocol\Request\FetchResponseV11;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceResponse;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * **Fetch v12** (Kafka 2.7) against the container: the flexible encoding of KIP-482 and the epoch validation of
 * KIP-595.
 *
 * Version 12 is the first flexible version of the api - compact strings, compact arrays, a compact record set,
 * a tagged-field section behind every structure, the request header v2 and the response header v1 - and it puts
 * the `last_fetched_epoch` of the fetcher into every partition entry of the request. A leader that is given one
 * compares it with its own log and answers the **tagged** `diverging_epoch` instead of records when the two do
 * not match, which is the truncation detection of KIP-320 without the second round trip.
 *
 * Every case of the table in the document is measured here, against a fresh topic whose log is at the epoch 0.
 *
 * @see docs/protocol/2.8.md, sections "Epoch validation in the fetch itself (v12, KIP-595)" and
 *      "Fetch API (key 1, v0 to v12)"
 */
#[CoversClass(FetchRequest::class)]
#[CoversClass(FetchRequestTopicPartition::class)]
#[CoversClass(FetchResponse::class)]
#[CoversClass(FetchResponsePartition::class)]
#[CoversClass(FetchResponseDivergingEpoch::class)]
final class FetchEpochValidationTest extends IntegrationTestCase
{
    /**
     * Client id that identifies the requests of this test in the logs of the broker
     */
    private const string CLIENT_ID = 'kafka-client-t2-27-epoch';

    private const int PRODUCE_TIMEOUT_MS = 5000;

    private const int FETCH_MAX_WAIT_MS = 500;

    /**
     * Epoch a one-broker log that was never re-elected is written in
     */
    private const int LOG_EPOCH = 0;

    /**
     * Topic of the current test, one partition on one broker
     */
    private string $topic;

    protected function setUp(): void
    {
        parent::setUp();

        $this->topic = self::uniqueTopicName('t2-27-epoch');
        self::createTopic($this->topic);
        new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, self::CLIENT_ID)
            ->awaitTopicWithLeaders($this->topic);

        $this->produce('epoch-validation');
    }

    protected function tearDown(): void
    {
        // The property is only set once setUp() ran past its skip, i.e. only when there is a broker at all
        if (isset($this->topic)) {
            self::deleteTopic($this->topic);
        }

        parent::tearDown();
    }

    public function testAVersionTwelveFetchIsTheVersionElevenAnswerInTheFlexibleEncoding(): void
    {
        $flexible = $this->fetch(FetchRequest::class, FetchResponse::class, 940, 0);
        $plain    = $this->fetch(FetchRequestV11::class, FetchResponseV11::class, 941, 0);

        // The same question and the same answer, field for field: version 12 changed the encoding and added two
        // things that an ordinary consumer does not use, not what a fetch of a partition returns
        self::assertSame(KafkaException::NO_ERROR, $flexible->errorCode);
        self::assertSame($plain->errorCode, $flexible->errorCode);
        self::assertSame($plain->highWaterMarkOffset, $flexible->highWaterMarkOffset);
        self::assertSame($plain->lastStableOffset, $flexible->lastStableOffset);
        self::assertSame($plain->logStartOffset, $flexible->logStartOffset);
        self::assertSame($plain->preferredReadReplica, $flexible->preferredReadReplica);
        self::assertSame($plain->messageSet, $flexible->messageSet);
        self::assertSame(['epoch-validation'], array_column($flexible->getRecords()->getRecords(), 'value'));

        // None of the three tagged fields of version 12 is answered to a consumer by a ZooKeeper-backed broker
        self::assertNull($flexible->divergingEpoch);
        self::assertNull($flexible->currentLeader, 'the current leader belongs to the raft replication');
        self::assertNull($flexible->snapshotId, 'and so does the snapshot id of KIP-630');
    }

    public function testAFetchOffsetBehindTheEndOfTheStatedEpochIsAnsweredWithADivergingEpoch(): void
    {
        $highWaterMark = $this->fetch(FetchRequest::class, FetchResponse::class, 942, 0)->highWaterMarkOffset;

        // The epoch is the real one, the offset lies behind the end of it: `Partition.readRecords` @ 2.8.2 finds
        // that the two logs diverge and answers the tag 0 instead of records
        $diverging = $this->fetch(FetchRequest::class, FetchResponse::class, 943, $highWaterMark + 4, self::LOG_EPOCH);

        self::assertSame(KafkaException::NO_ERROR, $diverging->errorCode, 'a divergence is not an error');
        self::assertSame('', $diverging->messageSet, 'and it is answered with no record at all');
        self::assertSame($highWaterMark, $diverging->highWaterMarkOffset);
        self::assertInstanceOf(FetchResponseDivergingEpoch::class, $diverging->divergingEpoch);
        self::assertSame(self::LOG_EPOCH, $diverging->divergingEpoch->epoch);
        self::assertSame(
            $highWaterMark,
            $diverging->divergingEpoch->endOffset,
            'the offset the fetcher has to truncate to is the end of the epoch, i.e. the end of this log'
        );
    }

    public function testTheEndOfTheStatedEpochItselfIsNoDivergence(): void
    {
        $highWaterMark = $this->fetch(FetchRequest::class, FetchResponse::class, 944, 0)->highWaterMarkOffset;

        $atTheEnd = $this->fetch(FetchRequest::class, FetchResponse::class, 945, $highWaterMark, self::LOG_EPOCH);
        $atTheStart = $this->fetch(FetchRequest::class, FetchResponse::class, 946, 0, self::LOG_EPOCH);

        // The fetcher read the whole log and says so: the offset is exactly where the epoch ends, which is where
        // the leader is as well
        self::assertSame(KafkaException::NO_ERROR, $atTheEnd->errorCode);
        self::assertNull($atTheEnd->divergingEpoch);
        self::assertSame('', $atTheEnd->messageSet, 'there is nothing behind the high water mark');

        // And the same epoch with the offset 0 is served like any other fetch
        self::assertSame(KafkaException::NO_ERROR, $atTheStart->errorCode);
        self::assertNull($atTheStart->divergingEpoch);
        self::assertSame(['epoch-validation'], array_column($atTheStart->getRecords()->getRecords(), 'value'));
    }

    public function testAnEpochTheLogNeverHadIsAnsweredWithOffsetOutOfRangeInsteadOfADivergence(): void
    {
        // The leader cannot say where an epoch it does not know ends, so it throws OffsetOutOfRangeException
        // instead of reporting a divergence - the partition comes back with the code 1 and a high water mark of -1
        $unknown = $this->fetch(FetchRequest::class, FetchResponse::class, 947, 0, self::LOG_EPOCH + 5);

        self::assertSame(KafkaException::OFFSET_OUT_OF_RANGE, $unknown->errorCode);
        self::assertSame(-1, $unknown->highWaterMarkOffset);
        self::assertNull($unknown->divergingEpoch);
        self::assertSame('', $unknown->messageSet);
    }

    public function testAFetchThatStatesNoEpochIsNeverAnsweredWithADivergence(): void
    {
        $highWaterMark = $this->fetch(FetchRequest::class, FetchResponse::class, 948, 0)->highWaterMarkOffset;

        // The very offset that produced a diverging epoch above, asked without the field: this is what this client
        // and the Java consumer @ 2.8.2 send, and the broker reads the log as it always did
        $beyond = $this->fetch(FetchRequest::class, FetchResponse::class, 949, $highWaterMark + 4);

        self::assertSame(KafkaException::OFFSET_OUT_OF_RANGE, $beyond->errorCode);
        self::assertNull($beyond->divergingEpoch);
        self::assertSame(
            FetchRequestTopicPartition::UNKNOWN_LAST_FETCHED_EPOCH,
            FetchRequest::lastFetchedEpochOf($highWaterMark),
            'a plain offset means "I have read no record of this partition"'
        );
    }

    /**
     * Fetches the single partition of the topic, optionally with the `last_fetched_epoch` of KIP-595
     */
    private function fetch(
        string $requestClass,
        string $responseClass,
        int $correlationId,
        int $fetchOffset,
        ?int $lastFetchedEpoch = null
    ): FetchResponsePartition {
        $position = $lastFetchedEpoch === null
            ? $fetchOffset
            : [$fetchOffset, FetchRequestTopicPartition::UNKNOWN_LEADER_EPOCH, $lastFetchedEpoch];

        $stream = $this->connect();
        new $requestClass(
            [$this->topic => [0 => $position]],
            self::FETCH_MAX_WAIT_MS,
            1,
            65536,
            -1,
            self::CLIENT_ID,
            $correlationId
        )->writeTo($stream);

        return $responseClass::unpack($stream)->topics[$this->topic]->partitions[0];
    }

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
            939
        )->writeTo($stream);

        self::assertSame(
            KafkaException::NO_ERROR,
            ProduceResponse::unpack($stream)->topics[$this->topic]->partitions[0]->errorCode
        );
    }

    /**
     * Deletes the topic of this test through the `kafka-topics.sh` of the container, so that the shared broker
     * does not accumulate the topics of every run
     */
    private static function deleteTopic(string $topic): void
    {
        $output   = [];
        $exitCode = 0;
        exec(
            sprintf(
                'docker exec %s /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete'
                . ' --topic %s 2>&1',
                escapeshellarg(self::container()),
                escapeshellarg($topic)
            ),
            $output,
            $exitCode
        );
    }

    /**
     * Creates a one-partition topic through the `kafka-topics.sh` of the container
     */
    private static function createTopic(string $topic): void
    {
        $output   = [];
        $exitCode = 0;
        exec(
            sprintf(
                'docker exec %s /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create'
                . ' --if-not-exists --topic %s --partitions 1 --replication-factor 1 2>&1',
                escapeshellarg(self::container()),
                escapeshellarg($topic)
            ),
            $output,
            $exitCode
        );

        if ($exitCode !== 0) {
            self::fail("Can not create the topic {$topic}: " . implode("\n", $output));
        }
    }

    /**
     * Name of the container the broker of this line runs in
     */
    private static function container(): string
    {
        $container = getenv('KAFKA_CONTAINER');

        return $container === false || trim($container) === '' ? 'kafka-2-8-2' : trim($container);
    }
}
