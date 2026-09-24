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
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Data\FetchRequestTopic;
use Protocol\Kafka\Protocol\Data\FetchRequestTopicPartition;
use Protocol\Kafka\Protocol\Data\FetchResponsePartition;
use Protocol\Kafka\Protocol\Data\FetchResponseTopic;
use Protocol\Kafka\Protocol\Data\OffsetsRequestPartition;
use Protocol\Kafka\Protocol\Data\OffsetsRequestPartitionV0;
use Protocol\Kafka\Protocol\Data\OffsetsRequestTopic;
use Protocol\Kafka\Protocol\Data\OffsetsRequestTopicV0;
use Protocol\Kafka\Protocol\Data\OffsetsResponsePartition;
use Protocol\Kafka\Protocol\Data\OffsetsResponsePartitionV0;
use Protocol\Kafka\Protocol\Data\OffsetsResponseTopic;
use Protocol\Kafka\Protocol\Data\OffsetsResponseTopicV0;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\FetchRequestV1;
use Protocol\Kafka\Protocol\Request\FetchRequestV4;
use Protocol\Kafka\Protocol\Request\FetchResponseV4;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;
use Protocol\Kafka\Protocol\Request\OffsetsRequestV0;
use Protocol\Kafka\Protocol\Request\OffsetsRequestV1;
use Protocol\Kafka\Protocol\Request\OffsetsRequestV10;
use Protocol\Kafka\Protocol\Request\OffsetsRequestV2;
use Protocol\Kafka\Protocol\Request\OffsetsRequestV3;
use Protocol\Kafka\Protocol\Request\OffsetsRequestV6;
use Protocol\Kafka\Protocol\Request\OffsetsRequestV7;
use Protocol\Kafka\Protocol\Request\OffsetsRequestV8;
use Protocol\Kafka\Protocol\Request\OffsetsRequestV9;
use Protocol\Kafka\Protocol\Request\OffsetsResponse;
use Protocol\Kafka\Protocol\Request\OffsetsResponseV0;
use Protocol\Kafka\Protocol\Request\OffsetsResponseV1;
use Protocol\Kafka\Protocol\Request\OffsetsResponseV2;
use Protocol\Kafka\Protocol\Request\OffsetsResponseV3;
use Protocol\Kafka\Protocol\Request\ProduceRequestV3;
use Protocol\Kafka\Protocol\Request\ProduceResponseV3;
use Protocol\Kafka\Tests\Fixture\RemovedVersionProbe;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * Verifies the Fetch and Offsets APIs against the Kafka 4.3.1 node of this line.
 *
 * On the lines up to 3.x the messages of this class were hand-written Produce v0 bytes read back with a Fetch v1 and
 * a decoder of the message format v0. **Kafka 4.0 removed all three** (KIP-896): Produce v0 to v2, Fetch v0 to v3 and
 * ListOffsets v0 close the connection on a 4.x node. The records are therefore produced as a record batch v2 with a
 * Produce v3 and fetched with a Fetch v4 - the lowest versions the node serves - and the removed ListOffsets v0 is
 * measured as the refusal it is now.
 *
 * @see docs/protocol/4.3.md, sections "Fetch API (key 1, v0 to v18)" and "Offsets API (key 2, v0 to v11),
 *      a.k.a. ListOffset"
 */
#[CoversClass(FetchRequestV4::class)]
#[CoversClass(FetchResponseV4::class)]
#[CoversClass(FetchRequestTopic::class)]
#[CoversClass(FetchRequestTopicPartition::class)]
#[CoversClass(FetchResponseTopic::class)]
#[CoversClass(FetchResponsePartition::class)]
#[CoversClass(OffsetsRequest::class)]
#[CoversClass(OffsetsRequestV0::class)]
#[CoversClass(OffsetsResponse::class)]
#[CoversClass(OffsetsResponseV0::class)]
#[CoversClass(OffsetsRequestTopic::class)]
#[CoversClass(OffsetsRequestTopicV0::class)]
#[CoversClass(OffsetsRequestPartition::class)]
#[CoversClass(OffsetsRequestPartitionV0::class)]
#[CoversClass(OffsetsResponseTopic::class)]
#[CoversClass(OffsetsResponseTopicV0::class)]
#[CoversClass(OffsetsResponsePartition::class)]
#[CoversClass(OffsetsResponsePartitionV0::class)]
final class FetchOffsetsTest extends IntegrationTestCase
{
    /**
     * Client id that every request of this test class sends
     */
    private const string CLIENT_ID = 'kafka-client-t5';

    /**
     * Partition that the tests produce to and fetch from
     */
    private const int PARTITION = 0;

    /**
     * Per-partition MaxBytes that is big enough for every message set of these tests
     */
    private const int MAX_BYTES = 1048576;

    /**
     * How long to wait for an auto-created topic to become writable, in seconds
     */
    private const float TOPIC_TIMEOUT = 30.0;

    /**
     * How long to wait between two attempts at a partition that is not servable yet, `retry.backoff.ms` in style
     */
    private const int RETRY_BACKOFF_MICROSECONDS = 200000;

    /**
     * Error codes of a partition that exists but is not being served by this broker yet
     *
     * A freshly auto-created topic runs through all three of them: the broker knows nothing about the topic (3),
     * the controller has not elected a leader for the partition yet (5), and the elected leader has not finished
     * taking it over (6). None of them is a permanent failure, so a request that meets one is repeated.
     *
     * @var list<int>
     */
    private const array NOT_SERVABLE_YET = [
        KafkaException::UNKNOWN_TOPIC_OR_PARTITION,
        KafkaException::LEADER_NOT_AVAILABLE,
        KafkaException::NOT_LEADER_FOR_PARTITION,
    ];

    public function testFetchFromTheBeginningReturnsEveryProducedMessage(): void
    {
        $stream = $this->connect();
        $topic  = $this->createTopic($stream, 't5-fetch-begin');
        $values = ['first', 'second', 'third', 'fourth', 'fifth'];
        $this->produce($stream, $topic, $values);

        $partition = $this->fetch($stream, $topic, 0);

        self::assertSame(0, $partition->errorCode);
        self::assertSame(5, $partition->highWaterMarkOffset, 'the log end offset is the number of produced messages');
        self::assertSame([0 => 'first', 1 => 'second', 2 => 'third', 3 => 'fourth', 4 => 'fifth'], self::decode($partition));
        self::assertFalse($partition->isSingleMessageTooLarge(0));
    }

    public function testFetchFromTheMiddleReturnsTheMessagesFromThatOffsetOn(): void
    {
        $stream = $this->connect();
        $topic  = $this->createTopic($stream, 't5-fetch-middle');
        $this->produce($stream, $topic, ['first', 'second', 'third', 'fourth', 'fifth']);

        $partition = $this->fetch($stream, $topic, 2);

        // The log holds the five records as ONE record batch, and a broker hands out whole batches: the answer
        // starts at the batch that holds the fetch offset, and the records in front of it are the client's to skip
        self::assertSame(
            [0 => 'first', 1 => 'second', 2 => 'third', 3 => 'fourth', 4 => 'fifth'],
            self::decode($partition),
            'the whole batch comes back, where a message set of the format v0 started at the fetch offset itself'
        );
        self::assertSame(
            [2 => 'third', 3 => 'fourth', 4 => 'fifth'],
            array_filter(self::decode($partition), static fn(int $offset): bool => $offset >= 2, ARRAY_FILTER_USE_KEY)
        );
        self::assertSame(5, $partition->highWaterMarkOffset);
    }

    public function testFetchAtTheEndOfTheLogReturnsAnEmptyMessageSet(): void
    {
        $stream = $this->connect();
        $topic  = $this->createTopic($stream, 't5-fetch-end');
        $this->produce($stream, $topic, ['first', 'second']);

        $partition = $this->fetch($stream, $topic, 2);

        self::assertSame(0, $partition->errorCode);
        self::assertSame('', $partition->messageSet);
        self::assertSame([], self::decode($partition));
        self::assertFalse($partition->isSingleMessageTooLarge(2), 'there is simply nothing left to read');
    }

    public function testABatchBiggerThanMaxBytesIsReturnedWhole(): void
    {
        $stream = $this->connect();
        $topic  = $this->createTopic($stream, 't5-fetch-maxbytes');
        $this->produce($stream, $topic, [str_repeat('x', 4096)]);

        // A 0.9.0.1 broker cut the message set off at MaxBytes and did not guarantee progress, and so did every
        // Fetch below version 3 on the brokers after it. Fetch v4, the lowest version a 4.x node serves, hands the
        // first batch of the answer out whole however small the limit is (KIP-74)
        $partition = $this->fetch($stream, $topic, 0, maxBytes: 64);

        self::assertSame(0, $partition->errorCode, 'an oversized batch is not an error of the partition');
        self::assertGreaterThan(4096, strlen((string) $partition->messageSet));
        self::assertSame([0 => str_repeat('x', 4096)], self::decode($partition));
        self::assertSame(1, $partition->highWaterMarkOffset);
        self::assertFalse($partition->isSingleMessageTooLarge(0));

        // ... and the Fetch v1 that cut the message off costs the connection
        self::assertSame(
            RemovedVersionProbe::CLOSED,
            new RemovedVersionProbe(self::firstBootstrapServer())->send(
                new FetchRequestV1([$topic => [self::PARTITION => 0]], 1000, 1, 64, -1, self::CLIENT_ID, 13)
            )
        );
    }

    public function testFetchOfAnEmptyLogBlocksUntilMaxWaitTimeIsOver(): void
    {
        $stream = $this->connect();
        $topic  = $this->createTopic($stream, 't5-fetch-longpoll');
        $this->produce($stream, $topic, ['first']);

        // MinBytes = 1 with nothing to read: the broker holds the request for the whole MaxWaitTime
        $startedAt = microtime(true);
        $partition = $this->fetch($stream, $topic, 1, maxWaitTime: 1500, minBytes: 1);
        $elapsedMs = (microtime(true) - $startedAt) * 1000;

        self::assertSame('', $partition->messageSet);
        self::assertGreaterThan(1000, $elapsedMs, 'the long poll has to wait for MaxWaitTime');
        self::assertLessThan(5000, $elapsedMs);
    }

    public function testFetchWithMinBytesZeroReturnsImmediately(): void
    {
        $stream = $this->connect();
        $topic  = $this->createTopic($stream, 't5-fetch-nopoll');
        $this->produce($stream, $topic, ['first']);

        // MinBytes = 0 makes the broker answer at once, even though MaxWaitTime would allow it to wait
        $startedAt = microtime(true);
        $partition = $this->fetch($stream, $topic, 1, maxWaitTime: 5000, minBytes: 0);
        $elapsedMs = (microtime(true) - $startedAt) * 1000;

        self::assertSame('', $partition->messageSet);
        self::assertLessThan(1500, $elapsedMs, 'nothing has to be accumulated, so nothing is waited for');
    }

    public function testFetchAtAnOffsetPastTheEndOfTheLogFails(): void
    {
        $stream = $this->connect();
        $topic  = $this->createTopic($stream, 't5-fetch-out-of-range');
        $this->produce($stream, $topic, ['first', 'second']);

        $partition = $this->fetch($stream, $topic, 1000);

        self::assertSame(KafkaException::OFFSET_OUT_OF_RANGE, $partition->errorCode);
        self::assertSame(-1, $partition->highWaterMarkOffset, 'a failed partition has no high water mark');
    }

    public function testEarliestAndLatestOffsetsOfALogWithMessages(): void
    {
        $stream = $this->connect();
        $topic  = $this->createTopic($stream, 't5-offsets');
        $this->produce($stream, $topic, ['first', 'second', 'third']);

        $earliest = $this->listOffsets($stream, $topic, OffsetsRequest::EARLIEST);
        $latest   = $this->listOffsets($stream, $topic, OffsetsRequest::LATEST);

        self::assertSame(0, $earliest->errorCode);
        self::assertSame(0, $earliest->offset, 'nothing has been deleted, so the log starts at offset 0');
        self::assertSame(0, $latest->errorCode);
        self::assertSame(3, $latest->offset, 'the latest offset is the one the next message will get');
        self::assertSame(
            OffsetsResponsePartition::UNKNOWN_TIMESTAMP,
            $latest->timestamp,
            'the two special target times never read a message, so their answer has no timestamp'
        );
    }

    public function testLatestOffsetOfAnEmptyLogIsZero(): void
    {
        $stream = $this->connect();
        $topic  = $this->createTopic($stream, 't5-offsets-empty');

        self::assertSame(0, $this->listOffsets($stream, $topic, OffsetsRequest::LATEST)->offset);
        self::assertSame(0, $this->listOffsets($stream, $topic, OffsetsRequest::EARLIEST)->offset);
    }

    public function testOffsetsOfAnUnknownPartitionFail(): void
    {
        $stream = $this->connect();
        $topic  = $this->createTopic($stream, 't5-offsets-unknown');

        // The topic is created with 3 partitions, so partition 42 does not exist
        new OffsetsRequest([$topic => [42 => OffsetsRequest::LATEST]], -1, FetchRequest::READ_UNCOMMITTED, self::CLIENT_ID, 21)->writeTo($stream);
        $response = OffsetsResponse::unpack($stream);

        self::assertSame(21, $response->getCorrelationId());

        $partition = $response->topics[$topic]->partitions[42];
        self::assertSame(KafkaException::UNKNOWN_TOPIC_OR_PARTITION, $partition->errorCode);
        self::assertSame(OffsetsResponsePartition::UNKNOWN_OFFSET, $partition->offset);
        self::assertSame(OffsetsResponsePartition::UNKNOWN_TIMESTAMP, $partition->timestamp);
    }

    public function testOffsetsOfSeveralPartitionsComeBackInOneResponse(): void
    {
        $stream = $this->connect();
        $topic  = $this->createTopic($stream, 't5-offsets-multi');
        $this->produce($stream, $topic, ['first', 'second']);

        new OffsetsRequest(
            [$topic => [0 => OffsetsRequest::LATEST, 1 => OffsetsRequest::LATEST, 2 => OffsetsRequest::LATEST]],
            -1,
            FetchRequest::READ_UNCOMMITTED,
            self::CLIENT_ID,
            22
        )->writeTo($stream);
        $response = OffsetsResponse::unpack($stream);

        $partitions = $response->topics[$topic]->partitions;
        ksort($partitions);
        self::assertSame([0, 1, 2], array_keys($partitions), 'the response is indexed by the partition id');
        self::assertSame(2, $partitions[0]->offset);
        self::assertSame(0, $partitions[1]->offset, 'nothing was produced to the other partitions');
        self::assertSame(0, $partitions[2]->offset);
    }

    public function testVersionZeroOfTheOffsetsApiClosesTheConnection(): void
    {
        $stream = $this->connect();
        $topic  = $this->createTopic($stream, 't5-offsets-v0');
        $this->produce($stream, $topic, ['first', 'second', 'third']);

        // `ListOffsetsRequest.json` @ 4.0.0: "Version 0 was removed in Apache Kafka 4.0, Version 1 is the new
        // baseline" - the list of segment offsets a 3.9.2 node still answered is gone with it (KIP-896)
        self::assertSame(
            RemovedVersionProbe::CLOSED,
            new RemovedVersionProbe(self::firstBootstrapServer())->send(new OffsetsRequestV0(
                [$topic => [self::PARTITION => OffsetsRequest::LATEST]],
                5,
                -1,
                self::CLIENT_ID,
                23
            ))
        );

        // The lowest version the node serves answers one offset per partition
        new OffsetsRequestV1([$topic => [self::PARTITION => OffsetsRequest::LATEST]], -1, 0, self::CLIENT_ID, 24)
            ->writeTo($stream);
        $partition = OffsetsResponseV1::unpack($stream)->topics[$topic]->partitions[self::PARTITION];

        self::assertSame(0, $partition->errorCode);
        self::assertSame(3, $partition->offset, 'the log end offset, where version 0 answered the list [3, 0]');
    }

    public function testTheVersionsTwoAndThreeAskTheSameQuestionAndGetTheSameAnswer(): void
    {
        // `ListOffsetsRequest.json` and `ListOffsetsResponse.json` @ 2.8.2 both say "Version 3 is the same as
        // version 2": what version 3 (Kafka 2.0, KIP-219) states is that the client waits out the throttle time
        // of the answer itself.
        $stream = $this->connect();
        $topic  = $this->createTopic($stream, 't5-offsets-v3');
        $this->produce($stream, $topic, ['first', 'second']);

        new OffsetsRequestV2(
            [$topic => [self::PARTITION => OffsetsRequest::LATEST]],
            -1,
            FetchRequest::READ_UNCOMMITTED,
            self::CLIENT_ID,
            24
        )->writeTo($stream);
        $versionTwo = OffsetsResponseV2::unpack($stream);

        new OffsetsRequestV3(
            [$topic => [self::PARTITION => OffsetsRequest::LATEST]],
            -1,
            FetchRequest::READ_UNCOMMITTED,
            self::CLIENT_ID,
            25
        )->writeTo($stream);
        $versionThree = OffsetsResponseV3::unpack($stream);

        self::assertSame(3, OffsetsRequestV3::VERSION, 'the version Kafka 2.0 added');
        self::assertSame(6, OffsetsRequestV6::VERSION, 'the flexible version Kafka 2.8 added');
        self::assertSame(7, OffsetsRequestV7::VERSION, 'the version Kafka 3.0 added');
        self::assertSame(8, OffsetsRequestV8::VERSION, 'the version Kafka 3.5 added');
        self::assertSame(9, OffsetsRequestV9::VERSION, 'the version Kafka 3.9 added');
        self::assertSame(10, OffsetsRequestV10::VERSION, 'the version Kafka 4.0 added (KIP-1075)');
        self::assertSame(11, OffsetsRequest::VERSION, 'and the version Kafka 4.2 added (KIP-1023)');
        self::assertSame($versionTwo->getMessageSize(), $versionThree->getMessageSize());
        self::assertSame(0, $versionThree->throttleTimeMs, 'no quota is set for this client id');

        $two   = $versionTwo->topics[$topic]->partitions[self::PARTITION];
        $three = $versionThree->topics[$topic]->partitions[self::PARTITION];
        self::assertSame(0, $three->errorCode);
        self::assertSame($two->offset, $three->offset);
        self::assertSame(2, $three->offset, 'the log end offset of the two produced records');
        self::assertSame($two->timestamp, $three->timestamp);
        self::assertSame(OffsetsResponsePartition::UNKNOWN_TIMESTAMP, $three->timestamp);
    }

    /**
     * Creates a topic and waits until the partition under test really serves requests
     *
     * Auto-creation is asynchronous and happens in two steps that a client sees separately. Asking for the metadata
     * of an unknown topic creates it, but the controller elects the leaders of its partitions afterwards, so the
     * metadata announces the topic without a leader for a while ({@see TopicMetadataProbe} waits for that). A broker
     * that has just been made the leader of a partition still needs a moment to start serving it, and answers
     * UnknownTopicOrPartition (3), LeaderNotAvailable (5) or NotLeaderForPartition (6) in between - which is what a
     * cold broker does after the metadata already looks good, so the first request is retried as well.
     */
    private function createTopic(SocketStream $stream, string $prefix): string
    {
        $topic = self::uniqueTopicName($prefix);
        new TopicMetadataProbe(fn(): Stream => $this->connect(), self::TOPIC_TIMEOUT, self::CLIENT_ID)
            ->awaitTopicWithLeaders($topic);

        // listOffsets() itself waits for a partition that is not servable yet, so this is the second step
        self::assertSame(0, $this->listOffsets($stream, $topic, OffsetsRequest::LATEST)->errorCode);

        return $topic;
    }

    /**
     * Repeats a request while the partition it addresses is not servable yet
     *
     * Only the error codes of a partition that is still being handed over are retried; every other answer, the
     * successful one and the failures that the tests assert on alike, is given back as it is.
     *
     * @param \Closure(): (FetchResponsePartition|OffsetsResponsePartition) $request Request to repeat
     */
    private function awaitServablePartition(
        string $topic,
        \Closure $request
    ): FetchResponsePartition|OffsetsResponsePartition {
        $deadline = microtime(true) + self::TOPIC_TIMEOUT;

        do {
            $partition = $request();
            if (!in_array($partition->errorCode, self::NOT_SERVABLE_YET, true)) {
                return $partition;
            }
            usleep(self::RETRY_BACKOFF_MICROSECONDS);
        } while (microtime(true) < $deadline);

        self::fail(sprintf(
            'The partition %s-%d still answered with the error code %d after %.0f seconds',
            $topic,
            self::PARTITION,
            $partition->errorCode,
            self::TOPIC_TIMEOUT
        ));
    }

    /**
     * Produces the given values to the partition under test, retrying while the fresh topic has no leader yet
     *
     * One record batch of the message format v2 in a Produce **v3**, the lowest version a 4.x node serves.
     *
     * @param list<string> $values Values of the records to produce, in order
     */
    private function produce(SocketStream $stream, string $topic, array $values): void
    {
        $now     = (int) round(microtime(true) * 1000);
        $records = array_map(static fn(string $value): Record => new Record($value)->withCreateTime($now), $values);
        $batch   = RecordBatch::fromRecords($records)->toBuffer();

        $deadline = microtime(true) + self::TOPIC_TIMEOUT;
        do {
            new ProduceRequestV3([$topic => [self::PARTITION => $batch]], 1, 5000, self::CLIENT_ID, 2)->writeTo($stream);

            $errorCode = ProduceResponseV3::unpack($stream)->topics[$topic]->partitions[self::PARTITION]->errorCode;
            if ($errorCode === 0) {
                return;
            }
            // The auto-created topic may still be electing a leader for its partitions, or handing one over
            self::assertContains(
                $errorCode,
                self::NOT_SERVABLE_YET,
                "The broker refused to accept the records of {$topic} with error code {$errorCode}"
            );
            usleep(self::RETRY_BACKOFF_MICROSECONDS);
        } while (microtime(true) < $deadline);

        self::fail("The partition {$topic}-" . self::PARTITION . ' did not get a leader in time');
    }

    /**
     * Fetches the partition under test and returns its part of the response
     */
    private function fetch(
        SocketStream $stream,
        string $topic,
        int $fetchOffset,
        int $maxBytes = self::MAX_BYTES,
        int $maxWaitTime = 1000,
        int $minBytes = 1
    ): FetchResponsePartition {
        return $this->awaitServablePartition($topic, function () use (
            $stream,
            $topic,
            $fetchOffset,
            $maxBytes,
            $maxWaitTime,
            $minBytes
        ): FetchResponsePartition {
            new FetchRequestV4(
                [$topic => [self::PARTITION => $fetchOffset]],
                $maxWaitTime,
                $minBytes,
                $maxBytes,
                -1,
                self::CLIENT_ID,
                11
            )->writeTo($stream);

            $response = FetchResponseV4::unpack($stream);
            self::assertSame(11, $response->getCorrelationId());
            self::assertArrayHasKey($topic, $response->topics);

            return $response->topics[$topic]->partitions[self::PARTITION];
        });
    }

    /**
     * Lists the offsets of the partition under test for the given target time
     */
    private function listOffsets(SocketStream $stream, string $topic, int $timestamp): OffsetsResponsePartition
    {
        return $this->awaitServablePartition(
            $topic,
            function () use ($stream, $topic, $timestamp): OffsetsResponsePartition {
                new OffsetsRequest(
                    [$topic => [self::PARTITION => $timestamp]],
                    -1,
                    FetchRequest::READ_UNCOMMITTED,
                    self::CLIENT_ID,
                    12
                )->writeTo($stream);

                $response = OffsetsResponse::unpack($stream);
                self::assertSame(12, $response->getCorrelationId());
                self::assertArrayHasKey($topic, $response->topics);

                return $response->topics[$topic]->partitions[self::PARTITION];
            }
        );
    }

    /**
     * Decodes the record batches of a partition and returns the values of the records indexed by their offset
     *
     * @return array<int, string>
     */
    private static function decode(FetchResponsePartition $partition): array
    {
        $values = [];
        foreach ($partition->getRecords()->getRecords() as $record) {
            $values[(int) $record->offset] = (string) $record->value;
        }

        return $values;
    }
}
