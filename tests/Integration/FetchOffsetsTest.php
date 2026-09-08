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
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\FetchRequestTopic;
use Protocol\Kafka\Protocol\Data\FetchRequestTopicPartition;
use Protocol\Kafka\Protocol\Data\FetchResponsePartition;
use Protocol\Kafka\Protocol\Data\FetchResponseTopic;
use Protocol\Kafka\Protocol\Data\OffsetsRequestPartition;
use Protocol\Kafka\Protocol\Data\OffsetsRequestTopic;
use Protocol\Kafka\Protocol\Data\OffsetsResponsePartition;
use Protocol\Kafka\Protocol\Data\OffsetsResponseTopic;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\FetchResponse;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;
use Protocol\Kafka\Protocol\Request\OffsetsResponse;
use Protocol\Kafka\Tests\Fixture\ClusterMetadataResponse;

/**
 * Verifies the Fetch and Offsets APIs against a real Kafka 0.8.2.2 broker.
 *
 * The messages are produced with hand-written Produce v0 bytes, so that these tests only depend on the wire format
 * of the spec and not on the state of the other protocol classes.
 *
 * @see docs/protocol/0.8.2.md, sections "Fetch API (key 1, v0)" and "Offsets API (key 2, v0), a.k.a. ListOffset"
 */
#[CoversClass(FetchRequest::class)]
#[CoversClass(FetchResponse::class)]
#[CoversClass(FetchRequestTopic::class)]
#[CoversClass(FetchRequestTopicPartition::class)]
#[CoversClass(FetchResponseTopic::class)]
#[CoversClass(FetchResponsePartition::class)]
#[CoversClass(OffsetsRequest::class)]
#[CoversClass(OffsetsResponse::class)]
#[CoversClass(OffsetsRequestTopic::class)]
#[CoversClass(OffsetsRequestPartition::class)]
#[CoversClass(OffsetsResponseTopic::class)]
#[CoversClass(OffsetsResponsePartition::class)]
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

        self::assertSame([2 => 'third', 3 => 'fourth', 4 => 'fifth'], self::decode($partition));
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

    public function testMessageBiggerThanMaxBytesComesBackAsAnEmptyMessageSet(): void
    {
        $stream = $this->connect();
        $topic  = $this->createTopic($stream, 't5-fetch-maxbytes');
        $this->produce($stream, $topic, [str_repeat('x', 4096)]);

        // A 0.8.2.2 broker cuts the message set off at MaxBytes and does not guarantee progress, unlike the later
        // protocol versions: what comes back are the first 64 bytes of a message that is 4110 bytes long
        $partition = $this->fetch($stream, $topic, 0, maxBytes: 64);

        self::assertSame(0, $partition->errorCode, 'an oversized message is not an error of the partition');
        self::assertLessThanOrEqual(64, strlen((string) $partition->messageSet));
        self::assertSame([], self::decode($partition), 'the partial trailing message is dropped');
        self::assertSame(1, $partition->highWaterMarkOffset);
        self::assertTrue($partition->isSingleMessageTooLarge(0));

        // The very same fetch with enough room returns the message
        self::assertSame([0 => str_repeat('x', 4096)], self::decode($this->fetch($stream, $topic, 0)));
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
        self::assertSame([0], $earliest->offsets, 'nothing has been deleted, so the log starts at offset 0');
        self::assertSame(0, $latest->errorCode);
        self::assertSame([3], $latest->offsets, 'the latest offset is the one the next message will get');
    }

    public function testLatestOffsetOfAnEmptyLogIsZero(): void
    {
        $stream = $this->connect();
        $topic  = $this->createTopic($stream, 't5-offsets-empty');

        self::assertSame([0], $this->listOffsets($stream, $topic, OffsetsRequest::LATEST)->offsets);
        self::assertSame([0], $this->listOffsets($stream, $topic, OffsetsRequest::EARLIEST)->offsets);
    }

    public function testOffsetsOfAnUnknownPartitionFail(): void
    {
        $stream = $this->connect();
        $topic  = $this->createTopic($stream, 't5-offsets-unknown');

        // The topic is created with 3 partitions, so partition 42 does not exist
        new OffsetsRequest([$topic => [42 => OffsetsRequest::LATEST]], 1, -1, self::CLIENT_ID, 21)->writeTo($stream);
        $response = OffsetsResponse::unpack($stream);

        self::assertSame(21, $response->getCorrelationId());
        self::assertSame(
            KafkaException::UNKNOWN_TOPIC_OR_PARTITION,
            $response->topics[$topic]->partitions[42]->errorCode
        );
    }

    public function testOffsetsOfSeveralPartitionsComeBackInOneResponse(): void
    {
        $stream = $this->connect();
        $topic  = $this->createTopic($stream, 't5-offsets-multi');
        $this->produce($stream, $topic, ['first', 'second']);

        new OffsetsRequest(
            [$topic => [0 => OffsetsRequest::LATEST, 1 => OffsetsRequest::LATEST, 2 => OffsetsRequest::LATEST]],
            1,
            -1,
            self::CLIENT_ID,
            22
        )->writeTo($stream);
        $response = OffsetsResponse::unpack($stream);

        $partitions = $response->topics[$topic]->partitions;
        self::assertSame([0, 1, 2], array_keys($partitions), 'the response is indexed by the partition id');
        self::assertSame([2], $partitions[0]->offsets);
        self::assertSame([0], $partitions[1]->offsets, 'nothing was produced to the other partitions');
        self::assertSame([0], $partitions[2]->offsets);
    }

    /**
     * Creates a topic through a metadata request and waits until the partition under test can be queried
     *
     * Auto-creation is asynchronous: until the controller has assigned a leader to the new partitions the broker
     * answers with UnknownTopicOrPartition (3), LeaderNotAvailable (5) or, once a leader has been elected but this
     * broker has not been told about it yet, NotLeaderForPartition (6).
     */
    private function createTopic(SocketStream $stream, string $prefix): string
    {
        $topic = self::uniqueTopicName($prefix);
        new MetadataRequest([$topic], self::CLIENT_ID, 1)->writeTo($stream);
        ClusterMetadataResponse::unpack($stream);

        $deadline = microtime(true) + self::TOPIC_TIMEOUT;
        do {
            $errorCode = $this->listOffsets($stream, $topic, OffsetsRequest::LATEST)->errorCode;
            if ($errorCode === 0) {
                return $topic;
            }
            self::assertContains(
                $errorCode,
                [
                    KafkaException::UNKNOWN_TOPIC_OR_PARTITION,
                    KafkaException::LEADER_NOT_AVAILABLE,
                    KafkaException::NOT_LEADER_FOR_PARTITION,
                ],
                "The broker answered with error code {$errorCode} for the fresh topic {$topic}"
            );
            usleep(200000);
        } while (microtime(true) < $deadline);

        self::fail("The partition {$topic}-" . self::PARTITION . ' did not become available in time');
    }

    /**
     * Produces the given values to the partition under test, retrying while the fresh topic has no leader yet
     *
     * @param list<string> $values Values of the messages to produce, in order
     */
    private function produce(SocketStream $stream, string $topic, array $values): void
    {
        $messageSet = '';
        foreach ($values as $offset => $value) {
            $message    = pack('ccN', 0 /* MagicByte */, 0 /* Attributes */, 0xFFFFFFFF /* null Key */)
                . pack('N', strlen($value)) . $value;
            $message    = pack('N', crc32($message)) . $message;
            $messageSet .= pack('JN', $offset, strlen($message)) . $message;
        }

        $body = pack('nN', 1 /* RequiredAcks */, 5000 /* Timeout */)
            . pack('N', 1) . pack('n', strlen($topic)) . $topic
            . pack('N', 1) . pack('N', self::PARTITION) . pack('N', strlen($messageSet)) . $messageSet;

        $deadline = microtime(true) + self::TOPIC_TIMEOUT;
        do {
            $frame = pack('nnN', ApiKeys::PRODUCE, 0, 2)
                . pack('n', strlen(self::CLIENT_ID)) . self::CLIENT_ID
                . $body;
            $stream->writeBuffer(pack('N', strlen($frame)) . $frame);

            $errorCode = self::readProduceErrorCode($stream);
            if ($errorCode === 0) {
                return;
            }
            // The auto-created topic may still be electing a leader for its partitions
            self::assertContains(
                $errorCode,
                [
                    KafkaException::UNKNOWN_TOPIC_OR_PARTITION,
                    KafkaException::LEADER_NOT_AVAILABLE,
                    KafkaException::NOT_LEADER_FOR_PARTITION,
                ],
                "The broker refused to accept the messages of {$topic} with error code {$errorCode}"
            );
            usleep(200000);
        } while (microtime(true) < $deadline);

        self::fail("The partition {$topic}-" . self::PARTITION . ' did not get a leader in time');
    }

    /**
     * Reads a Produce response v0 and returns the error code of its only partition
     */
    private static function readProduceErrorCode(SocketStream $stream): int
    {
        $messageSize = $stream->read('NmessageSize')['messageSize'];
        $payload     = new StringStream((string) $stream->read("a{$messageSize}data")['data']);

        $payload->read('NcorrelationId/NnumberOfTopics');
        $topicLength = $payload->read('ntopicLength')['topicLength'];
        $payload->read("a{$topicLength}topic/NnumberOfPartitions");
        ['errorCode' => $errorCode] = $payload->read('Npartition/nerrorCode/Joffset');

        return $errorCode > 0x7FFF ? $errorCode - 0x10000 : $errorCode;
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
        new FetchRequest(
            [$topic => [self::PARTITION => $fetchOffset]],
            $maxWaitTime,
            $minBytes,
            $maxBytes,
            -1,
            self::CLIENT_ID,
            11
        )->writeTo($stream);

        $response = FetchResponse::unpack($stream);
        self::assertSame(11, $response->getCorrelationId());
        self::assertArrayHasKey($topic, $response->topics);

        return $response->topics[$topic]->partitions[self::PARTITION];
    }

    /**
     * Lists the offsets of the partition under test for the given target time
     */
    private function listOffsets(SocketStream $stream, string $topic, int $timestamp): OffsetsResponsePartition
    {
        new OffsetsRequest(
            [$topic => [self::PARTITION => $timestamp]],
            1,
            -1,
            self::CLIENT_ID,
            12
        )->writeTo($stream);

        $response = OffsetsResponse::unpack($stream);
        self::assertSame(12, $response->getCorrelationId());
        self::assertArrayHasKey($topic, $response->topics);

        return $response->topics[$topic]->partitions[self::PARTITION];
    }

    /**
     * Decodes the raw message set of a partition, dropping the partial message that the broker may have left at its
     * end, and returns the values of the messages indexed by their offset
     *
     * @return array<int, string>
     */
    private static function decode(FetchResponsePartition $partition): array
    {
        $buffer     = $partition->messageSet ?? '';
        $bufferSize = strlen($buffer);
        $values     = [];

        for ($position = 0; $position + 12 <= $bufferSize; $position += 12 + $messageSize) {
            ['offset' => $offset, 'size' => $messageSize] = (array) unpack('Joffset/Nsize', $buffer, $position);
            if ($position + 12 + $messageSize > $bufferSize) {
                break; // partial trailing message
            }
            $message   = substr($buffer, $position + 12, $messageSize);
            $keyLength = (int) unpack('Nlength', $message, 6)['length'];
            $valueAt   = 10 + ($keyLength === 0xFFFFFFFF ? 0 : $keyLength);

            $values[$offset] = substr($message, $valueAt + 4, (int) unpack('Nlength', $message, $valueAt)['length']);
        }

        return $values;
    }
}
