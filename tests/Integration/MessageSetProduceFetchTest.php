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
use PHPUnit\Framework\Attributes\DataProvider;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Record\CompressionCodec;
use Protocol\Kafka\Common\Record\Lz4;
use Protocol\Kafka\Common\Record\Message;
use Protocol\Kafka\Common\Record\MessageSet;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\Snappy;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Data\FetchResponsePartition;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\FetchRequestV0;
use Protocol\Kafka\Protocol\Request\FetchResponse;
use Protocol\Kafka\Protocol\Request\FetchResponseV0;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceResponse;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * Produces message sets to a real Kafka 0.10.2.2 broker and fetches them back.
 *
 * The broker is the authority on the message format: it validates the checksum of every message it appends, it
 * decompresses a compressed set to assign the offsets of its inner messages, and it recompresses it with the codec
 * the producer chose. A set that survives this round trip is a set that Kafka itself accepts.
 *
 * The batches of this suite are written in message format v1 - the default of the producer - and read back with a
 * Fetch request of version 1, which makes the broker convert its answer down to message format v0: the values, the
 * keys and the offsets survive that conversion, the timestamps do not. What the log really holds and what a Fetch
 * v2 request answers is the subject of {@see MessageFormatV1Test}.
 *
 * @see docs/protocol/0.10.2.md, section "MessageSet and Message"
 */
#[CoversClass(MessageSet::class)]
#[CoversClass(Message::class)]
#[CoversClass(CompressionCodec::class)]
#[CoversClass(Snappy::class)]
#[CoversClass(Lz4::class)]
#[CoversClass(FetchRequestV0::class)]
#[CoversClass(FetchResponseV0::class)]
#[CoversClass(FetchResponsePartition::class)]
final class MessageSetProduceFetchTest extends IntegrationTestCase
{
    /**
     * Client id that identifies the requests of this test in the logs of the broker
     */
    private const string CLIENT_ID = 'kafka-client-t3';

    /**
     * Partition that every test of this class produces to and fetches from
     */
    private const int PARTITION = 0;

    /**
     * How long the broker may take to acknowledge a produce request, in milliseconds
     */
    private const int PRODUCE_TIMEOUT_MS = 5000;

    /**
     * Topic of the current test, created and given a leader by {@see MessageSetProduceFetchTest::setUp()}
     */
    private string $topic;

    protected function setUp(): void
    {
        parent::setUp();

        $this->topic = self::uniqueTopicName('t3-message-set');
        new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, self::CLIENT_ID)
            ->awaitTopicWithLeaders($this->topic);
    }

    /**
     * @return iterable<string, array{0: int}>
     */
    public static function compressionCodecs(): iterable
    {
        yield 'uncompressed' => [CompressionCodec::NONE];
        yield 'gzip'         => [CompressionCodec::GZIP];
        yield 'snappy'       => [CompressionCodec::SNAPPY];
        yield 'lz4'          => [CompressionCodec::LZ4];
    }

    #[DataProvider('compressionCodecs')]
    public function testAMessageSetSurvivesTheRoundTripThroughTheBroker(int $codec): void
    {
        $records = [
            new Record('alpha', 'first'),
            new Record('bravo'),
            new Record(str_repeat('a larger payload that compresses well. ', 100), 'third'),
        ];

        $messageSet = MessageSet::fromRecords($records, $codec);
        // The offsets of a produced set always count from 0; producing it twice shows that the broker replaces them,
        // inside a compressed wrapper as well
        $this->produce($messageSet);
        $baseOffset = $this->produce($messageSet);
        $fetched    = $this->fetch($baseOffset);

        self::assertSame(3, $baseOffset, 'the second set is appended after the three messages of the first one');
        self::assertCount(3, $fetched);
        self::assertSame(['alpha', 'bravo', $records[2]->value], array_map(
            static fn(Record $record): ?string => $record->value,
            $fetched
        ));
        self::assertSame(['first', null, 'third'], array_map(
            static fn(Record $record): ?string => $record->key,
            $fetched
        ));
        self::assertSame(
            [$baseOffset, $baseOffset + 1, $baseOffset + 2],
            array_map(static fn(Record $record): ?int => $record->offset, $fetched),
            'the broker assigns consecutive offsets, inner messages of a compressed set included'
        );
    }

    public function testANullValueSurvivesTheRoundTripThroughTheBroker(): void
    {
        $baseOffset = $this->produce(MessageSet::fromRecords([
            new Record(null, 'tombstone'),
            new Record('', 'empty'),
            new Record('value', null),
        ]));
        $fetched = $this->fetch($baseOffset);

        self::assertCount(3, $fetched);
        self::assertNull($fetched[0]->value, 'a null value is not the same thing as an empty one');
        self::assertSame('tombstone', $fetched[0]->key);
        self::assertSame('', $fetched[1]->value);
        self::assertSame('value', $fetched[2]->value);
        self::assertNull($fetched[2]->key);
    }

    public function testAFetchThatDoesNotFitTheFirstMessageComesBackWithoutRecords(): void
    {
        $baseOffset = $this->produce(MessageSet::fromRecords([new Record(str_repeat('x', 4096))]));

        // MaxBytes below the size of the first message: the broker answers with a message it cut short
        $partition = $this->fetchPartition($baseOffset, 64);

        self::assertSame([], $partition->getMessageSet()->getRecords(), 'the partial message is dropped');
        self::assertTrue($partition->getMessageSet()->hasPartialTrailingMessage());
        self::assertTrue($partition->isSingleMessageTooLarge($baseOffset));
        self::assertCount(1, $this->fetch($baseOffset));
    }

    public function testTheBrokerAcceptsTheChecksumsThisClientComputes(): void
    {
        // A corrupt checksum would make the broker answer with error code 2 instead of a base offset
        $offset = $this->produce(MessageSet::fromRecords([new Record('bar', 'foo')]));

        self::assertGreaterThanOrEqual(0, $offset);
    }

    public function testVersion1FetchAnswerIsPrefixedWithAThrottleTimeAndVersion0IsNot(): void
    {
        $baseOffset = $this->produce(MessageSet::fromRecords([new Record('throttle', 'probe')]));
        $stream     = $this->connect();

        new FetchRequest([$this->topic => [self::PARTITION => $baseOffset]], 1000, 1, 65536, -1, self::CLIENT_ID, 51)
            ->writeTo($stream);
        $versionOne = FetchResponse::unpack($stream);

        self::assertSame(51, $versionOne->getCorrelationId());
        self::assertSame(0, $versionOne->throttleTimeMs, 'the test broker enforces no consumer quota');

        new FetchRequestV0([$this->topic => [self::PARTITION => $baseOffset]], 1000, 1, 65536, -1, self::CLIENT_ID, 52)
            ->writeTo($stream);
        $versionZero = FetchResponseV0::unpack($stream);

        self::assertSame(52, $versionZero->getCorrelationId());
        self::assertSame(
            $versionOne->getMessageSize() - 4,
            $versionZero->getMessageSize(),
            'the ThrottleTimeMs prefix of version 1 is the only difference between the two answers'
        );
        self::assertSame(
            bin2hex((string) $versionOne->topics[$this->topic]->partitions[self::PARTITION]->messageSet),
            bin2hex((string) $versionZero->topics[$this->topic]->partitions[self::PARTITION]->messageSet)
        );
    }

    /**
     * Produces the message set into the partition under test and returns the offset of its first message
     */
    private function produce(MessageSet $messageSet): int
    {
        $stream = $this->connect();
        new ProduceRequest(
            [$this->topic => [self::PARTITION => $messageSet]],
            1,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            1
        )->writeTo($stream);

        $partition = ProduceResponse::unpack($stream)->topics[$this->topic]->partitions[self::PARTITION];
        if ($partition->errorCode !== 0) {
            throw KafkaException::fromCode($partition->errorCode, ['topic' => $this->topic, 'partitionId' => self::PARTITION]);
        }

        return $partition->baseOffset;
    }

    /**
     * Fetches the partition under test from the given offset and returns the records it holds
     *
     * @return list<Record>
     */
    private function fetch(int $offset, int $maxBytes = 65536): array
    {
        return $this->fetchPartition($offset, $maxBytes)->getMessageSet()->getRecords();
    }

    /**
     * Fetches the partition under test from the given offset
     */
    private function fetchPartition(int $offset, int $maxBytes = 65536): FetchResponsePartition
    {
        $stream = $this->connect();
        new FetchRequest([$this->topic => [self::PARTITION => $offset]], 1000, 1, $maxBytes, -1, self::CLIENT_ID, 2)
            ->writeTo($stream);

        $partition = FetchResponse::unpack($stream)->topics[$this->topic]->partitions[self::PARTITION];
        if ($partition->errorCode !== 0) {
            throw KafkaException::fromCode($partition->errorCode, ['topic' => $this->topic, 'partitionId' => self::PARTITION]);
        }

        return $partition;
    }
}
