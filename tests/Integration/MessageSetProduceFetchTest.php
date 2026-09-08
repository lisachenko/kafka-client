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
use Protocol\Kafka\Common\Record\Message;
use Protocol\Kafka\Common\Record\MessageSet;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\Snappy;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Data\FetchResponsePartition;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\FetchResponse;
use Protocol\Kafka\Protocol\Request\ProduceResponse;
use Protocol\Kafka\Tests\Fixture\ClusterReadinessProbe;
use Protocol\Kafka\Tests\Fixture\MessageSetProduceRequest;

/**
 * Produces message sets to a real Kafka 0.8.2.2 broker and fetches them back.
 *
 * The broker is the authority on the message format: it validates the checksum of every message it appends, it
 * decompresses a compressed set to assign the offsets of its inner messages, and it recompresses it with the codec
 * the producer chose. A set that survives this round trip is a set that Kafka itself accepts.
 *
 * @see docs/protocol/0.8.2.md, section "MessageSet and Message"
 */
#[CoversClass(MessageSet::class)]
#[CoversClass(Message::class)]
#[CoversClass(CompressionCodec::class)]
#[CoversClass(Snappy::class)]
final class MessageSetProduceFetchTest extends IntegrationTestCase
{
    /**
     * Client id that identifies the requests of this test in the logs of the broker
     */
    private const string CLIENT_ID = 'kafka-client-t3';

    /**
     * How long to keep retrying a produce while the freshly created topic has no leader yet, in seconds
     */
    private const float LEADER_TIMEOUT = 30.0;

    /**
     * @return iterable<string, array{0: int}>
     */
    public static function compressionCodecs(): iterable
    {
        yield 'uncompressed' => [CompressionCodec::NONE];
        yield 'gzip'         => [CompressionCodec::GZIP];
        yield 'snappy'       => [CompressionCodec::SNAPPY];
    }

    #[DataProvider('compressionCodecs')]
    public function testAMessageSetSurvivesTheRoundTripThroughTheBroker(int $codec): void
    {
        $topic   = $this->createTopic('t3-message-set');
        $records = [
            new Record('alpha', 'first'),
            new Record('bravo'),
            new Record(str_repeat('a larger payload that compresses well. ', 100), 'third'),
        ];

        $messageSet = MessageSet::fromRecords($records, $codec);
        // The offsets of a produced set always count from 0; producing it twice shows that the broker replaces them,
        // inside a compressed wrapper as well
        $this->produce($topic, $messageSet);
        $baseOffset = $this->produce($topic, $messageSet);
        $fetched    = $this->fetch($topic, $baseOffset);

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
        $topic = $this->createTopic('t3-message-set-null');

        $baseOffset = $this->produce($topic, MessageSet::fromRecords([
            new Record(null, 'tombstone'),
            new Record('', 'empty'),
            new Record('value', null),
        ]));
        $fetched = $this->fetch($topic, $baseOffset);

        self::assertCount(3, $fetched);
        self::assertNull($fetched[0]->value, 'a null value is not the same thing as an empty one');
        self::assertSame('tombstone', $fetched[0]->key);
        self::assertSame('', $fetched[1]->value);
        self::assertSame('value', $fetched[2]->value);
        self::assertNull($fetched[2]->key);
    }

    public function testAFetchThatDoesNotFitTheFirstMessageComesBackWithoutRecords(): void
    {
        $topic      = $this->createTopic('t3-message-set-partial');
        $baseOffset = $this->produce($topic, MessageSet::fromRecords([new Record(str_repeat('x', 4096))]));

        // MaxBytes below the size of the first message: the broker answers with a message it cut short
        $partial = $this->fetch($topic, $baseOffset, 64);

        self::assertSame([], $partial, 'a partial trailing message is dropped instead of failing the fetch');
        self::assertCount(1, $this->fetch($topic, $baseOffset, 65536));
    }

    public function testTheBrokerAcceptsTheChecksumsThisClientComputes(): void
    {
        $topic = $this->createTopic('t3-message-set-crc');

        // A corrupt checksum would make the broker answer with error code 2 instead of an offset
        $offset = $this->produce($topic, MessageSet::fromRecords([new Record('bar', 'foo')]));

        self::assertGreaterThanOrEqual(0, $offset);
    }

    /**
     * Creates a topic through the auto-creation of the broker and waits until its metadata is published
     */
    private function createTopic(string $prefix): string
    {
        $topic = self::uniqueTopicName($prefix);

        new ClusterReadinessProbe(
            fn(): Stream => $this->connect(),
            self::LEADER_TIMEOUT,
            clientId: self::CLIENT_ID
        )->awaitBrokers($topic);

        return $topic;
    }

    /**
     * Produces the message set into partition 0 and returns the offset the broker assigned to its first message
     */
    private function produce(string $topic, MessageSet $messageSet): int
    {
        $deadline  = microtime(true) + self::LEADER_TIMEOUT;
        $lastError = 0;

        do {
            $stream = $this->connect();
            new MessageSetProduceRequest([$topic => [0 => $messageSet]], 1, 5000, self::CLIENT_ID, 1)->writeTo($stream);

            $partition = ProduceResponse::unpack($stream)->topics[$topic][0];
            if ($partition->errorCode === KafkaException::NO_ERROR) {
                return $partition->offset;
            }
            // A freshly auto-created topic has no leader for a moment
            $lastError = $partition->errorCode;
            usleep(250000);
        } while (microtime(true) < $deadline);

        throw KafkaException::fromCode($lastError, ['topic' => $topic, 'partitionId' => 0]);
    }

    /**
     * Fetches partition 0 of the topic from the given offset
     *
     * @return list<Record>
     */
    private function fetch(string $topic, int $offset, int $maxBytes = 65536): array
    {
        $stream = $this->connect();
        new FetchRequest([$topic => [0 => $offset]], 1000, 1, $maxBytes, -1, self::CLIENT_ID, 2)->writeTo($stream);

        /** @var FetchResponsePartition $partition */
        $partition = FetchResponse::unpack($stream)->topics[$topic][0];
        if ($partition->errorCode !== KafkaException::NO_ERROR) {
            throw KafkaException::fromCode($partition->errorCode, ['topic' => $topic, 'partitionId' => 0]);
        }

        return array_values($partition->messageSet);
    }
}
