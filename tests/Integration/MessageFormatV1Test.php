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
use Protocol\Kafka\Common\Record\MessageV0;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\TimestampType;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Data\FetchResponsePartition;
use Protocol\Kafka\Protocol\Request\FetchRequestV1;
use Protocol\Kafka\Protocol\Request\FetchRequestV2;
use Protocol\Kafka\Protocol\Request\FetchResponse;
use Protocol\Kafka\Protocol\Request\ProduceRequest;
use Protocol\Kafka\Protocol\Request\ProduceResponse;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * The message format v1 of Kafka 0.10 against the real broker: timestamps, relative offsets and the conversions.
 *
 * The broker is the authority on the format. It validates the checksum of every message it appends, it decompresses
 * a compressed set to assign the offsets of its inner messages, it converts a batch into the
 * `message.format.version` of the topic, and it converts the log back down to message format v0 for every client
 * that fetches with a request below version 2. Everything this suite asserts was read out of a log that a 0.10.2.2
 * broker wrote.
 *
 * @see docs/protocol/0.11.0.md, section "MessageSet and Message"
 */
#[CoversClass(MessageSet::class)]
#[CoversClass(Message::class)]
#[CoversClass(MessageV0::class)]
#[CoversClass(TimestampType::class)]
#[CoversClass(CompressionCodec::class)]
#[CoversClass(Lz4::class)]
final class MessageFormatV1Test extends IntegrationTestCase
{
    /**
     * Client id that identifies the requests of this test in the logs of the broker
     */
    private const string CLIENT_ID = 'kafka-client-t2';

    /**
     * Partition that every test of this class produces to and fetches from
     */
    private const int PARTITION = 0;

    /**
     * How long the broker may take to acknowledge a produce request, in milliseconds
     */
    private const int PRODUCE_TIMEOUT_MS = 5000;

    /**
     * A fixed CreateTime for the produced records, 2017-03-12T12:00:00Z
     */
    private const int CREATE_TIME = 1489324800000;

    /**
     * Topic of the current test, created and given a leader by {@see MessageFormatV1Test::setUp()}
     */
    private string $topic;

    protected function setUp(): void
    {
        parent::setUp();

        $this->topic = self::uniqueTopicName('t2-message-format');
        $this->awaitTopic($this->topic);
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
    public function testTheTimestampsOfABatchSurviveTheRoundTripThroughTheBroker(int $codec): void
    {
        $records = [
            new Record('alpha', 'first', 0, null, self::CREATE_TIME),
            new Record('bravo', null, 0, null, self::CREATE_TIME + 10),
            new Record('charlie', 'third', 0, null, self::CREATE_TIME + 5),
        ];

        $baseOffset = $this->produce(MessageSet::fromRecords($records, $codec));
        $fetched    = $this->fetch($baseOffset, 2);

        self::assertCount(3, $fetched);
        self::assertSame(
            [self::CREATE_TIME, self::CREATE_TIME + 10, self::CREATE_TIME + 5],
            array_map(static fn(Record $record): ?int => $record->timestamp, $fetched),
            'the broker stores the CreateTime of every record as it was produced'
        );
        self::assertSame(
            [TimestampType::CREATE_TIME, TimestampType::CREATE_TIME, TimestampType::CREATE_TIME],
            array_map(static fn(Record $record): int => $record->timestampType, $fetched)
        );
        self::assertSame(
            [$baseOffset, $baseOffset + 1, $baseOffset + 2],
            array_map(static fn(Record $record): ?int => $record->offset, $fetched),
            'the broker assigns consecutive offsets, inner messages of a compressed set included'
        );
        self::assertSame(['alpha', 'bravo', 'charlie'], array_map(
            static fn(Record $record): ?string => $record->value,
            $fetched
        ));
    }

    #[DataProvider('compressionCodecs')]
    public function testTheBrokerStoresTheMessageFormatTheProducerWrote(int $codec): void
    {
        $baseOffset = $this->produce(MessageSet::fromRecords(
            [new Record('alpha', null, 0, null, self::CREATE_TIME)],
            $codec
        ));

        $stored = MessageSet::shallowFromBuffer($this->fetchPartition($baseOffset, 2)->messageSet ?? '');

        self::assertSame(Message::MAGIC_V1, $stored->getMagic(), 'the log holds message format v1');
        [, $message] = $stored->getMessages()[0];
        self::assertSame($codec, $message->getCompressionCodec(), 'and the codec the producer chose');
        self::assertSame(TimestampType::CREATE_TIME, $message->getTimestampType());
    }

    public function testTheInnerOffsetsOfACompressedBatchAreRelativeToTheWrapper(): void
    {
        $records = [new Record('alpha'), new Record('bravo'), new Record('charlie')];

        // The first batch takes the offsets 0 to 2, so the wrapper of the second one sits at the offset 5
        $this->produce(MessageSet::fromRecords($records, CompressionCodec::GZIP));
        $baseOffset = $this->produce(MessageSet::fromRecords($records, CompressionCodec::GZIP));

        $stored             = MessageSet::shallowFromBuffer($this->fetchPartition($baseOffset, 2)->messageSet ?? '');
        [$offset, $wrapper] = $stored->getMessages()[0];

        self::assertSame(3, $baseOffset);
        self::assertSame(5, $offset, 'the wrapper carries the absolute offset of the last inner message');
        self::assertSame(
            [0, 1, 2],
            array_map(
                static fn(Record $record): ?int => $record->offset,
                MessageSet::fromBuffer($wrapper->decompressValue())->getRecords()
            ),
            'the inner offsets stay relative, the broker does not rewrite them in message format v1'
        );
        // ... and the reader turns them back into the absolute offsets of the log
        self::assertSame([3, 4, 5], array_map(
            static fn(Record $record): ?int => $record->offset,
            $this->fetch($baseOffset, 2)
        ));
    }

    public function testATopicWithLogAppendTimeReplacesTheTimestampsOfEveryRecord(): void
    {
        $topic = self::uniqueTopicName('t2-log-append-time');
        self::createTopic($topic, ['message.timestamp.type=LogAppendTime']);
        $this->awaitTopic($topic);

        $before     = (int) round(microtime(true) * 1000);
        $baseOffset = $this->produce(
            MessageSet::fromRecords([
                new Record('alpha', null, 0, null, self::CREATE_TIME),
                new Record('bravo', null, 0, null, self::CREATE_TIME + 10),
            ]),
            $topic
        );
        $after = (int) round(microtime(true) * 1000);

        $records = $this->fetch($baseOffset, 2, $topic);

        self::assertCount(2, $records);
        foreach ($records as $record) {
            self::assertSame(TimestampType::LOG_APPEND_TIME, $record->timestampType);
            self::assertNotNull($record->timestamp);
            self::assertGreaterThanOrEqual($before, $record->timestamp, 'the clock of the broker, not of the record');
            self::assertLessThanOrEqual($after, $record->timestamp);
        }
        self::assertSame(
            $records[0]->timestamp,
            $records[1]->timestamp,
            'every record of one append gets the same timestamp'
        );
    }

    public function testALogAppendTimeWrapperCarriesTheTimestampOfTheWholeCompressedBatch(): void
    {
        $topic = self::uniqueTopicName('t2-log-append-gzip');
        self::createTopic($topic, ['message.timestamp.type=LogAppendTime']);
        $this->awaitTopic($topic);

        $baseOffset = $this->produce(
            MessageSet::fromRecords([
                new Record('alpha', null, 0, null, self::CREATE_TIME),
                new Record('bravo', null, 0, null, self::CREATE_TIME + 10),
            ], CompressionCodec::GZIP),
            $topic
        );

        $stored  = MessageSet::shallowFromBuffer($this->fetchPartition($baseOffset, 2, $topic)->messageSet ?? '');
        $wrapper = $stored->getMessages()[0][1];

        self::assertSame(TimestampType::LOG_APPEND_TIME, $wrapper->getTimestampType());
        self::assertSame(
            CompressionCodec::GZIP | TimestampType::MASK,
            $wrapper->attributes,
            'the codec and the timestamp type live in the attributes of the wrapper'
        );

        // The broker sets the bit on the wrapper only: the compressed inner messages still hold what was produced
        $inner = MessageSet::shallowFromBuffer($wrapper->decompressValue());
        self::assertSame(
            [TimestampType::CREATE_TIME, TimestampType::CREATE_TIME],
            array_map(
                static fn(array $entry): int => $entry[1]->getTimestampType(),
                $inner->getMessages()
            )
        );
        self::assertSame(
            [self::CREATE_TIME, self::CREATE_TIME + 10],
            array_map(static fn(array $entry): ?int => $entry[1]->getTimestamp(), $inner->getMessages()),
            'the inner timestamps are left alone and have to be ignored by the reader'
        );
        // ... and the reader reports the timestamp of the wrapper for every record of the batch
        $records = $this->fetch($baseOffset, 2, $topic);
        self::assertSame(
            [$wrapper->getTimestamp(), $wrapper->getTimestamp()],
            array_map(static fn(Record $record): ?int => $record->timestamp, $records)
        );
    }

    #[DataProvider('compressionCodecs')]
    public function testTheBrokerDownConvertsToMessageFormatV0ForAFetchBelowVersionTwo(int $codec): void
    {
        $records = [
            new Record('alpha', 'first', 0, null, self::CREATE_TIME),
            new Record('bravo', null, 0, null, self::CREATE_TIME + 10),
        ];

        $baseOffset = $this->produce(MessageSet::fromRecords($records, $codec));

        $downConverted = MessageSet::shallowFromBuffer($this->fetchPartition($baseOffset, 1)->messageSet ?? '');
        self::assertSame(Message::MAGIC_V0, $downConverted->getMagic(), 'Fetch v1 never sees message format v1');

        $fetchedWithV1 = $this->fetch($baseOffset, 1);
        self::assertSame(['alpha', 'bravo'], array_map(
            static fn(Record $record): ?string => $record->value,
            $fetchedWithV1
        ));
        self::assertSame([null, null], array_map(
            static fn(Record $record): ?int => $record->timestamp,
            $fetchedWithV1
        ), 'the down-conversion drops the timestamps of the log');
        self::assertSame(
            [TimestampType::NO_TIMESTAMP_TYPE, TimestampType::NO_TIMESTAMP_TYPE],
            array_map(static fn(Record $record): int => $record->timestampType, $fetchedWithV1)
        );
        self::assertSame([$baseOffset, $baseOffset + 1], array_map(
            static fn(Record $record): ?int => $record->offset,
            $fetchedWithV1
        ));
    }

    public function testADownConvertedCompressedBatchCarriesAbsoluteInnerOffsets(): void
    {
        $records = [new Record('alpha'), new Record('bravo'), new Record('charlie')];

        $this->produce(MessageSet::fromRecords($records, CompressionCodec::GZIP));
        $baseOffset = $this->produce(MessageSet::fromRecords($records, CompressionCodec::GZIP));

        $stored  = MessageSet::shallowFromBuffer($this->fetchPartition($baseOffset, 1)->messageSet ?? '');
        $wrapper = $stored->getMessages()[0][1];

        self::assertSame(Message::MAGIC_V0, $wrapper->magicByte);
        self::assertSame(5, $stored->getMessages()[0][0]);
        self::assertSame(
            [3, 4, 5],
            array_map(
                static fn(Record $record): ?int => $record->offset,
                MessageSet::fromBuffer($wrapper->decompressValue())->getRecords()
            ),
            'a message format v0 set stores the absolute offsets inside the wrapper'
        );
    }

    public function testTheBrokerUpConvertsAMessageFormatV0BatchOfTheProducer(): void
    {
        $baseOffset = $this->produce(MessageSet::fromRecords(
            [new Record('alpha', 'first'), new Record('bravo')],
            CompressionCodec::NONE,
            Message::MAGIC_V0
        ));

        $stored = MessageSet::shallowFromBuffer($this->fetchPartition($baseOffset, 2)->messageSet ?? '');

        self::assertSame(Message::MAGIC_V1, $stored->getMagic(), 'the topic decides the format of the log');
        self::assertSame(
            [null, null],
            array_map(static fn(array $entry): ?int => $entry[1]->getTimestamp(), $stored->getMessages()),
            'an up-converted message carries the timestamp -1, i.e. none at all'
        );
        self::assertSame(
            [Message::NO_TIMESTAMP, Message::NO_TIMESTAMP],
            array_map(static fn(array $entry): int => $entry[1]->timestamp, $stored->getMessages())
        );
    }

    public function testATopicOfTheOlderMessageFormatMakesTheBrokerDownConvertOnAppend(): void
    {
        $topic = self::uniqueTopicName('t2-message-format-090');
        self::createTopic($topic, ['message.format.version=0.9.0']);
        $this->awaitTopic($topic);

        $baseOffset = $this->produce(
            MessageSet::fromRecords([new Record('alpha', null, 0, null, self::CREATE_TIME)], CompressionCodec::NONE),
            $topic
        );

        $stored = MessageSet::shallowFromBuffer($this->fetchPartition($baseOffset, 2, $topic)->messageSet ?? '');

        self::assertSame(
            Message::MAGIC_V0,
            $stored->getMagic(),
            'message.format.version=0.9.0 stores message format v0, whatever the producer sent'
        );
        self::assertNull($this->fetch($baseOffset, 2, $topic)[0]->timestamp);
    }

    public function testTheBrokerReadsTheLz4FramesOfThisClientAndWritesOnesItCanRead(): void
    {
        $records    = [new Record(str_repeat('a repetitive payload that compresses well. ', 50), 'lz4')];
        $baseOffset = $this->produce(MessageSet::fromRecords($records, CompressionCodec::LZ4));

        // The broker validated the frame: it decompressed the batch to assign the offsets of its inner messages
        $withV2 = MessageSet::shallowFromBuffer($this->fetchPartition($baseOffset, 2)->messageSet ?? '');
        self::assertSame(CompressionCodec::LZ4, $withV2->getMessages()[0][1]->getCompressionCodec());
        self::assertStringStartsWith(
            Lz4::frameDescriptor(false),
            (string) $withV2->getMessages()[0][1]->value,
            'a message format v1 frame carries the correct descriptor checksum'
        );

        // ... and the frame that the broker itself writes for a message format v0 answer carries the broken one
        $withV1 = MessageSet::shallowFromBuffer($this->fetchPartition($baseOffset, 1)->messageSet ?? '');
        self::assertSame(Message::MAGIC_V0, $withV1->getMagic());
        self::assertStringStartsWith(
            Lz4::frameDescriptor(true),
            (string) $withV1->getMessages()[0][1]->value,
            'KAFKA-3160: the frame of a message format v0 message has the broken descriptor checksum'
        );
        self::assertSame($records[0]->value, $this->fetch($baseOffset, 1)[0]->value);
        self::assertSame($records[0]->value, $this->fetch($baseOffset, 2)[0]->value);
    }

    /**
     * Creates a topic with the given configuration entries through the tools of the broker container
     *
     * @param list<string> $configuration `key=value` entries of the topic configuration
     */
    private static function createTopic(string $topic, array $configuration = []): void
    {
        $container = getenv('KAFKA_CONTAINER');
        $command   = [
            'docker', 'exec', $container === false || $container === '' ? 'kafka-0-10-2-2' : $container,
            '/opt/kafka/bin/kafka-topics.sh', '--zookeeper', 'localhost:2181',
            '--create', '--topic', $topic, '--partitions', '1', '--replication-factor', '1',
        ];
        foreach ($configuration as $entry) {
            $command[] = '--config';
            $command[] = $entry;
        }

        $output   = [];
        $exitCode = 0;
        exec(implode(' ', array_map(escapeshellarg(...), $command)) . ' 2>&1', $output, $exitCode);

        if ($exitCode !== 0) {
            self::markTestSkipped("The topic {$topic} can not be created: " . implode("\n", $output));
        }
    }

    /**
     * Waits until the topic exists and its partitions have a leader
     */
    private function awaitTopic(string $topic): void
    {
        new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, self::CLIENT_ID)
            ->awaitTopicWithLeaders($topic);
    }

    /**
     * Produces the message set into the partition under test and returns the offset of its first message
     */
    private function produce(MessageSet $messageSet, ?string $topic = null): int
    {
        $topic ??= $this->topic;
        $stream = $this->connect();
        new ProduceRequest(
            [$topic => [self::PARTITION => $messageSet]],
            1,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            1
        )->writeTo($stream);

        $partition = ProduceResponse::unpack($stream)->topics[$topic]->partitions[self::PARTITION];
        if ($partition->errorCode !== 0) {
            throw KafkaException::fromCode(
                $partition->errorCode,
                ['topic' => $topic, 'partitionId' => self::PARTITION]
            );
        }

        return $partition->baseOffset;
    }

    /**
     * Fetches the partition under test from the given offset and returns the records it holds
     *
     * @return list<Record>
     */
    private function fetch(int $offset, int $version, ?string $topic = null): array
    {
        return MessageSet::fromBuffer($this->fetchPartition($offset, $version, $topic)->messageSet ?? '')
            ->getRecords();
    }

    /**
     * Fetches the partition under test with a Fetch request of the given version
     *
     * Version 2 is the one that says "I understand message format v1"; with version 1 the broker converts the log
     * down to message format v0 before it answers, see {@see FetchRequestV2}.
     */
    private function fetchPartition(int $offset, int $version, ?string $topic = null): FetchResponsePartition
    {
        $topic ??= $this->topic;
        $stream        = $this->connect();
        $requestClass  = $version === 2 ? FetchRequestV2::class : FetchRequestV1::class;

        new $requestClass([$topic => [self::PARTITION => $offset]], 1000, 1, 1048576, -1, self::CLIENT_ID, 2)
            ->writeTo($stream);

        $partition = FetchResponse::unpack($stream)->topics[$topic]->partitions[self::PARTITION];
        if ($partition->errorCode !== 0) {
            throw KafkaException::fromCode(
                $partition->errorCode,
                ['topic' => $topic, 'partitionId' => self::PARTITION]
            );
        }

        return $partition;
    }
}
