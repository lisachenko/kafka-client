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
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Producer\DefaultPartitioner;
use Protocol\Kafka\Producer\Internals\CompressingClient;
use Protocol\Kafka\Producer\KafkaProducer;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Producer\RecordMetadata;
use Protocol\Kafka\Protocol\Data\FetchResponsePartition;
use Protocol\Kafka\Protocol\Request\FetchRequest;
use Protocol\Kafka\Protocol\Request\FetchResponse;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * Verifies the producer against a real Kafka 0.8.2.2 broker.
 *
 * Every test writes with the producer and reads the partitions back with a raw Fetch request, so that what the
 * broker really stored is checked, not what the client believes it sent: the partition a key was placed in, the
 * offsets that the promises were resolved with, and the compression of a batch.
 *
 * @see docs/protocol/0.8.2.md, section "Produce API (key 0, v0)"
 */
#[CoversClass(KafkaProducer::class)]
#[CoversClass(DefaultPartitioner::class)]
#[CoversClass(ProducerConfig::class)]
#[CoversClass(CompressingClient::class)]
final class KafkaProducerTest extends IntegrationTestCase
{
    /**
     * Client id that identifies the requests of this test in the logs of the broker
     */
    private const string CLIENT_ID = 'kafka-client-t8-producer';

    /**
     * How long the broker may take to acknowledge a produce request, in milliseconds
     */
    private const int PRODUCE_TIMEOUT_MS = 10000;

    /**
     * Number of bytes that a fetch of a whole partition of these tests may return
     */
    private const int FETCH_MAX_BYTES = 4194304;

    /**
     * Topic of the current test, created and given a leader by {@see KafkaProducerTest::setUp()}
     */
    private string $topic;

    /**
     * Number of partitions that the broker created the topic with
     */
    private int $partitionCount;

    protected function setUp(): void
    {
        parent::setUp();

        $this->topic = self::uniqueTopicName('t8-producer');

        $metadata             = new TopicMetadataProbe(fn(): Stream => $this->connect(), 30.0, self::CLIENT_ID)
            ->awaitTopicWithLeaders($this->topic);
        $this->partitionCount = count($metadata->partitions);
    }

    /**
     * Number of acknowledgements that the durable produce modes ask the broker for
     *
     * @return \Generator<string, array{0: int}>
     */
    public static function acknowledgedModes(): \Generator
    {
        yield 'acks=1'  => [ProducerConfig::ACKS_LEADER];
        yield 'acks=-1' => [ProducerConfig::ACKS_ALL];
    }

    #[DataProvider('acknowledgedModes')]
    public function testAThousandKeyedRecordsGoToThePartitionOfTheirKey(int $requiredAcks): void
    {
        $producer = $this->producer([
            ProducerConfig::ACKS       => $requiredAcks,
            // One request per flush instead of one per record
            ProducerConfig::BATCH_SIZE => 1024 * 1024,
        ]);

        $acknowledged     = [];
        $failures         = [];
        $expectedContents = [];
        for ($record = 0; $record < 1000; $record++) {
            $key   = "key-{$record}";
            $value = "value-{$record}";

            $expectedContents[$this->partitionOf($key)][] = [$key, $value];

            $producer->send($this->topic, Record::fromKeyValue($key, $value))->then(
                static function (RecordMetadata $metadata) use (&$acknowledged): void {
                    $acknowledged[] = $metadata;
                },
                static function (\Throwable $error) use (&$failures): void {
                    $failures[] = $error;
                }
            );
        }
        $producer->flush();

        self::assertSame([], array_map(strval(...), $failures), 'The broker acknowledged every batch');
        self::assertCount(1000, $acknowledged, 'Every record is answered, with the metadata of the batch it went in');

        $acknowledgedPartitions = array_unique(array_map(
            static fn(RecordMetadata $metadata): int => $metadata->partition,
            $acknowledged
        ));
        sort($acknowledgedPartitions);
        $expectedPartitions = array_keys($expectedContents);
        sort($expectedPartitions);
        self::assertSame($expectedPartitions, $acknowledgedPartitions, 'Every partition got its share');

        foreach ($expectedContents as $partition => $expectedRecords) {
            $storedRecords = $this->fetchRecords($partition);

            self::assertSame(
                $expectedRecords,
                array_map(static fn(Record $record): array => [$record->key, $record->value], $storedRecords),
                "The records of the partition {$partition} are the ones the murmur2 hash of their key selects"
            );
        }
    }

    public function testTheOffsetOfAPromiseIsTheOffsetTheRecordsWereStoredAt(): void
    {
        $producer = $this->producer([
            ProducerConfig::ACKS       => ProducerConfig::ACKS_ALL,
            ProducerConfig::BATCH_SIZE => 1024 * 1024,
        ]);

        // Two batches for the same partition, so that the second one starts at the offset the first one ended at
        $firstBatch = $secondBatch = null;
        for ($record = 0; $record < 5; $record++) {
            $producer->send($this->topic, Record::fromValue("first-{$record}"), 0)->then(
                static function (RecordMetadata $metadata) use (&$firstBatch): void {
                    $firstBatch = $metadata;
                }
            );
        }
        $producer->flush();

        for ($record = 0; $record < 3; $record++) {
            $producer->send($this->topic, Record::fromValue("second-{$record}"), 0)->then(
                static function (RecordMetadata $metadata) use (&$secondBatch): void {
                    $secondBatch = $metadata;
                }
            );
        }
        $producer->flush();

        self::assertInstanceOf(RecordMetadata::class, $firstBatch);
        self::assertInstanceOf(RecordMetadata::class, $secondBatch);
        self::assertSame(0, $firstBatch->offset, 'The topic was empty, so the first batch starts at 0');
        self::assertSame(5, $secondBatch->offset, 'The base offset of a batch is the offset of its first record');
        self::assertSame($this->topic . '-0@5', (string) $secondBatch);

        $storedRecords = $this->fetchRecords(0);
        self::assertCount(8, $storedRecords);
        self::assertSame('second-0', $storedRecords[5]->value);
        self::assertSame(5, $storedRecords[5]->offset);
    }

    public function testAFireAndForgetSendIsNotAcknowledgedButStillStored(): void
    {
        $producer = $this->producer([
            ProducerConfig::ACKS       => ProducerConfig::ACKS_NONE,
            ProducerConfig::BATCH_SIZE => 1024 * 1024,
        ]);

        $offsets = [];
        for ($record = 0; $record < 10; $record++) {
            $producer->send($this->topic, Record::fromValue("fire-and-forget-{$record}"), 1)->then(
                static function (RecordMetadata $metadata) use (&$offsets): void {
                    $offsets[] = $metadata->offset;
                }
            );
        }
        $producer->flush();

        self::assertSame(array_fill(0, 10, -1), $offsets, 'A request the broker never answers has no offset');

        // The records are on their way, the broker has not promised anything about when they will be readable
        $storedRecords = $this->awaitRecords(1, 10);

        self::assertCount(10, $storedRecords);
        self::assertSame('fire-and-forget-0', $storedRecords[0]->value);
        self::assertSame('fire-and-forget-9', $storedRecords[9]->value);
    }

    /**
     * Compression type of the producer and the codec that the broker has to store
     *
     * @return \Generator<string, array{0: string, 1: int}>
     */
    public static function compressionTypes(): \Generator
    {
        yield 'gzip'   => [ProducerConfig::COMPRESSION_TYPE_GZIP, CompressionCodec::GZIP];
        yield 'snappy' => [ProducerConfig::COMPRESSION_TYPE_SNAPPY, CompressionCodec::SNAPPY];
    }

    #[DataProvider('compressionTypes')]
    public function testACompressedBatchIsStoredAsOneCompressedMessage(
        string $compressionType,
        int $expectedCodec
    ): void {
        $producer = $this->producer([
            ProducerConfig::ACKS             => ProducerConfig::ACKS_LEADER,
            ProducerConfig::BATCH_SIZE       => 1024 * 1024,
            ProducerConfig::COMPRESSION_TYPE => $compressionType,
        ]);

        $expectedRecords = [];
        $metadata        = null;
        for ($record = 0; $record < 25; $record++) {
            $value             = "a repetitive value that compresses well #{$record}";
            $expectedRecords[] = $value;

            $producer->send($this->topic, Record::fromKeyValue("key-{$record}", $value), 2)->then(
                static function (RecordMetadata $recordMetadata) use (&$metadata): void {
                    $metadata = $recordMetadata;
                }
            );
        }
        $producer->flush();

        self::assertInstanceOf(RecordMetadata::class, $metadata);
        self::assertSame(0, $metadata->offset, 'The broker answers with the offset of the first inner record');

        // The log holds a single wrapper message, whose value is the whole batch compressed with the codec
        $partition    = $this->fetchPartition(2);
        $storedBuffer = (string) $partition->messageSet;
        $wrapper      = self::firstMessageOf($storedBuffer);

        self::assertTrue($wrapper->isCompressed(), 'The broker stored the batch as it was produced');
        self::assertSame($expectedCodec, $wrapper->getCompressionCodec());
        self::assertLessThan(
            MessageSet::fromRecords(array_map(Record::fromValue(...), $expectedRecords))->sizeInBytes(),
            strlen($storedBuffer),
            'The stored batch is smaller than the records it holds'
        );

        // ... and it comes back as the records it was built from, with the offsets the broker assigned to them
        $storedRecords = $partition->getMessageSet()->getRecords();

        self::assertCount(25, $storedRecords);
        self::assertSame($expectedRecords, array_column($storedRecords, 'value'));
        self::assertSame('key-24', $storedRecords[24]->key);
        self::assertSame(range(0, 24), array_column($storedRecords, 'offset'));
    }

    public function testTheProducerReportsThePartitionsOfATopic(): void
    {
        $partitions = $this->producer()->partitionsFor($this->topic);

        self::assertCount($this->partitionCount, $partitions);
        foreach ($partitions as $partitionId => $partitionMetadata) {
            self::assertSame($partitionId, $partitionMetadata->partitionId);
            self::assertNotSame(-1, $partitionMetadata->leader, 'Every partition of the topic has a leader');
        }
    }

    public function testARecordOfAnUnknownTopicIsRejected(): void
    {
        // The partitioner has to know the partitions of the topic to place the record, and the metadata of the
        // cluster hold nothing about a topic whose name a broker would refuse to create
        $producer = $this->producer();

        $this->expectException(KafkaException::class);

        $producer->send('an invalid topic name', Record::fromValue('a value'));
    }

    /**
     * Builds a producer that talks to the broker of the test environment
     *
     * @param array<string, mixed> $configuration Producer options on top of the defaults of this test
     */
    private function producer(array $configuration = []): KafkaProducer
    {
        return new KafkaProducer($configuration + [
            ProducerConfig::BOOTSTRAP_SERVERS => ['tcp://' . self::firstBootstrapServer()],
            ProducerConfig::CLIENT_ID         => self::CLIENT_ID,
            ProducerConfig::TIMEOUT_MS        => self::PRODUCE_TIMEOUT_MS,
            ProducerConfig::REQUEST_TIMEOUT_MS => self::PRODUCE_TIMEOUT_MS,
        ]);
    }

    /**
     * Returns the partition that the key of a record is placed in, as the Java client computes it
     */
    private function partitionOf(string $key): int
    {
        return DefaultPartitioner::toPositive(DefaultPartitioner::murmur2($key)) % $this->partitionCount;
    }

    /**
     * Fetches a partition of the topic under test from its beginning
     *
     * @return list<Record>
     */
    private function fetchRecords(int $partition): array
    {
        return $this->fetchPartition($partition)->getMessageSet()->getRecords();
    }

    /**
     * Fetches a partition of the topic under test until it holds the expected number of records
     *
     * @return list<Record>
     */
    private function awaitRecords(int $partition, int $expectedCount, float $timeout = 10.0): array
    {
        $deadline = microtime(true) + $timeout;
        do {
            $records = $this->fetchRecords($partition);
            if (count($records) >= $expectedCount) {
                return $records;
            }
            usleep(100000);
        } while (microtime(true) < $deadline);

        return $records;
    }

    /**
     * Fetches a partition of the topic under test from its beginning, with the raw bytes the broker stored
     */
    private function fetchPartition(int $partition): FetchResponsePartition
    {
        $stream = $this->connect();
        new FetchRequest(
            [$this->topic => [$partition => 0]],
            1000,
            1,
            self::FETCH_MAX_BYTES,
            -1,
            self::CLIENT_ID,
            1
        )->writeTo($stream);

        $partitionResponse = FetchResponse::unpack($stream)->topics[$this->topic]->partitions[$partition];
        if ($partitionResponse->errorCode !== 0) {
            throw KafkaException::fromCode(
                $partitionResponse->errorCode,
                ['topic' => $this->topic, 'partitionId' => $partition]
            );
        }

        return $partitionResponse;
    }

    /**
     * Reads the first message of a serialized message set, without unwrapping a compressed one
     */
    private static function firstMessageOf(string $buffer): Message
    {
        /** @var array{messageSize: int} $header */
        $header = unpack('Joffset/NmessageSize', $buffer);

        return Message::fromBuffer(substr($buffer, MessageSet::ENTRY_OVERHEAD, $header['messageSize']));
    }
}
