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
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Common\Record\Snappy;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Data\FetchResponsePartition;
use Protocol\Kafka\Protocol\Request\FetchRequestV0;
use Protocol\Kafka\Protocol\Request\FetchRequestV1;
use Protocol\Kafka\Protocol\Request\FetchRequestV4;
use Protocol\Kafka\Protocol\Request\FetchResponseV4;
use Protocol\Kafka\Protocol\Request\ProduceRequestV3;
use Protocol\Kafka\Protocol\Request\ProduceResponseV3;
use Protocol\Kafka\Tests\Fixture\RemovedVersionProbe;
use Protocol\Kafka\Tests\Fixture\TopicMetadataProbe;

/**
 * Produces record sets to the Kafka 4.3.1 node of this line and fetches them back.
 *
 * The broker is the authority on the record format: it validates the checksum of every batch it appends, it
 * decompresses a compressed batch to validate its records, and it hands the batch back to a fetch as the log holds
 * it. A batch that survives this round trip is a batch that Kafka itself accepts.
 *
 * **This class wrote its test data as message sets of the formats v0 and v1 through a Produce v2 on the lines up to
 * 3.x.** Kafka 4.0 removed Produce v0 to v2 and Fetch v0 to v3 (KIP-896), which were the only versions that carry a
 * message set, so a 4.x node can neither be given one nor be asked to convert its log into one. The round trip is
 * therefore the one of the record batch v2, through the lowest versions the node serves - Produce v3 and Fetch v4,
 * which answers the batch as the log holds it - and the refusal of the versions that carried message sets is
 * measured here. The message sets themselves stay a format of this package: their classes, the vectors the lines
 * below captured on real brokers, and the unit tests of the codecs.
 *
 * @see docs/protocol/4.3.md, sections "MessageSet and Message" and "The versions Kafka 4.0 removed (KIP-896)"
 */
#[CoversClass(RecordBatch::class)]
#[CoversClass(MessageSet::class)]
#[CoversClass(CompressionCodec::class)]
#[CoversClass(Snappy::class)]
#[CoversClass(Lz4::class)]
#[CoversClass(FetchRequestV4::class)]
#[CoversClass(FetchResponseV4::class)]
#[CoversClass(FetchResponsePartition::class)]
final class MessageSetProduceFetchTest extends IntegrationTestCase
{
    /**
     * Client id that identifies the requests of this test in the logs of the broker
     */
    private const string CLIENT_ID = 'kafka-client-t2-40-record-sets';

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

        $this->topic = self::uniqueTopicName('t2-40-record-set');
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
    public function testARecordBatchSurvivesTheRoundTripThroughTheBroker(int $codec): void
    {
        $now     = self::currentTimestampMs();
        $records = [
            new Record('alpha', 'first')->withCreateTime($now),
            new Record('bravo')->withCreateTime($now),
            new Record(str_repeat('a larger payload that compresses well. ', 100), 'third')->withCreateTime($now),
        ];

        $batch = RecordBatch::fromRecords($records, $codec)->toBuffer();
        // The base offset of a produced batch always counts from 0; producing it twice shows that the broker
        // replaces it, and the offset deltas of the records inside a compressed batch stay relative to it
        $this->produce($batch);
        $baseOffset = $this->produce($batch);
        $partition  = $this->fetchPartition($baseOffset);
        $fetched    = $partition->getRecords()->getRecords();

        self::assertSame(3, $baseOffset, 'the second batch is appended after the three records of the first one');
        self::assertSame(RecordBatch::MAGIC, $partition->getRecords()->getMagic(), 'a Fetch v4 converts nothing');
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
            'the broker assigns consecutive offsets, the records of a compressed batch included'
        );
        self::assertSame([$now, $now, $now], array_map(
            static fn(Record $record): ?int => $record->timestamp,
            $fetched
        ));
    }

    public function testANullValueSurvivesTheRoundTripThroughTheBroker(): void
    {
        $now        = self::currentTimestampMs();
        $baseOffset = $this->produce(RecordBatch::fromRecords([
            new Record(null, 'tombstone')->withCreateTime($now),
            new Record('', 'empty')->withCreateTime($now),
            new Record('value', null)->withCreateTime($now),
        ])->toBuffer());
        $fetched = $this->fetch($baseOffset);

        self::assertCount(3, $fetched);
        self::assertNull($fetched[0]->value, 'a null value is not the same thing as an empty one');
        self::assertSame('tombstone', $fetched[0]->key);
        self::assertSame('', $fetched[1]->value);
        self::assertSame('value', $fetched[2]->value);
        self::assertNull($fetched[2]->key);
    }

    public function testAFetchSmallerThanTheFirstBatchStillGetsTheWholeBatch(): void
    {
        $baseOffset = $this->produce(RecordBatch::fromRecords([
            new Record(str_repeat('x', 4096))->withCreateTime(self::currentTimestampMs()),
        ])->toBuffer());

        // MaxBytes below the size of the first batch, on a Fetch v4: from version 3 on (KIP-74) the limit is soft
        // for the first batch of the answer, so the broker hands the whole batch out rather than a truncated slice
        // or nothing - which is what a Fetch v0 to v2 got, and why a consumer of those versions could get stuck
        // on a message bigger than its fetch size. Those versions are gone from a 4.x node (KIP-896)
        $partition = $this->fetchPartition($baseOffset, 64);
        $records   = $partition->getRecords()->getRecords();

        self::assertCount(1, $records, 'the batch comes back whole');
        self::assertSame(str_repeat('x', 4096), $records[0]->value);
        self::assertFalse($partition->getRecords()->hasPartialTrailingRecord());
        self::assertFalse($partition->isSingleMessageTooLarge($baseOffset));
    }

    public function testTheBrokerAcceptsTheChecksumsThisClientComputes(): void
    {
        // A corrupt CRC-32C would make the broker answer with the error code 2 instead of a base offset
        $offset = $this->produce(
            RecordBatch::fromRecords([new Record('bar', 'foo')->withCreateTime(self::currentTimestampMs())])
                ->toBuffer()
        );

        self::assertGreaterThanOrEqual(0, $offset);
    }

    public function testTheFetchVersionsThatAnsweredAMessageSetCloseTheConnection(): void
    {
        // Fetch v0 and v1 were the versions this class read message sets back with, and the node down-converted its
        // record batches for them; `FetchRequest.json` @ 4.0.0 starts at version 4, and the parser of a 4.x node
        // refuses everything below it by closing the connection (KIP-896)
        $baseOffset = $this->produce(
            RecordBatch::fromRecords([new Record('throttle', 'probe')->withCreateTime(self::currentTimestampMs())])
                ->toBuffer()
        );
        $probe = new RemovedVersionProbe(self::firstBootstrapServer());

        self::assertSame(RemovedVersionProbe::CLOSED, $probe->send(new FetchRequestV1(
            [$this->topic => [self::PARTITION => $baseOffset]],
            1000,
            1,
            65536,
            -1,
            self::CLIENT_ID,
            51
        )));
        self::assertSame(RemovedVersionProbe::CLOSED, $probe->send(new FetchRequestV0(
            [$this->topic => [self::PARTITION => $baseOffset]],
            1000,
            1,
            65536,
            -1,
            self::CLIENT_ID,
            52
        )));

        // ... while the lowest version the node serves reads the very same record
        self::assertSame('throttle', $this->fetch($baseOffset)[0]->value);
    }

    public function testAMessageSetOfTheFormatsV0AndV1IsRefusedByEveryServedProduceVersion(): void
    {
        foreach ([Message::MAGIC_V0, Message::MAGIC_V1] as $magic) {
            $stream = $this->connect();
            new ProduceRequestV3(
                [$this->topic => [self::PARTITION => MessageSet::fromRecords([new Record('legacy')], 0, $magic)]],
                1,
                self::PRODUCE_TIMEOUT_MS,
                self::CLIENT_ID,
                53
            )->writeTo($stream);

            $partition = ProduceResponseV3::unpack($stream)->topics[$this->topic]->partitions[self::PARTITION];
            self::assertSame(KafkaException::INVALID_RECORD, $partition->errorCode, "magic {$magic}: 87");
        }
    }

    /**
     * Produces the record batch into the partition under test and returns the offset of its first record
     *
     * The request is a Produce **v3**, the lowest version a node of Kafka 4.0 or later serves (KIP-896).
     */
    private function produce(string $recordBatch): int
    {
        $stream = $this->connect();
        new ProduceRequestV3(
            [$this->topic => [self::PARTITION => $recordBatch]],
            1,
            self::PRODUCE_TIMEOUT_MS,
            self::CLIENT_ID,
            1
        )->writeTo($stream);

        $partition = ProduceResponseV3::unpack($stream)->topics[$this->topic]->partitions[self::PARTITION];
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
        return $this->fetchPartition($offset, $maxBytes)->getRecords()->getRecords();
    }

    /**
     * Fetches the partition under test from the given offset with a Fetch **v4**, the lowest version a 4.x node serves
     */
    private function fetchPartition(int $offset, int $maxBytes = 65536): FetchResponsePartition
    {
        $stream = $this->connect();
        new FetchRequestV4([$this->topic => [self::PARTITION => $offset]], 1000, 1, $maxBytes, -1, self::CLIENT_ID, 2)
            ->writeTo($stream);

        $partition = FetchResponseV4::unpack($stream)->topics[$this->topic]->partitions[self::PARTITION];
        if ($partition->errorCode !== 0) {
            throw KafkaException::fromCode($partition->errorCode, ['topic' => $this->topic, 'partitionId' => self::PARTITION]);
        }

        return $partition;
    }

    /**
     * The current time in milliseconds: a record stamped with a timestamp of the past falls to the retention
     */
    private static function currentTimestampMs(): int
    {
        return (int) round(microtime(true) * 1000);
    }
}
