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

namespace Protocol\Kafka\Tests\Unit\Common\Record;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\Errors\CorruptMessageException;
use Protocol\Kafka\Common\Record\CompressionCodec;
use Protocol\Kafka\Common\Record\ControlRecordType;
use Protocol\Kafka\Common\Record\EndTransactionMarker;
use Protocol\Kafka\Common\Record\Header;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Common\Record\TimestampType;
use Protocol\Kafka\Common\Utils\ByteUtils;

/**
 * Byte-exact specification of the record batch of the message format v2.
 *
 * The reference batches are the ones the 0.11.0.3 broker of this line wrote and this suite decodes them back into
 * the very same bytes; the same batches are replayed as wire vectors by `tests/Compliance`, together with the
 * batches that only a raw Produce v3 frame can write.
 *
 * @see docs/protocol/0.11.0.md, section "RecordBatch (message format v2)"
 */
#[CoversClass(RecordBatch::class)]
final class RecordBatchTest extends TestCase
{
    /**
     * A fixed CreateTime for the records below, 2020-09-13T12:26:40Z
     */
    private const int CREATE_TIME = 1600000000000;

    /**
     * The uncompressed batch of the vector `messageformat.v2.none.createtime`, written by a 0.11.0.3 broker
     */
    private const string BROKER_BATCH = '00000000000000000000005a00000000026b07fa76000000000002000001a085bee29a000001a085'
        . 'bee2a2ffffffffffffffffffffffffffff0000000316000000010a616c706861001c001002066b65790a627261766f001a001004'
        . '010e636861726c696500';

    public function testABatchOfTheBrokerIsDecodedIntoItsHeaderFields(): void
    {
        $batch = RecordBatch::fromBuffer((string) hex2bin(self::BROKER_BATCH));

        self::assertSame(0, $batch->getBaseOffset());
        self::assertSame(90, $batch->batchLength);
        self::assertSame(102, $batch->sizeInBytes(), 'the length does not count the offset and the length itself');
        self::assertSame(0, $batch->getPartitionLeaderEpoch(), 'the broker assigned the leader epoch 0');
        self::assertSame(RecordBatch::MAGIC, $batch->magic);
        self::assertSame(1795684982, $batch->crc);
        self::assertSame(0, $batch->attributes);
        self::assertSame(2, $batch->lastOffsetDelta);
        self::assertSame(2, $batch->getLastOffset());
        self::assertSame(1788950274714, $batch->getFirstTimestamp());
        self::assertSame(1788950274722, $batch->getMaxTimestamp());
        self::assertSame(RecordBatch::NO_PRODUCER_ID, $batch->getProducerId());
        self::assertSame(RecordBatch::NO_PRODUCER_EPOCH, $batch->getProducerEpoch());
        self::assertSame(RecordBatch::NO_SEQUENCE, $batch->getBaseSequence());
        self::assertSame(RecordBatch::NO_SEQUENCE, $batch->getLastSequence());
        self::assertSame(3, $batch->count());
        self::assertFalse($batch->isEmpty());
        self::assertFalse($batch->isControlBatch());
        self::assertFalse($batch->isTransactional());
        self::assertSame(CompressionCodec::NONE, $batch->getCompressionCodec());
        self::assertSame(TimestampType::CREATE_TIME, $batch->getTimestampType());
    }

    public function testABatchOfTheBrokerIsEncodedBackByteForByte(): void
    {
        $batch = RecordBatch::fromBuffer((string) hex2bin(self::BROKER_BATCH));

        self::assertSame(self::BROKER_BATCH, bin2hex($batch->toBuffer()));
        self::assertSame(self::BROKER_BATCH, bin2hex((string) $batch));
    }

    public function testTheDeltasOfTheBrokerBatchBecomeAbsoluteOffsetsAndTimestamps(): void
    {
        $records = RecordBatch::fromBuffer((string) hex2bin(self::BROKER_BATCH))->getRecords();

        self::assertCount(3, $records);
        self::assertSame([0, 1, 2], array_map(static fn(Record $r): ?int => $r->offset, $records));
        self::assertSame(
            [1788950274714, 1788950274722, 1788950274722],
            array_map(static fn(Record $r): ?int => $r->timestamp, $records)
        );
        self::assertSame(['alpha', 'bravo', 'charlie'], array_map(static fn(Record $r): ?string => $r->value, $records));
        self::assertSame([null, 'key', null], array_map(static fn(Record $r): ?string => $r->key, $records));
        self::assertSame(
            [TimestampType::CREATE_TIME, TimestampType::CREATE_TIME, TimestampType::CREATE_TIME],
            array_map(static fn(Record $r): int => $r->timestampType, $records)
        );
    }

    /**
     * @return iterable<string, array{0: int}>
     */
    public static function compressionCodecs(): iterable
    {
        yield 'none'   => [CompressionCodec::NONE];
        yield 'gzip'   => [CompressionCodec::GZIP];
        yield 'snappy' => [CompressionCodec::SNAPPY];
        yield 'lz4'    => [CompressionCodec::LZ4];
    }

    #[DataProvider('compressionCodecs')]
    public function testABatchSurvivesAWriteAndReadRoundTripInEveryCodec(int $codec): void
    {
        $batch = RecordBatch::fromRecords(self::records(), $codec);

        $read = RecordBatch::fromBuffer($batch->toBuffer());

        self::assertSame(bin2hex($batch->toBuffer()), bin2hex($read->toBuffer()));
        self::assertSame($codec, $read->getCompressionCodec());
        self::assertSame(3, $read->count(), 'the record count is readable without decompressing anything');
        self::assertEquals($batch->getRecords(), $read->getRecords());
    }

    #[DataProvider('compressionCodecs')]
    public function testOnlyTheRecordsOfABatchAreCompressed(int $codec): void
    {
        $batch = RecordBatch::fromRecords(self::records(), $codec);

        // The 61 header bytes are always plain, whatever the codec is
        $header = substr($batch->toBuffer(), 0, RecordBatch::OVERHEAD);
        $plain  = substr(RecordBatch::fromRecords(self::records(), CompressionCodec::NONE)->toBuffer(), 0, RecordBatch::OVERHEAD);

        self::assertSame(RecordBatch::OVERHEAD, strlen($header));
        self::assertSame(
            substr($plain, RecordBatch::ATTRIBUTES_OFFSET + 2),
            substr($header, RecordBatch::ATTRIBUTES_OFFSET + 2),
            'everything after the attributes of the header is the same, whatever the codec is'
        );
    }

    public function testTheChecksumIsTheCrc32COfEverythingFromTheAttributesOn(): void
    {
        $bytes = (string) hex2bin(self::BROKER_BATCH);
        $batch = RecordBatch::fromBuffer($bytes);

        self::assertSame(
            ByteUtils::crc32c(substr($bytes, RecordBatch::ATTRIBUTES_OFFSET)),
            $batch->crc,
            'the checksum covers the batch from its attributes to its end'
        );
        self::assertSame($batch->crc, $batch->computeCrc());
    }

    public function testTheChecksumDoesNotCoverTheOffsetAndTheLeaderEpoch(): void
    {
        $bytes = (string) hex2bin(self::BROKER_BATCH);
        // The broker rewrites the base offset and the partition leader epoch of every batch it appends without
        // touching the checksum, so a batch with other values in those fields is still valid
        $moved = pack('J', 4711) . substr($bytes, 8, 4) . pack('N', 3) . substr($bytes, 16);

        $batch = RecordBatch::fromBuffer($moved);

        self::assertSame(4711, $batch->getBaseOffset());
        self::assertSame(3, $batch->getPartitionLeaderEpoch());
        self::assertSame(4713, $batch->getLastOffset());
        self::assertSame(4711, $batch->getRecords()[0]->offset);
    }

    public function testABatchWithAWrongChecksumIsRefused(): void
    {
        $bytes = (string) hex2bin(self::BROKER_BATCH);
        // Flip one byte of the value of the first record, which the checksum covers
        $bytes[RecordBatch::OVERHEAD + 6] = 'A';

        $this->expectException(CorruptMessageException::class);
        RecordBatch::fromBuffer($bytes);
    }

    public function testAWrongChecksumIsAcceptedWhenTheCallerDoesNotAskForTheCheck(): void
    {
        $bytes    = (string) hex2bin(self::BROKER_BATCH);
        $bytes[RecordBatch::OVERHEAD + 6] = 'A';

        $batch = RecordBatch::fromBuffer($bytes, false);

        self::assertSame('Alpha', $batch->getRecords()[0]->value);
    }

    public function testABatchOfAnotherMessageFormatIsRefused(): void
    {
        $bytes                              = (string) hex2bin(self::BROKER_BATCH);
        $bytes[RecordBatch::MAGIC_OFFSET]   = "\x01";

        $this->expectException(CorruptMessageException::class);
        RecordBatch::fromBuffer($bytes);
    }

    public function testABatchShorterThanItsHeaderIsRefused(): void
    {
        $this->expectException(CorruptMessageException::class);
        RecordBatch::fromBuffer(substr((string) hex2bin(self::BROKER_BATCH), 0, RecordBatch::OVERHEAD - 1));
    }

    public function testABatchThatAnnouncesMoreBytesThanTheBufferHoldsIsRefused(): void
    {
        $this->expectException(CorruptMessageException::class);
        RecordBatch::fromBuffer(substr((string) hex2bin(self::BROKER_BATCH), 0, RecordBatch::OVERHEAD + 4));
    }

    public function testABatchWithMoreBytesThanRecordsIsRefused(): void
    {
        $batch  = RecordBatch::fromRecords(self::records());
        $buffer = $batch->toBuffer();
        // Announce two records where three are stored, which leaves the third one unread
        $buffer = substr($buffer, 0, RecordBatch::OVERHEAD - 4) . pack('N', 2) . substr($buffer, RecordBatch::OVERHEAD);

        $this->expectException(CorruptMessageException::class);
        RecordBatch::fromBuffer($buffer, false)->getRecords();
    }

    public function testTheHeadersOfARecordTravelWithIt(): void
    {
        $record = new Record('value', 'key', 0, null, self::CREATE_TIME, TimestampType::CREATE_TIME, [
            new Header('content-type', 'application/json'),
            new Header('null-value', null),
            new Header('empty-value', ''),
        ]);

        $read = RecordBatch::fromBuffer(RecordBatch::fromRecords([$record])->toBuffer())->getRecords()[0];

        self::assertCount(3, $read->headers);
        self::assertSame('content-type', $read->headers[0]->key);
        self::assertSame('application/json', $read->headers[0]->value);
        self::assertNull($read->headers[1]->value);
        self::assertSame('', $read->headers[2]->value);
    }

    public function testALogAppendTimeBatchGivesEveryRecordTheAppendTime(): void
    {
        $batch = RecordBatch::fromRecords(
            self::records(),
            CompressionCodec::NONE,
            0,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
            RecordBatch::NO_SEQUENCE,
            false,
            RecordBatch::NO_PARTITION_LEADER_EPOCH,
            TimestampType::LOG_APPEND_TIME
        );

        self::assertSame(TimestampType::MASK, $batch->attributes, 'bit 3 announces the LogAppendTime');
        self::assertSame(self::CREATE_TIME, $batch->getFirstTimestamp(), 'the first timestamp stays the CreateTime');
        self::assertSame(self::CREATE_TIME + 10, $batch->getMaxTimestamp());

        foreach (RecordBatch::fromBuffer($batch->toBuffer())->getRecords() as $record) {
            self::assertSame(self::CREATE_TIME + 10, $record->timestamp, 'every record answers the append time');
            self::assertSame(TimestampType::LOG_APPEND_TIME, $record->timestampType);
        }
    }

    public function testAnIdempotentBatchCarriesItsProducerAndItsSequenceNumbers(): void
    {
        $batch = RecordBatch::fromRecords(self::records(), CompressionCodec::NONE, 0, 4711, 3, 100);

        $read = RecordBatch::fromBuffer($batch->toBuffer());

        self::assertSame(4711, $read->getProducerId());
        self::assertSame(3, $read->getProducerEpoch());
        self::assertSame(100, $read->getBaseSequence());
        self::assertSame(102, $read->getLastSequence(), 'the last sequence is baseSequence + lastOffsetDelta');
        self::assertFalse($read->isTransactional());
    }

    public function testTheSequenceNumbersWrapAtTheLargestInt32(): void
    {
        $batch = RecordBatch::fromRecords(self::records(), CompressionCodec::NONE, 0, 4711, 3, 0x7FFFFFFF - 1);

        // The three sequence numbers are 2147483646, 2147483647 and then 0 again
        self::assertSame(0, $batch->getLastSequence());
    }

    public function testATransactionalBatchSetsTheTransactionalBit(): void
    {
        $batch = RecordBatch::fromRecords(self::records(), CompressionCodec::NONE, 0, 4711, 3, 0, true);

        $read = RecordBatch::fromBuffer($batch->toBuffer());

        self::assertTrue($read->isTransactional());
        self::assertFalse($read->isControlBatch());
        self::assertSame(RecordBatch::TRANSACTIONAL_FLAG_MASK, $read->attributes);
    }

    public function testAControlBatchCarriesOneEndOfTransactionMarker(): void
    {
        $marker = new EndTransactionMarker(ControlRecordType::COMMIT, 7);

        $batch = RecordBatch::fromEndTransactionMarker($marker, 4711, 3, self::CREATE_TIME, 12);
        $read  = RecordBatch::fromBuffer($batch->toBuffer());

        self::assertTrue($read->isControlBatch());
        self::assertTrue($read->isTransactional());
        self::assertSame(
            RecordBatch::CONTROL_FLAG_MASK | RecordBatch::TRANSACTIONAL_FLAG_MASK,
            $read->attributes
        );
        self::assertSame(4711, $read->getProducerId());
        self::assertSame(3, $read->getProducerEpoch());
        self::assertSame(RecordBatch::NO_SEQUENCE, $read->getBaseSequence(), 'a marker has no sequence number');
        self::assertSame(1, $read->count());

        $parsed = EndTransactionMarker::fromRecord($read->getRecords()[0]);
        self::assertSame(ControlRecordType::COMMIT, $parsed->controlType);
        self::assertSame(7, $parsed->coordinatorEpoch);
        self::assertSame(12, $read->getRecords()[0]->offset);
    }

    public function testAnEmptyBatchIsAValidBatch(): void
    {
        $batch = RecordBatch::fromRecords([]);

        self::assertSame(0, $batch->count());
        self::assertTrue($batch->isEmpty());
        self::assertSame(RecordBatch::OVERHEAD, $batch->sizeInBytes());
        self::assertSame(RecordBatch::NO_TIMESTAMP, $batch->getFirstTimestamp());

        $read = RecordBatch::fromBuffer($batch->toBuffer());
        self::assertSame([], $read->getRecords());
        self::assertSame(bin2hex($batch->toBuffer()), bin2hex($read->toBuffer()));
    }

    public function testABatchOfARecordWithoutATimestampCarriesNoTimestampAtAll(): void
    {
        $batch = RecordBatch::fromRecords([new Record('value')]);

        $record = RecordBatch::fromBuffer($batch->toBuffer())->getRecords()[0];

        self::assertSame(RecordBatch::NO_TIMESTAMP, $batch->getFirstTimestamp());
        self::assertNull($record->timestamp);
        self::assertSame(TimestampType::NO_TIMESTAMP_TYPE, $record->timestampType);
    }

    public function testABatchNeedsATimestampType(): void
    {
        $this->expectException(\UnexpectedValueException::class);
        RecordBatch::fromRecords(
            self::records(),
            CompressionCodec::NONE,
            0,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
            RecordBatch::NO_SEQUENCE,
            false,
            RecordBatch::NO_PARTITION_LEADER_EPOCH,
            TimestampType::NO_TIMESTAMP_TYPE
        );
    }

    public function testABaseOffsetShiftsEveryRecordOfTheBatch(): void
    {
        $batch = RecordBatch::fromRecords(self::records(), CompressionCodec::NONE, 1000);

        $records = RecordBatch::fromBuffer($batch->toBuffer())->getRecords();

        self::assertSame([1000, 1001, 1002], array_map(static fn(Record $r): ?int => $r->offset, $records));
        self::assertSame(1002, $batch->getLastOffset());
    }

    /**
     * The three records that the console producer of the reference capture wrote, with a fixed CreateTime
     *
     * @return list<Record>
     */
    private static function records(): array
    {
        return [
            new Record('alpha', null, 0, null, self::CREATE_TIME, TimestampType::CREATE_TIME),
            new Record('bravo', 'key', 0, null, self::CREATE_TIME + 10, TimestampType::CREATE_TIME),
            new Record('charlie', null, 0, null, self::CREATE_TIME + 5, TimestampType::CREATE_TIME),
        ];
    }
}
