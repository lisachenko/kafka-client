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

namespace Protocol\Kafka\Common\Record;

use Protocol\Kafka\Common\Errors\CorruptMessageException;
use Protocol\Kafka\Common\Utils\ByteUtils;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * The record batch of the message format v2, the wire structure that Kafka 0.11 replaced the message set with.
 *
 * <pre>
 *   RecordBatch =>
 *     BaseOffset           => int64
 *     BatchLength          => int32
 *     PartitionLeaderEpoch => int32
 *     Magic                => int8 (2)
 *     Crc                  => int32 (CRC-32C, unsigned)
 *     Attributes           => int16
 *     LastOffsetDelta      => int32
 *     FirstTimestamp       => int64
 *     MaxTimestamp         => int64
 *     ProducerId           => int64
 *     ProducerEpoch        => int16
 *     BaseSequence         => int32
 *     RecordCount          => int32
 *     Records              => the RecordCount records, compressed as one block when a codec is announced
 * </pre>
 *
 * A batch is the unit of everything now: of compression, of the checksum, of a transaction and of the sequence
 * numbers of an idempotent producer. Only its header is fixed width - 61 bytes, {@see RecordBatch::OVERHEAD} - and
 * everything inside a {@see RecordV2} is varint-encoded and stored as a delta to the `baseOffset` and the
 * `firstTimestamp` of this header.
 *
 * Four properties of the format shape this class:
 *
 *  * the **checksum** is a CRC-32C (Castagnoli, {@see ByteUtils::crc32c()}) over everything from the `Attributes`
 *    field to the end of the batch. It sits *after* the magic byte, so a reader parses the magic before it knows how
 *    to read the rest, and it does not cover `BaseOffset`, `BatchLength` and `PartitionLeaderEpoch`: the broker
 *    assigns the offset and the leader epoch on append and must not have to recompute the checksum for it;
 *  * **compression** compresses the records alone. There is no wrapper message any more: the 61 header bytes -
 *    `RecordCount` included - are always plain, and the bytes that follow them are the compressed block of records.
 *    A batch therefore always knows how many records it holds without decompressing anything;
 *  * `LastOffsetDelta` and `BaseSequence` survive **log compaction**: a compacted batch keeps the offsets and the
 *    sequence numbers of the records that were removed, so that a broker can rebuild the state of a producer from
 *    the log. This is also why a batch may hold zero records and still exist;
 *  * a **control batch** (bit 5 of the attributes) carries markers of the transaction protocol instead of data and
 *    is never handed to an application, see {@see ControlRecordType}.
 *
 * The `Attributes` are an int16 in this format, not the int8 of a message:
 *
 * <pre>
 *   | Unused (6-15) | Control (5) | Transactional (4) | Timestamp type (3) | Compression codec (0-2) |
 * </pre>
 *
 * The bytes of the records are kept exactly as they were read, which is what makes {@see RecordBatch::toBuffer()}
 * reproduce a captured batch byte for byte: re-compressing a block with another implementation of gzip, snappy or
 * lz4 would give different - equally valid - bytes.
 *
 * @see docs/protocol/2.8.md, section "RecordBatch (message format v2)"
 * @see org/apache/kafka/common/record/DefaultRecordBatch.java @ 0.11.0.3
 */
class RecordBatch implements BinarySchemaInterface, \Countable, \Stringable
{
    /**
     * Message format of a record batch, the third one of the protocol
     */
    public const int MAGIC = 2;

    /**
     * Size of the `BaseOffset` and `BatchLength` fields, which are not part of the length they announce
     */
    public const int LOG_OVERHEAD = 8 + 4;

    /**
     * Size of the header of a batch, up to and including the `RecordCount` field
     */
    public const int OVERHEAD = 61;

    /**
     * Position of the `Magic` byte inside a batch; a message set entry carries its magic at the same position
     */
    public const int MAGIC_OFFSET = 16;

    /**
     * Position of the `Attributes` field, where the region covered by the checksum starts
     */
    public const int ATTRIBUTES_OFFSET = 21;

    /**
     * Mask of the attributes bit that marks a batch of a transaction (bit 4)
     */
    public const int TRANSACTIONAL_FLAG_MASK = 0x10;

    /**
     * Mask of the attributes bit that marks a control batch (bit 5)
     */
    public const int CONTROL_FLAG_MASK = 0x20;

    /**
     * Timestamp of a batch that carries none, the `RecordBatch.NO_TIMESTAMP` of the broker
     */
    public const int NO_TIMESTAMP = -1;

    /**
     * Producer id of a batch that no idempotent or transactional producer wrote
     */
    public const int NO_PRODUCER_ID = -1;

    /**
     * Producer epoch of a batch that no idempotent or transactional producer wrote
     */
    public const int NO_PRODUCER_EPOCH = -1;

    /**
     * Base sequence of a batch that carries no sequence numbers, and of every control batch
     */
    public const int NO_SEQUENCE = -1;

    /**
     * Partition leader epoch that a producer writes, the value the broker replaces on append (KIP-101)
     */
    public const int NO_PARTITION_LEADER_EPOCH = -1;

    /**
     * Offset of the first record of the batch; a producer writes 0 and the broker assigns the real one
     */
    public int $baseOffset = 0;

    /**
     * Size of everything that follows this field, i.e. the size of the batch minus {@see RecordBatch::LOG_OVERHEAD}
     */
    public int $batchLength = 0;

    /**
     * Epoch of the partition leader that appended this batch, assigned by the broker (KIP-101)
     */
    public int $partitionLeaderEpoch = self::NO_PARTITION_LEADER_EPOCH;

    /**
     * Version id of the batch binary format, always {@see RecordBatch::MAGIC}
     */
    public int $magic = self::MAGIC;

    /**
     * CRC-32C of the batch from its `Attributes` field on, as an unsigned int32
     */
    public int $crc = 0;

    /**
     * Metadata of the batch: bits 0-2 the codec, bit 3 the timestamp type, bit 4 transactional, bit 5 control
     */
    public int $attributes = 0;

    /**
     * Offset of the last record of the batch, relative to the `BaseOffset`
     */
    public int $lastOffsetDelta = 0;

    /**
     * Timestamp of the first record of the batch, the base of every `timestampDelta`
     */
    public int $firstTimestamp = self::NO_TIMESTAMP;

    /**
     * Largest timestamp of the batch, or the append time of the broker for a `LogAppendTime` batch
     */
    public int $maxTimestamp = self::NO_TIMESTAMP;

    /**
     * Producer id of the idempotent or transactional producer that wrote the batch (KIP-98)
     */
    public int $producerId = self::NO_PRODUCER_ID;

    /**
     * Epoch of that producer, which fences a zombie instance of it
     */
    public int $producerEpoch = self::NO_PRODUCER_EPOCH;

    /**
     * Sequence number of the first record of the batch, which the broker deduplicates on
     */
    public int $baseSequence = self::NO_SEQUENCE;

    /**
     * Number of records the batch holds, readable without decompressing them
     */
    public int $recordCount = 0;

    /**
     * The records of the batch exactly as they are on the wire, compressed when the attributes announce a codec
     */
    private string $recordsBuffer = '';

    /**
     * Decoded records of the batch, read from the buffer on the first access
     *
     * @var list<RecordV2>|null
     */
    private ?array $decodedRecords = null;

    /**
     * A batch is built by one of the factories, which are the only places that keep its fields consistent
     */
    private function __construct() {}

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'baseOffset'           => BinarySchema::TYPE_INT64,
            'batchLength'          => BinarySchema::TYPE_INT32,
            'partitionLeaderEpoch' => BinarySchema::TYPE_INT32,
            'magic'                => BinarySchema::TYPE_INT8,
            'crc'                  => BinarySchema::TYPE_INT32,
            'attributes'           => BinarySchema::TYPE_INT16,
            'lastOffsetDelta'      => BinarySchema::TYPE_INT32,
            'firstTimestamp'       => BinarySchema::TYPE_INT64,
            'maxTimestamp'         => BinarySchema::TYPE_INT64,
            'producerId'           => BinarySchema::TYPE_INT64,
            'producerEpoch'        => BinarySchema::TYPE_INT16,
            'baseSequence'         => BinarySchema::TYPE_INT32,
            'recordCount'          => BinarySchema::TYPE_INT32,
        ];
    }

    /**
     * Builds a batch out of user-facing records, compressing the records part as a whole when a codec is given.
     *
     * The offsets of a produced batch are ignored by the broker, which assigns the real ones on append, so the
     * records are numbered 0, 1, 2 … and the `baseOffset` is 0 - the offset deltas of the records are the same in
     * the log as they are here, which is the whole point of the delta encoding. The `firstTimestamp` is the
     * timestamp of the first record and every other record stores its difference to it.
     *
     * With {@see TimestampType::CREATE_TIME}, the type a producer always writes, the `maxTimestamp` is the largest
     * timestamp of the batch; with {@see TimestampType::LOG_APPEND_TIME} that field *is* the append time of the
     * whole batch and every record reads it back as its own timestamp, so a batch built with that type carries the
     * largest timestamp of its records as the append time of all of them.
     *
     * @param iterable<Record> $records              Records to write, with their `CreateTime` timestamps
     * @param int              $compressionCodec     One of the {@see CompressionCodec} constants
     * @param int              $baseOffset           Offset of the first record; a producer leaves it at 0
     * @param int              $producerId           Producer id of an idempotent producer, -1 without one (KIP-98)
     * @param int              $producerEpoch        Epoch of that producer, -1 without one
     * @param int              $baseSequence         Sequence number of the first record, -1 without a producer id
     * @param bool             $isTransactional      Whether the batch belongs to a transaction
     * @param int              $partitionLeaderEpoch Leader epoch, -1 for everything a client writes (KIP-101)
     * @param int              $timestampType        One of the {@see TimestampType} constants
     */
    public static function fromRecords(
        iterable $records,
        int $compressionCodec = CompressionCodec::NONE,
        int $baseOffset = 0,
        int $producerId = self::NO_PRODUCER_ID,
        int $producerEpoch = self::NO_PRODUCER_EPOCH,
        int $baseSequence = self::NO_SEQUENCE,
        bool $isTransactional = false,
        int $partitionLeaderEpoch = self::NO_PARTITION_LEADER_EPOCH,
        int $timestampType = TimestampType::CREATE_TIME,
    ): self {
        $wireRecords    = [];
        $firstTimestamp = null;
        $maxTimestamp   = self::NO_TIMESTAMP;
        $offsetDelta    = 0;

        foreach ($records as $record) {
            $timestamp      = $record->timestamp ?? self::NO_TIMESTAMP;
            $firstTimestamp ??= $timestamp;
            $maxTimestamp   = max($maxTimestamp, $timestamp);

            $wireRecords[] = new RecordV2(
                $record->value,
                $record->key,
                array_values($record->headers),
                0,
                $timestamp - $firstTimestamp,
                $offsetDelta++
            );
        }

        return self::of(
            $wireRecords,
            $compressionCodec,
            $baseOffset,
            $firstTimestamp ?? self::NO_TIMESTAMP,
            $maxTimestamp,
            $producerId,
            $producerEpoch,
            $baseSequence,
            $isTransactional,
            false,
            $partitionLeaderEpoch,
            $timestampType
        );
    }

    /**
     * Builds the control batch of an end of transaction marker, the batch a transaction coordinator writes.
     *
     * Such a batch holds exactly one record, is always transactional, never compressed and carries no sequence
     * number of its own - `WriteTxnMarkers` and `MemoryRecords.withEndTransactionMarker()` build it this way.
     *
     * @param EndTransactionMarker $marker        The commit or the abort of the transaction, with its epoch
     * @param int                  $producerId    Producer id of the transaction that ends here
     * @param int                  $producerEpoch Epoch of that producer
     * @param int                  $timestamp     Time the coordinator wrote the marker at
     * @param int                  $baseOffset    Offset of the marker; a coordinator lets the broker assign it
     */
    public static function fromEndTransactionMarker(
        EndTransactionMarker $marker,
        int $producerId,
        int $producerEpoch,
        int $timestamp,
        int $baseOffset = 0,
    ): self {
        $record = new RecordV2($marker->serializeValue(), $marker->serializeKey());

        return self::of(
            [$record],
            CompressionCodec::NONE,
            $baseOffset,
            $timestamp,
            $timestamp,
            $producerId,
            $producerEpoch,
            self::NO_SEQUENCE,
            true,
            true,
            self::NO_PARTITION_LEADER_EPOCH,
            TimestampType::CREATE_TIME
        );
    }

    /**
     * Reads a batch out of a buffer that starts with its `BaseOffset` field.
     *
     * The buffer has to hold the whole batch; everything past the announced `BatchLength` is left alone, which is
     * what lets {@see MemoryRecords} hand over a region that holds more than one batch.
     *
     * @param bool $checkCrcs Whether to validate the CRC-32C of the batch, the `check.crcs` behaviour of the
     *                        official clients
     *
     * @throws CorruptMessageException on a batch of another message format, of an impossible size, or with a
     *                                 checksum that does not match its contents
     */
    public static function fromBuffer(string $buffer, bool $checkCrcs = true): self
    {
        $available = strlen($buffer);
        if ($available < self::OVERHEAD) {
            throw new CorruptMessageException([
                'error'          => 'A record batch is shorter than the header of the message format v2',
                'availableBytes' => $available,
                'minimumBytes'   => self::OVERHEAD,
            ]);
        }

        $batch = new self();
        $header = new StringStream(substr($buffer, 0, self::OVERHEAD));
        foreach (self::getScheme() as $fieldName => $schemeType) {
            $batch->$fieldName = BinarySchema::readSingleType($schemeType, $header, "recordBatch->{$fieldName}");
        }
        // The engine reads an int32 as a signed value, while a CRC-32C is the unsigned number the polynomial gives
        $batch->crc = $batch->crc & 0xFFFFFFFF;

        if ($batch->magic !== self::MAGIC) {
            throw new CorruptMessageException([
                'error' => 'A record batch announces a message format that is not the record batch v2',
                'magic' => $batch->magic,
            ]);
        }
        $sizeInBytes = self::LOG_OVERHEAD + $batch->batchLength;
        if ($batch->batchLength < self::OVERHEAD - self::LOG_OVERHEAD || $sizeInBytes > $available) {
            throw new CorruptMessageException([
                'error'          => 'A record batch announces a length that its bytes can not hold',
                'batchLength'    => $batch->batchLength,
                'availableBytes' => $available,
            ]);
        }
        if ($batch->recordCount < 0) {
            throw new CorruptMessageException([
                'error'       => 'A record batch announces a negative record count',
                'recordCount' => $batch->recordCount,
            ]);
        }

        $batch->recordsBuffer = substr($buffer, self::OVERHEAD, $sizeInBytes - self::OVERHEAD);
        if ($checkCrcs) {
            $batch->validateCrc();
        }

        return $batch;
    }

    /**
     * Serializes the batch into the byte region that a produce or a fetch partition carries
     */
    public function toBuffer(): string
    {
        $stream = new StringStream();
        BinarySchema::writeObjectToStream($this, $stream);
        $stream->writeBuffer($this->recordsBuffer);

        return $stream->getBuffer();
    }

    /**
     * Returns the size of the serialized batch in bytes, the header and the records part together
     */
    public function sizeInBytes(): int
    {
        return self::LOG_OVERHEAD + $this->batchLength;
    }

    /**
     * Returns the records of the batch, with the absolute offsets and timestamps that the deltas stand for.
     *
     * The timestamp of a record of a `CreateTime` batch is `firstTimestamp + timestampDelta`; in a `LogAppendTime`
     * batch the broker stamped the whole batch at once and every record answers the `maxTimestamp` of the batch,
     * which is that append time (`DefaultRecordBatch.RecordIterator`). A control batch answers its markers here -
     * it is {@see MemoryRecords::getRecords()} that keeps them away from an application.
     *
     * @return list<Record>
     *
     * @throws CorruptMessageException when the records part does not hold the records the header announces
     */
    public function getRecords(): array
    {
        $isLogAppendTime = $this->getTimestampType() === TimestampType::LOG_APPEND_TIME;
        $timestampType   = $isLogAppendTime ? TimestampType::LOG_APPEND_TIME : TimestampType::CREATE_TIME;

        $records = [];
        foreach ($this->getWireRecords() as $wireRecord) {
            $timestamp = $isLogAppendTime
                ? $this->maxTimestamp
                : $this->firstTimestamp + $wireRecord->timestampDelta;

            $records[] = new Record(
                $wireRecord->value,
                $wireRecord->key,
                $wireRecord->attributes,
                $this->baseOffset + $wireRecord->offsetDelta,
                $timestamp === self::NO_TIMESTAMP ? null : $timestamp,
                $timestamp === self::NO_TIMESTAMP ? TimestampType::NO_TIMESTAMP_TYPE : $timestampType,
                $wireRecord->headers
            );
        }

        return $records;
    }

    /**
     * Returns the records of the batch in their wire form, with the deltas the batch stores
     *
     * @return list<RecordV2>
     *
     * @throws CorruptMessageException when the records part does not hold the records the header announces
     */
    public function getWireRecords(): array
    {
        if ($this->decodedRecords !== null) {
            return $this->decodedRecords;
        }

        $buffer = CompressionCodec::decompress($this->getCompressionCodec(), $this->recordsBuffer);
        $stream = new StringStream($buffer);

        $records = [];
        for ($index = 0; $index < $this->recordCount; $index++) {
            $records[] = RecordV2::unpackFrom($stream);
        }
        if (!$stream->isEmpty()) {
            throw new CorruptMessageException([
                'error'       => 'A record batch holds more bytes than the records it announces occupy',
                'recordCount' => $this->recordCount,
                'unreadBytes' => $stream->remaining(),
            ]);
        }

        return $this->decodedRecords = $records;
    }

    /**
     * Returns the compression codec that the attributes of the batch announce
     */
    public function getCompressionCodec(): int
    {
        return CompressionCodec::fromAttributes($this->attributes);
    }

    /**
     * Returns the timestamp type that the attributes of the batch announce
     */
    public function getTimestampType(): int
    {
        return ($this->attributes & TimestampType::MASK) === 0
            ? TimestampType::CREATE_TIME
            : TimestampType::LOG_APPEND_TIME;
    }

    /**
     * Tells whether the batch belongs to a transaction (bit 4 of the attributes)
     */
    public function isTransactional(): bool
    {
        return ($this->attributes & self::TRANSACTIONAL_FLAG_MASK) !== 0;
    }

    /**
     * Tells whether the batch carries markers of the transaction protocol instead of data (bit 5 of the attributes)
     */
    public function isControlBatch(): bool
    {
        return ($this->attributes & self::CONTROL_FLAG_MASK) !== 0;
    }

    /**
     * Returns the offset of the first record of the batch
     */
    public function getBaseOffset(): int
    {
        return $this->baseOffset;
    }

    /**
     * Returns the offset of the last record of the batch, `baseOffset + lastOffsetDelta`
     */
    public function getLastOffset(): int
    {
        return $this->baseOffset + $this->lastOffsetDelta;
    }

    /**
     * Returns the timestamp of the first record of the batch, the base of every timestamp delta
     */
    public function getFirstTimestamp(): int
    {
        return $this->firstTimestamp;
    }

    /**
     * Returns the largest timestamp of the batch, which is the append time of a `LogAppendTime` batch
     */
    public function getMaxTimestamp(): int
    {
        return $this->maxTimestamp;
    }

    /**
     * Returns the producer id of the batch, -1 for a batch that no idempotent producer wrote
     */
    public function getProducerId(): int
    {
        return $this->producerId;
    }

    /**
     * Returns the epoch of that producer, -1 for a batch that no idempotent producer wrote
     */
    public function getProducerEpoch(): int
    {
        return $this->producerEpoch;
    }

    /**
     * Returns the sequence number of the first record of the batch, -1 when it carries none
     */
    public function getBaseSequence(): int
    {
        return $this->baseSequence;
    }

    /**
     * Returns the sequence number of the last record of the batch, -1 when it carries none.
     *
     * The sequence numbers of a producer are int32 values that wrap around, which is why the last one is not simply
     * the sum: `DefaultRecordBatch.incrementSequence()` continues at 0 after `Integer.MAX_VALUE`.
     */
    public function getLastSequence(): int
    {
        if ($this->baseSequence === self::NO_SEQUENCE) {
            return self::NO_SEQUENCE;
        }

        return self::incrementSequence($this->baseSequence, $this->lastOffsetDelta);
    }

    /**
     * Returns the epoch of the partition leader that appended the batch, -1 for a batch a client built
     */
    public function getPartitionLeaderEpoch(): int
    {
        return $this->partitionLeaderEpoch;
    }

    /**
     * Returns the number of records of the batch, without decompressing them
     */
    public function count(): int
    {
        return $this->recordCount;
    }

    /**
     * Tells whether the batch holds no record at all, which a compacted batch may well not
     */
    public function isEmpty(): bool
    {
        return $this->recordCount === 0;
    }

    /**
     * Returns the CRC-32C of the current contents of the batch, as an unsigned int32
     */
    public function computeCrc(): int
    {
        return ByteUtils::crc32c(substr($this->toBuffer(), self::ATTRIBUTES_OFFSET));
    }

    /**
     * Verifies the checksum of the batch, the way the broker and the official clients do on every read
     *
     * @throws CorruptMessageException when the checksum does not match the contents of the batch
     */
    public function validateCrc(): void
    {
        $expectedCrc = $this->computeCrc();
        if ($expectedCrc !== $this->crc) {
            throw new CorruptMessageException(['expectedCrc' => $expectedCrc, 'actualCrc' => $this->crc]);
        }
    }

    public function __toString(): string
    {
        return $this->toBuffer();
    }

    /**
     * Advances a producer sequence number, wrapping around at the largest int32 as `incrementSequence()` does
     */
    private static function incrementSequence(int $baseSequence, int $increment): int
    {
        if ($baseSequence > 0x7FFFFFFF - $increment) {
            return $increment - (0x7FFFFFFF - $baseSequence) - 1;
        }

        return $baseSequence + $increment;
    }

    /**
     * Assembles a batch out of wire records and the values of its header, computing the derived fields
     *
     * @param list<RecordV2> $wireRecords
     */
    private static function of(
        array $wireRecords,
        int $compressionCodec,
        int $baseOffset,
        int $firstTimestamp,
        int $maxTimestamp,
        int $producerId,
        int $producerEpoch,
        int $baseSequence,
        bool $isTransactional,
        bool $isControlBatch,
        int $partitionLeaderEpoch,
        int $timestampType,
    ): self {
        if (!TimestampType::isValid($timestampType) || $timestampType === TimestampType::NO_TIMESTAMP_TYPE) {
            throw new \UnexpectedValueException(
                "A record batch needs a timestamp type, {$timestampType} given"
            );
        }

        $plain = '';
        foreach ($wireRecords as $wireRecord) {
            $plain .= $wireRecord->toBuffer();
        }

        $batch                       = new self();
        $batch->baseOffset           = $baseOffset;
        $batch->partitionLeaderEpoch = $partitionLeaderEpoch;
        $batch->magic                = self::MAGIC;
        $batch->attributes           = self::attributesOf(
            $compressionCodec,
            $timestampType,
            $isTransactional,
            $isControlBatch
        );
        $batch->lastOffsetDelta = $wireRecords === [] ? 0 : $wireRecords[count($wireRecords) - 1]->offsetDelta;
        $batch->firstTimestamp  = $firstTimestamp;
        $batch->maxTimestamp    = $maxTimestamp;
        $batch->producerId      = $producerId;
        $batch->producerEpoch   = $producerEpoch;
        $batch->baseSequence    = $baseSequence;
        $batch->recordCount     = count($wireRecords);
        $batch->recordsBuffer   = CompressionCodec::compress($compressionCodec, $plain, self::MAGIC);
        $batch->decodedRecords  = $wireRecords;
        $batch->batchLength     = self::OVERHEAD - self::LOG_OVERHEAD + strlen($batch->recordsBuffer);
        $batch->crc             = $batch->computeCrc();

        return $batch;
    }

    /**
     * Builds the `Attributes` field out of the four properties it holds, `DefaultRecordBatch.computeAttributes()`
     */
    private static function attributesOf(
        int $compressionCodec,
        int $timestampType,
        bool $isTransactional,
        bool $isControlBatch,
    ): int {
        $attributes = $compressionCodec & CompressionCodec::MASK;
        if ($timestampType === TimestampType::LOG_APPEND_TIME) {
            $attributes |= TimestampType::MASK;
        }
        if ($isTransactional) {
            $attributes |= self::TRANSACTIONAL_FLAG_MASK;
        }
        if ($isControlBatch) {
            $attributes |= self::CONTROL_FLAG_MASK;
        }

        return $attributes;
    }
}
