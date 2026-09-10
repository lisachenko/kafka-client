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
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * A single record of a {@see RecordBatch}, i.e. the wire form of a record in the message format v2 of Kafka 0.11.
 *
 * <pre>
 *   Record =>
 *     Length         => varint
 *     Attributes     => int8
 *     TimestampDelta => varlong
 *     OffsetDelta    => varint
 *     Key            => varint-prefixed bytes
 *     Value          => varint-prefixed bytes
 *     Headers        => varint-counted [Header]
 * </pre>
 *
 * Everything that can be small is small: the lengths and the two deltas are zigzag varints, and a record carries no
 * checksum of its own - the CRC-32C of the whole batch covers all of them. `Length` counts the bytes that **follow
 * it**, exactly like the `MessageSize` of a message set entry counts the bytes of its message.
 *
 * The offset and the timestamp of a record are stored as the **difference** to the `baseOffset` and the
 * `firstTimestamp` of the batch that holds it, which is what makes a batch of records appended within a few
 * milliseconds of each other so much smaller than a message set: `offset = baseOffset + offsetDelta` and
 * `timestamp = firstTimestamp + timestampDelta`. The sequence number of an idempotent producer follows the same
 * rule, `sequence = baseSequence + offsetDelta`. Turning those back into the absolute values of the log is the job
 * of {@see RecordBatch::getRecords()}, which is where a wire record becomes the user-facing {@see Record}.
 *
 * The `Attributes` byte of a record is unused in 0.11 (`DefaultRecord.writeTo()` always writes a 0) and is kept
 * because the format reserves it.
 *
 * This class is the `Record` of the pre-schema `main` and the `DefaultRecord` of the Java client; the name
 * {@see Record} belongs to the user-facing record of every protocol line of this package, so the wire form is named
 * after the message format it is the record of, the way {@see MessageV0} is.
 *
 * @see docs/protocol/1.1.md, section "RecordBatch (message format v2)"
 * @see org/apache/kafka/common/record/DefaultRecord.java @ 0.11.0.3
 */
class RecordV2 implements BinarySchemaInterface, \Stringable
{
    /**
     * Length of the record in bytes, counting everything that follows this field
     */
    public int $length = 0;

    /**
     * Record level attributes, unused by the message format v2 and always 0
     */
    public int $attributes = 0;

    /**
     * Difference between the timestamp of this record and the `firstTimestamp` of its batch
     */
    public int $timestampDelta = 0;

    /**
     * Difference between the offset of this record and the `baseOffset` of its batch
     */
    public int $offsetDelta = 0;

    /**
     * Optional key of the record, used for partition assignment and log compaction; null is a valid value
     */
    public ?string $key = null;

    /**
     * Contents of the record as an opaque byte array; null is a valid value, and so is an empty one
     */
    public ?string $value = null;

    /**
     * Application level headers of the record, in the order the producer wrote them (KIP-82)
     *
     * @var list<Header>
     */
    public array $headers = [];

    /**
     * @param string|null  $value          Contents of the record
     * @param string|null  $key            Optional key of the record
     * @param list<Header> $headers        Headers of the record, an empty list for a record without any
     * @param int          $attributes     Attributes byte, unused by the message format v2
     * @param int          $timestampDelta Timestamp of the record minus the `firstTimestamp` of its batch
     * @param int          $offsetDelta    Offset of the record minus the `baseOffset` of its batch
     */
    public function __construct(
        ?string $value = null,
        ?string $key = null,
        array $headers = [],
        int $attributes = 0,
        int $timestampDelta = 0,
        int $offsetDelta = 0,
    ) {
        $this->value          = $value;
        $this->key            = $key;
        $this->headers        = $headers;
        $this->attributes     = $attributes;
        $this->timestampDelta = $timestampDelta;
        $this->offsetDelta    = $offsetDelta;
        $this->length         = $this->computeLength();
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'length'         => BinarySchema::TYPE_VARINT_ZIGZAG,
            'attributes'     => BinarySchema::TYPE_INT8,
            'timestampDelta' => BinarySchema::TYPE_VARLONG_ZIGZAG,
            'offsetDelta'    => BinarySchema::TYPE_VARINT_ZIGZAG,
            'key'            => BinarySchema::TYPE_VARCHAR_ZIGZAG,
            'value'          => BinarySchema::TYPE_VARCHAR_ZIGZAG,
            'headers'        => [Header::class, BinarySchema::FLAG_VARARRAY => true],
        ];
    }

    /**
     * Reads one record from a stream that is positioned at its `Length` field.
     *
     * The length is read first and the body is then decoded out of exactly that many bytes, so a record can never
     * read into the one that follows it; afterwards the body has to be exhausted, which is the check that
     * `DefaultRecord.readFrom()` makes when it compares the position it reached with the declared size.
     *
     * @throws CorruptMessageException when the record does not hold the structure its length announces
     */
    public static function unpackFrom(Stream $stream): self
    {
        $length = BinarySchema::readSingleType(BinarySchema::TYPE_VARINT_ZIGZAG, $stream, 'record->length');
        if ($length < 0) {
            throw new CorruptMessageException(
                ['error' => 'A record of a batch announces a negative length', 'length' => $length]
            );
        }

        $record         = new self();
        $record->length = $length;

        $scheme = self::getScheme();
        unset($scheme['length'], $scheme['headers']);
        try {
            $body = new StringStream($stream->readRaw($length));
            foreach ($scheme as $fieldName => $schemeType) {
                $record->$fieldName = BinarySchema::readSingleType($schemeType, $body, "record->{$fieldName}");
            }
            $record->headers = self::readHeaders($body);
        } catch (CorruptMessageException $exception) {
            throw $exception;
        } catch (\Throwable $exception) {
            throw new CorruptMessageException([
                'error'  => 'A record of a batch does not hold the structure of the message format v2',
                'reason' => $exception->getMessage(),
            ]);
        }

        if (!$body->isEmpty()) {
            throw new CorruptMessageException([
                'error'          => 'A record announces more bytes than its fields occupy',
                'declaredLength' => $length,
                'unreadBytes'    => $body->remaining(),
            ]);
        }

        return $record;
    }

    /**
     * Reads one record from a buffer that starts with its `Length` field
     *
     * @throws CorruptMessageException when the record does not hold the structure its length announces
     */
    public static function fromBuffer(string $buffer): self
    {
        return self::unpackFrom(new StringStream($buffer));
    }

    /**
     * Returns the size of the serialized record in bytes, its `Length` field and the varint that carries it
     */
    public function sizeInBytes(): int
    {
        return ByteUtils::sizeOfVarint($this->length) + $this->length;
    }

    /**
     * Serializes the record
     */
    public function toBuffer(): string
    {
        $stream = new StringStream();
        BinarySchema::writeObjectToStream($this, $stream);

        return $stream->getBuffer();
    }

    public function __toString(): string
    {
        return $this->toBuffer();
    }

    /**
     * Reads the varint-counted headers of a record, refusing the two shapes the format forbids.
     *
     * `DefaultRecord.readFrom()` refuses a **negative** header count, which the array notation of the engine would
     * silently read as an empty list, and `readHeaders()` refuses a header with a **null key**: a header key is a
     * string that is always there, and only a header *value* may be null.
     *
     * @return list<Header>
     *
     * @throws CorruptMessageException on a negative header count or a header without a key
     */
    private static function readHeaders(StringStream $body): array
    {
        $count = BinarySchema::readSingleType(BinarySchema::TYPE_VARINT_ZIGZAG, $body, 'record->headers[size]');
        if ($count < 0) {
            throw new CorruptMessageException([
                'error'       => 'A record of a batch announces a negative number of headers',
                'headerCount' => $count,
            ]);
        }

        $headers = [];
        for ($index = 0; $index < $count; $index++) {
            try {
                /** @var Header $header */
                $header = BinarySchema::readObjectFromStream(Header::class, $body, "record->headers[{$index}]");
            } catch (\TypeError) {
                throw new CorruptMessageException([
                    'error'  => 'A header of a record carries a null key, which the message format v2 forbids',
                    'header' => $index,
                ]);
            }
            $headers[] = $header;
        }

        return $headers;
    }

    /**
     * Returns the value the `Length` field has to carry for the current contents of the record
     *
     * `Length` counts every byte that follows it, so it is the size of the whole record minus the size of the varint
     * that holds the length itself - and that varint is the only field whose size depends on the length.
     */
    private function computeLength(): int
    {
        $length       = $this->length;
        $this->length = 0;
        $sizeWithZero = BinarySchema::getObjectTypeSize($this);
        $this->length = $length;

        return $sizeWithZero - ByteUtils::sizeOfVarint(0);
    }
}
