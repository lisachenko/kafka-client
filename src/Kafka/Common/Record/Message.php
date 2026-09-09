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
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * A single message of the wire format, message format v1 (Kafka 0.10.0, KIP-31/32).
 *
 * <pre>
 *   Message => Crc MagicByte Attributes Timestamp Key Value
 *     Crc        => int32
 *     MagicByte  => int8  (1)
 *     Attributes => int8
 *     Timestamp  => int64 (only present when MagicByte is 1)
 *     Key        => bytes
 *     Value      => bytes
 * </pre>
 *
 * Message format v1 added the `Timestamp` field between the attributes and the key, and gave bit 3 of the
 * `Attributes` byte to the {@see TimestampType} that says where the value comes from; the three lowest bits still
 * hold the compression codec. A timestamp of {@see Message::NO_TIMESTAMP} (-1) means that the message carries none.
 *
 * The magic byte selects the layout, so it is read before the rest of the message and decides the class the bytes
 * are decoded into: this class for v1, {@see MessageV0} for the format of Kafka 0.8 and 0.9, which has no timestamp
 * and no timestamp type. A produced set is written in the format its producer asked for; the broker converts between
 * the two in both directions, driven by the `message.format.version` of the topic and by the version of the Fetch
 * request that asks for the log.
 *
 * A compressed message is a regular message whose Value is a complete, compressed {@see MessageSet}; the codec it
 * was compressed with is announced by the three lowest bits of the Attributes byte, and in message format v1 the
 * offsets of the inner messages are relative to the offset of that wrapper message.
 *
 * @see docs/protocol/0.11.0.md, section "MessageSet and Message"
 * @see kafka/message/Message.scala @ 0.10.2.2
 */
class Message implements BinarySchemaInterface, \Stringable
{
    /**
     * Message format v0, the only format that Kafka 0.8 and 0.9 know: no timestamp field
     */
    public const int MAGIC_V0 = 0;

    /**
     * Message format v1, added by Kafka 0.10.0: an int64 timestamp and a timestamp type in the attributes
     */
    public const int MAGIC_V1 = 1;

    /**
     * Message format that this class describes, the highest one of this protocol line
     */
    public const int MAGIC = self::MAGIC_V1;

    /**
     * Size of a message format v0 with a null key and a null value: crc, magic, attributes and two length prefixes
     */
    public const int MIN_SIZE_V0 = 4 + 1 + 1 + 4 + 4;

    /**
     * Size of a message format v1 with a null key and a null value: the fields of v0 plus the int64 timestamp
     */
    public const int MIN_SIZE_V1 = self::MIN_SIZE_V0 + 8;

    /**
     * Size of a message of this format with a null key and a null value
     */
    public const int MIN_SIZE = self::MIN_SIZE_V1;

    /**
     * Timestamp of a message that has none, the `Message.NoTimestamp` of the broker
     */
    public const int NO_TIMESTAMP = -1;

    /**
     * CRC-32 (IEEE) of the message bytes that follow this field, i.e. of MagicByte, Attributes, Timestamp (v1 only),
     * Key and Value.
     *
     * The value is the unsigned one that {@see crc32()} returns; it is written and read as an int32.
     */
    public int $crc = 0;

    /**
     * Version id of the message binary format, the magic of the class this message is an instance of
     */
    public int $magicByte = self::MAGIC;

    /**
     * Metadata about the message: bits 0-2 hold the compression codec, bit 3 the timestamp type in format v1
     */
    public int $attributes = 0;

    /**
     * Milliseconds since the epoch, {@see Message::NO_TIMESTAMP} for a message without a timestamp.
     *
     * The field only exists in message format v1; a {@see MessageV0} keeps it at {@see Message::NO_TIMESTAMP} and
     * never writes it.
     */
    public int $timestamp = self::NO_TIMESTAMP;

    /**
     * Optional key of the message, used for partition assignment and log compaction; null is a valid value
     */
    public ?string $key = null;

    /**
     * Contents of the message as an opaque byte array; null is a valid value, and so is an empty one
     */
    public ?string $value = null;

    /**
     * @param string|null $value      Contents of the message, or the compressed inner message set of a wrapper
     * @param string|null $key        Optional key of the message
     * @param int         $attributes Attributes byte, carrying the compression codec in its three lowest bits and
     *                                the timestamp type in bit 3
     * @param int         $timestamp  Milliseconds since the epoch, -1 for a message without a timestamp
     */
    public function __construct(
        ?string $value = null,
        ?string $key = null,
        int $attributes = 0,
        int $timestamp = self::NO_TIMESTAMP
    ) {
        $this->value      = $value;
        $this->key        = $key;
        $this->attributes = $attributes;
        $this->magicByte  = static::MAGIC;
        $this->timestamp  = static::MAGIC === self::MAGIC_V0 ? self::NO_TIMESTAMP : $timestamp;
        $this->crc        = $this->computeCrc();
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $scheme = [
            'crc'        => BinarySchema::TYPE_INT32,
            'magicByte'  => BinarySchema::TYPE_INT8,
            'attributes' => BinarySchema::TYPE_INT8,
        ];

        // The Timestamp field is the whole difference between the two message formats of this protocol line
        if (static::MAGIC > self::MAGIC_V0) {
            $scheme['timestamp'] = BinarySchema::TYPE_INT64;
        }

        return $scheme + [
            'key'   => BinarySchema::TYPE_BYTEARRAY,
            'value' => BinarySchema::TYPE_BYTEARRAY,
        ];
    }

    /**
     * Creates a message of the given message format
     *
     * @param int         $magic      One of {@see Message::MAGIC_V0} and {@see Message::MAGIC_V1}
     * @param string|null $value      Contents of the message
     * @param string|null $key        Optional key of the message
     * @param int         $attributes Attributes byte of the message
     * @param int         $timestamp  Milliseconds since the epoch, ignored for message format v0
     *
     * @throws CorruptMessageException for a magic byte that this protocol line does not know
     */
    public static function ofMagic(
        int $magic,
        ?string $value = null,
        ?string $key = null,
        int $attributes = 0,
        int $timestamp = self::NO_TIMESTAMP
    ): self {
        $class = self::classOfMagic($magic);

        return new $class($value, $key, $attributes, $timestamp);
    }

    /**
     * Creates the wrapper message of a compressed message set
     *
     * The wrapper of a message format v1 set carries the timestamp of the whole set - the largest timestamp of its
     * inner messages for `CreateTime`, the append time of the broker for `LogAppendTime` - and it is the only message
     * of the set whose attributes announce the codec and the timestamp type.
     *
     * @param string $messageSetBuffer Serialized inner message set, compressed by this method
     * @param int    $compressionCodec One of the {@see CompressionCodec} constants, other than NONE
     * @param int    $timestamp        Timestamp of the wrapper message, ignored for message format v0
     * @param int    $timestampType    Timestamp type to announce in the attributes of the wrapper
     */
    public static function compressed(
        string $messageSetBuffer,
        int $compressionCodec,
        int $timestamp = self::NO_TIMESTAMP,
        int $timestampType = TimestampType::CREATE_TIME
    ): static {
        $attributes = $compressionCodec;
        if (static::MAGIC > self::MAGIC_V0) {
            $attributes = TimestampType::updateAttributes($attributes, $timestampType);
        }

        return new static(
            CompressionCodec::compress($compressionCodec, $messageSetBuffer, static::MAGIC),
            null,
            $attributes,
            $timestamp
        );
    }

    /**
     * Returns the class that decodes messages of the given magic byte
     *
     * @return class-string<self>
     *
     * @throws CorruptMessageException for a magic byte that this protocol line does not know
     */
    public static function classOfMagic(int $magic): string
    {
        return match ($magic) {
            self::MAGIC_V0 => MessageV0::class,
            self::MAGIC_V1 => self::class,
            default        => throw new CorruptMessageException([
                'error' => 'A message announces a message format that Kafka 0.10.2.2 does not know',
                'magic' => $magic,
            ]),
        };
    }

    /**
     * Returns the size of a message of the given magic with a null key and a null value
     */
    public static function minSizeOfMagic(int $magic): int
    {
        return $magic === self::MAGIC_V0 ? self::MIN_SIZE_V0 : self::MIN_SIZE_V1;
    }

    /**
     * Reads a message from a stream that is positioned at its Crc field.
     *
     * The magic byte decides the layout of everything that follows it, so it is read before the rest of the fields
     * and selects the class the message is decoded into: {@see MessageV0} or this one.
     *
     * @param bool $checkCrcs Whether to validate the checksum of the message, the `check.crcs` behaviour of the
     *                        official clients
     *
     * @throws CorruptMessageException when the magic byte is unknown or the checksum does not match the contents
     */
    public static function unpackFrom(Stream $stream, bool $checkCrcs = true): self
    {
        $crc   = BinarySchema::readSingleType(BinarySchema::TYPE_INT32, $stream, 'message->crc');
        $magic = BinarySchema::readSingleType(BinarySchema::TYPE_INT8, $stream, 'message->magicByte');

        $class   = self::classOfMagic($magic);
        $message = new $class();
        // The engine reads an int32 as a signed value, while a checksum is the unsigned number crc32() returns
        $message->crc = $crc & 0xFFFFFFFF;

        $scheme = $class::getScheme();
        unset($scheme['crc'], $scheme['magicByte']);
        foreach ($scheme as $fieldName => $schemeType) {
            $message->$fieldName = BinarySchema::readSingleType($schemeType, $stream, "message->{$fieldName}");
        }

        if ($checkCrcs) {
            $message->validateCrc();
        }

        return $message;
    }

    /**
     * Reads a message from a buffer that holds exactly its bytes
     *
     * @throws CorruptMessageException when the magic byte is unknown or the checksum does not match the contents
     */
    public static function fromBuffer(string $buffer, bool $checkCrcs = true): self
    {
        return self::unpackFrom(new StringStream($buffer), $checkCrcs);
    }

    /**
     * Returns the compression codec that this message announces in its Attributes byte
     */
    public function getCompressionCodec(): int
    {
        return CompressionCodec::fromAttributes($this->attributes);
    }

    /**
     * Returns the timestamp type that this message announces in its Attributes byte
     *
     * A message of format v0 has no timestamp at all and answers {@see TimestampType::NO_TIMESTAMP_TYPE}.
     */
    public function getTimestampType(): int
    {
        return TimestampType::fromAttributes($this->attributes, static::MAGIC);
    }

    /**
     * Returns the timestamp of this message, or null when it carries none
     */
    public function getTimestamp(): ?int
    {
        if (static::MAGIC === self::MAGIC_V0 || $this->timestamp === self::NO_TIMESTAMP) {
            return null;
        }

        return $this->timestamp;
    }

    /**
     * Tells whether the Value of this message is a compressed message set instead of a payload
     */
    public function isCompressed(): bool
    {
        return $this->getCompressionCodec() !== CompressionCodec::NONE;
    }

    /**
     * Returns the checksum of the current contents of the message, as an unsigned int32
     */
    public function computeCrc(): int
    {
        $scheme = static::getScheme();
        unset($scheme['crc']);

        $stream = new StringStream();
        foreach ($scheme as $fieldName => $schemeType) {
            BinarySchema::writeSingleType($schemeType, $this->$fieldName, $stream);
        }

        return crc32($stream->getBuffer());
    }

    /**
     * Recomputes the checksum after the contents of the message have been changed
     */
    public function updateCrc(): void
    {
        $this->crc = $this->computeCrc();
    }

    /**
     * Verifies the checksum of the message, the way the broker and the official clients do on every read
     *
     * @throws CorruptMessageException when the checksum does not match the contents of the message
     */
    public function validateCrc(): void
    {
        $expectedCrc = $this->computeCrc();
        if ($expectedCrc !== $this->crc) {
            throw new CorruptMessageException(['expectedCrc' => $expectedCrc, 'actualCrc' => $this->crc]);
        }
    }

    /**
     * Returns the payload of a compressed message: the serialized message set that its Value wraps
     *
     * @throws CorruptMessageException when the compressed stream can not be decoded
     */
    public function decompressValue(): string
    {
        return CompressionCodec::decompress($this->getCompressionCodec(), $this->value ?? '');
    }

    /**
     * Returns the size of the serialized message in bytes, the MessageSize field of a message set entry
     */
    public function sizeInBytes(): int
    {
        return BinarySchema::getObjectTypeSize($this);
    }

    /**
     * Serializes the message
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
}
