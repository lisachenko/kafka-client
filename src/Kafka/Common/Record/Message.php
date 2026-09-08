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
 * A single message of the 0.8 wire format, message format v0.
 *
 * <pre>
 *   Message => Crc MagicByte Attributes Key Value
 *     Crc        => int32
 *     MagicByte  => int8
 *     Attributes => int8
 *     Key        => bytes
 *     Value      => bytes
 * </pre>
 *
 * A compressed message is a regular message whose Value is a complete, compressed {@see MessageSet}; the codec it
 * was compressed with is announced by the three lowest bits of the Attributes byte.
 *
 * @see docs/protocol/0.9.0.md, section "MessageSet and Message"
 * @see kafka/message/Message.scala @ 0.8.2.2
 */
class Message implements BinarySchemaInterface, \Stringable
{
    /**
     * The only magic byte that 0.8.2.2 knows: message format v0, without the Timestamp field of v1
     */
    public const int MAGIC_V0 = 0;

    /**
     * Size of a message with a null key and a null value: crc, magic, attributes and two length prefixes
     */
    public const int MIN_SIZE = 4 + 1 + 1 + 4 + 4;

    /**
     * CRC-32 (IEEE) of the message bytes that follow this field, i.e. of MagicByte, Attributes, Key and Value.
     *
     * The value is the unsigned one that {@see crc32()} returns; it is written and read as an int32.
     */
    public int $crc = 0;

    /**
     * Version id of the message binary format, always 0 in 0.8.2.2
     */
    public int $magicByte = self::MAGIC_V0;

    /**
     * Metadata about the message: bits 0-2 hold the compression codec, every other bit is 0 in message format v0
     */
    public int $attributes = 0;

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
     * @param int         $attributes Attributes byte, carrying the compression codec in its three lowest bits
     */
    public function __construct(?string $value = null, ?string $key = null, int $attributes = 0)
    {
        $this->value      = $value;
        $this->key        = $key;
        $this->attributes = $attributes;
        $this->crc        = $this->computeCrc();
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'crc'        => BinarySchema::TYPE_INT32,
            'magicByte'  => BinarySchema::TYPE_INT8,
            'attributes' => BinarySchema::TYPE_INT8,
            'key'        => BinarySchema::TYPE_BYTEARRAY,
            'value'      => BinarySchema::TYPE_BYTEARRAY,
        ];
    }

    /**
     * Creates the wrapper message of a compressed message set
     *
     * @param string $messageSetBuffer Serialized inner message set, compressed by this method
     * @param int    $compressionCodec One of the {@see CompressionCodec} constants, other than NONE
     */
    public static function compressed(string $messageSetBuffer, int $compressionCodec): static
    {
        return new static(CompressionCodec::compress($compressionCodec, $messageSetBuffer), null, $compressionCodec);
    }

    /**
     * Reads a message from a stream that is positioned at its Crc field
     *
     * @param bool $checkCrcs Whether to validate the checksum of the message, the `check.crcs` behaviour of the
     *                        official clients
     *
     * @throws CorruptMessageException when the checksum does not match the contents of the message
     */
    public static function unpackFrom(Stream $stream, bool $checkCrcs = true): static
    {
        /** @var static $message */
        $message = BinarySchema::readObjectFromStream(static::class, $stream);
        // The engine reads an int32 as a signed value, while a checksum is the unsigned number crc32() returns
        $message->crc &= 0xFFFFFFFF;
        if ($checkCrcs) {
            $message->validateCrc();
        }

        return $message;
    }

    /**
     * Reads a message from a buffer that holds exactly its bytes
     *
     * @throws CorruptMessageException when the checksum does not match the contents of the message
     */
    public static function fromBuffer(string $buffer, bool $checkCrcs = true): static
    {
        return static::unpackFrom(new StringStream($buffer), $checkCrcs);
    }

    /**
     * Returns the compression codec that this message announces in its Attributes byte
     */
    public function getCompressionCodec(): int
    {
        return CompressionCodec::fromAttributes($this->attributes);
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
        $scheme = self::getScheme();
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
