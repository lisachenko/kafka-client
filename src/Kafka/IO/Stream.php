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

/**
 * @author Alexander.Lisachenko
 * @date   26.07.2016
 */

namespace Protocol\Kafka\IO;

/**
 * Binary stream that the protocol schema engine reads from and writes to.
 *
 * The method set mirrors the one of the `main` branch. Variable-length integers come in two flavours here: the
 * **zigzag** varints of the record format of Kafka 0.11 ({@see self::readVarint()}, {@see self::readVarlong()}) and
 * the **unsigned** varints that Kafka 2.4 gave the protocol itself with the compact types and the tagged fields of
 * KIP-482 ({@see self::readUnsignedVarint()}).
 *
 * @see \Protocol\Kafka\Protocol\BinarySchema
 */
interface Stream
{
    /**
     * Writes arguments to the stream
     *
     * @param string $format       Format for packing arguments
     * @param mixed  ...$arguments List of arguments for packing
     *
     * @see pack() manual for format
     */
    public function write(string $format, ...$arguments): void;

    /**
     * Reads information from the stream, advancing the internal stream pointer
     *
     * @return array<string|int, mixed> List of unpacked arguments
     * @see unpack() manual for format
     */
    public function read(string $format): array;

    /**
     * Reads a non-nullable string from the stream: int16 length prefix followed by that many bytes
     */
    public function readString(): string;

    /**
     * Reads a byte array from the stream: int32 length prefix followed by that many bytes, -1 means null
     */
    public function readByteArray(): ?string;

    /**
     * Reads a raw (unsigned, not zigzag-decoded) varint of at most 5 bytes: 7 bits per byte, least significant
     * group first, the high bit set on every byte but the last (Kafka 0.11, record batch v2)
     */
    public function readVarint(): int;

    /**
     * Reads a raw varint of at most 10 bytes, the encoding of an int64
     */
    public function readVarlong(): int;

    /**
     * Reads an **unsigned varint** of at most 5 bytes: the length prefix of every compact type and the counter of
     * every tagged-field section of a flexible version (KIP-482, `ByteUtils.readUnsignedVarint` @ 2.8.2)
     *
     * The bytes are the ones {@see self::readVarint()} reads - 7 bits per byte, least significant group first, the
     * high bit set on every byte but the last - and the difference is what they mean: an unsigned varint carries
     * the value itself, where the varints of a record batch carry it zigzag-encoded. The two names are kept apart
     * because a caller that mixes them up produces bytes a broker silently drops the connection over.
     */
    public function readUnsignedVarint(): int;

    /**
     * Writes a non-nullable string to the stream: int16 length prefix followed by the content
     */
    public function writeString(string $string): void;

    /**
     * Writes a byte array to the stream: int32 length prefix followed by the content, null is written as -1
     */
    public function writeByteArray(?string $data): void;

    /**
     * Writes an unsigned value as a raw varint (the caller zigzag-encodes a signed one first)
     */
    public function writeVarint(int $value): void;

    /**
     * Writes an unsigned 64-bit value as a raw varint of up to 10 bytes
     */
    public function writeVarlong(int $value): void;

    /**
     * Writes an **unsigned varint** of at most 5 bytes (KIP-482, `ByteUtils.writeUnsignedVarint` @ 2.8.2)
     *
     * The counterpart of {@see self::readUnsignedVarint()}: the compact length of a string, byte array or array
     * (`length + 1`, `0` for `null`), the number of tagged fields of a structure, and the tag and the size of each
     * of them.
     */
    public function writeUnsignedVarint(int $value): void;

    /**
     * Writes the raw buffer into the stream as-is
     */
    public function writeBuffer(?string $buffer): void;

    /**
     * Checks whether we are actually connected to the server
     */
    public function isConnected(): bool;

    /**
     * Checks if the stream is empty
     */
    public function isEmpty(): bool;
}
