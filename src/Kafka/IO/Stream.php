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
 * Binary stream that speaks the Kafka protocol primitive types.
 *
 * All numeric types are big-endian and signed, see "Protocol Primitive Types" in docs/protocol/0.8.2.md.
 */
interface Stream
{
    /**
     * Reads exactly $length raw bytes from the stream, advancing the internal pointer.
     *
     * @throws \Protocol\Kafka\Common\Errors\NetworkException If the requested amount of bytes is not available
     */
    public function readRaw(int $length): string;

    /**
     * Writes raw bytes to the stream as is.
     */
    public function writeRaw(string $data): void;

    /**
     * Reads a signed 8-bit integer (int8).
     */
    public function readInt8(): int;

    /**
     * Reads a signed big-endian 16-bit integer (int16).
     */
    public function readInt16(): int;

    /**
     * Reads a signed big-endian 32-bit integer (int32).
     */
    public function readInt32(): int;

    /**
     * Reads a signed big-endian 64-bit integer (int64).
     */
    public function readInt64(): int;

    /**
     * Writes a signed 8-bit integer (int8).
     */
    public function writeInt8(int $value): void;

    /**
     * Writes a signed big-endian 16-bit integer (int16).
     */
    public function writeInt16(int $value): void;

    /**
     * Writes a signed big-endian 32-bit integer (int32).
     */
    public function writeInt32(int $value): void;

    /**
     * Writes a signed big-endian 64-bit integer (int64).
     */
    public function writeInt64(int $value): void;

    /**
     * Reads a string: int16 length prefix followed by that many bytes, -1 means null.
     */
    public function readString(): ?string;

    /**
     * Writes a string: int16 length prefix followed by the content, null is written as -1.
     */
    public function writeString(?string $value): void;

    /**
     * Reads a byte array: int32 length prefix followed by that many bytes, -1 means null.
     */
    public function readBytes(): ?string;

    /**
     * Writes a byte array: int32 length prefix followed by the content, null is written as -1.
     */
    public function writeBytes(?string $data): void;

    /**
     * Reads an int32-prefixed array of elements, delegating each element to the given reader.
     *
     * @param callable(Stream): mixed $elementReader
     *
     * @return list<mixed>
     */
    public function readArray(callable $elementReader): array;

    /**
     * Writes an int32-prefixed array of elements, delegating each element to the given writer.
     *
     * @param iterable<mixed>               $items
     * @param callable(Stream, mixed): void $elementWriter
     */
    public function writeArray(iterable $items, callable $elementWriter): void;

    /**
     * Writes arguments to the stream
     *
     * @param string $format       Format for packing arguments
     * @param mixed  ...$arguments List of arguments for packing
     *
     * @see pack() manual for format
     *
     * @deprecated Use the typed primitives instead, this is kept for the not-yet-migrated protocol classes.
     */
    public function write(string $format, mixed ...$arguments): void;

    /**
     * Reads information from the stream, advancing the internal pointer
     *
     * @param string $format Format for unpacking arguments
     * @see unpack() manual for format
     *
     * @return array<string|int, mixed> List of unpacked arguments
     *
     * @deprecated Use the typed primitives instead, this is kept for the not-yet-migrated protocol classes.
     */
    public function read(string $format): array;

    /**
     * Reads a byte array from the stream
     *
     * @deprecated Use {@see Stream::readBytes()} instead.
     */
    public function readByteArray(): ?string;

    /**
     * Writes a byte array to the stream
     *
     * @deprecated Use {@see Stream::writeBytes()} instead.
     */
    public function writeByteArray(?string $data): void;
}
