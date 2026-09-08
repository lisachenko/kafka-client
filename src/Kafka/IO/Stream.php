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
 * The method set mirrors the one of the `main` branch, minus the varint primitives: variable-length integers only
 * exist in the 0.11 record format and have no representation in the 0.8 protocol.
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
     * Writes a non-nullable string to the stream: int16 length prefix followed by the content
     */
    public function writeString(string $string): void;

    /**
     * Writes a byte array to the stream: int32 length prefix followed by the content, null is written as -1
     */
    public function writeByteArray(?string $data): void;

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
