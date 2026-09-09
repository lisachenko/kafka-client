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

namespace Protocol\Kafka\Common\Utils;

/**
 * Bit-level helpers of the record batch v2 of Kafka 0.11: the zigzag encoding of the varints and the CRC-32C.
 *
 * This is the `ByteUtils` of the pre-schema `main`, kept as the port of `org.apache.kafka.common.utils.ByteUtils`
 * (and `Crc32C`) @ 0.11.0.3, with three corrections: the 64-bit zigzag decoding shifts logically (the pre-schema
 * version answered 0 for `PHP_INT_MIN`), the size functions no longer compare against a mask that PHP parses as a
 * float, and the checksum is computed by the `crc32c` algorithm of ext/hash instead of a 256-entry table in PHP -
 * the same polynomial (Castagnoli, RFC 3720 section B.4), native speed, and immune to the JIT miscompilation of hot
 * byte loops that the pure-PHP LZ4 decoder suffers from in the sandbox.
 *
 * The varint bytes themselves are read and written by {@see \Protocol\Kafka\IO\Stream::readVarint()} and its
 * siblings; the scheme engine combines the two halves in its `TYPE_VARINT_ZIGZAG`, `TYPE_VARLONG_ZIGZAG` and
 * `TYPE_VARCHAR_ZIGZAG` types.
 */
final class ByteUtils
{
    /**
     * Zigzag-encodes a signed value into the unsigned one that goes into a varint: 0 → 0, -1 → 1, 1 → 2, -2 → 3 …
     *
     * @param int $base 32 for an int32 (`ByteUtils.writeVarint`), 64 for an int64 (`ByteUtils.writeVarlong`)
     */
    public static function encodeZigZag(int $value, int $base = 32): int
    {
        if ($base === 32) {
            return (($value << 1) ^ ($value >> 31)) & 0xFFFFFFFF;
        }

        return ($value << 1) ^ ($value >> 63);
    }

    /**
     * Reverses {@see encodeZigZag()}: the unsigned value read out of a varint becomes the signed one it stands for
     *
     * The shift is a logical one (`>>>` in Java), otherwise the sign bit of a 64-bit value would be smeared.
     */
    public static function decodeZigZag(int $value): int
    {
        return (($value >> 1) & PHP_INT_MAX) ^ -($value & 1);
    }

    /**
     * Returns the number of bytes a signed int32 occupies as a zigzag varint (1 to 5), `ByteUtils.sizeOfVarint`
     */
    public static function sizeOfVarint(int $value): int
    {
        return self::sizeOfUnsignedVarint(self::encodeZigZag($value, 32));
    }

    /**
     * Returns the number of bytes a signed int64 occupies as a zigzag varlong (1 to 10), `ByteUtils.sizeOfVarlong`
     */
    public static function sizeOfVarlong(int $value): int
    {
        return self::sizeOfUnsignedVarint(self::encodeZigZag($value, 64));
    }

    /**
     * Returns the number of bytes an unsigned value occupies as a raw varint: 7 bits per byte
     */
    public static function sizeOfUnsignedVarint(int $value): int
    {
        $bytes = 1;
        while (($value & ~0x7F) !== 0) {
            $bytes++;
            $value = ($value >> 7) & (PHP_INT_MAX >> 6);
        }

        return $bytes;
    }

    /**
     * Computes the CRC-32C (Castagnoli) checksum of the buffer, as an unsigned int32
     *
     * This is the checksum of a record batch v2, covering the batch from its `attributes` field to its end
     * (`DefaultRecordBatch.computeChecksum` @ 0.11.0.3); the message formats v0 and v1 use the plain CRC-32.
     */
    public static function crc32c(string $buffer): int
    {
        return (int) hexdec(hash('crc32c', $buffer));
    }
}
