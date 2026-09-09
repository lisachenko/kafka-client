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

/**
 * Snappy codec in the framing that Kafka speaks, with a pure-PHP fallback for the raw blocks.
 *
 * Kafka does not use the official snappy "framing format": the brokers and every client of the 0.8 era use the
 * blocking format of the xerial snappy-java library, which is what a message with `Attributes & 0x07 == 2` carries:
 *
 * <pre>
 *   +--------------+---------+--------+------------+--------------+-----+
 *   | Magic (8)    | Version | Compat | Block1 len | Block1 data  | ... |
 *   | 82 SNAPPY 00 |  int32  | int32  |   int32    | snappy bytes |     |
 *   +--------------+---------+--------+------------+--------------+-----+
 * </pre>
 *
 * The block length is the size of the *compressed* block; the amount of uncompressed data handed to snappy per block
 * is at most {@see Snappy::BLOCK_SIZE} (32 KiB), the default of the xerial library.
 *
 * Some producers write a single raw snappy block with no header at all, so decoding accepts both shapes.
 *
 * The raw blocks themselves are encoded and decoded by the `snappy` PHP extension when it is loaded, and by the
 * pure-PHP implementation of the snappy format below otherwise.
 *
 * @see https://github.com/google/snappy/blob/main/format_description.txt
 * @see https://github.com/xerial/snappy-java
 */
final class Snappy
{
    /**
     * Header that the xerial blocking format puts in front of the first block: magic, version 1, compatibility 1
     */
    public const string XERIAL_HEADER = "\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01";

    /**
     * Amount of uncompressed data that goes into a single block of the xerial format
     */
    public const int BLOCK_SIZE = 32768;

    /**
     * Element types of the snappy format, held in the two lowest bits of a tag byte
     */
    private const int TAG_LITERAL     = 0;
    private const int TAG_COPY_1_BYTE = 1;
    private const int TAG_COPY_2_BYTE = 2;
    private const int TAG_COPY_4_BYTE = 3;

    /**
     * The back reference of a copy element is at most 2 bytes wide in the elements that the encoder emits
     */
    private const int MAX_COPY_OFFSET = 65535;

    /**
     * The longest match that a single copy element can express
     */
    private const int MAX_COPY_LENGTH = 64;

    /**
     * This class is a namespace for the codec functions and is never instantiated
     */
    private function __construct() {}

    /**
     * Compresses the data into the xerial blocking format that Kafka expects in a snappy message
     */
    public static function compress(string $data): string
    {
        $result     = self::XERIAL_HEADER;
        $dataLength = strlen($data);

        for ($position = 0; $position < $dataLength; $position += self::BLOCK_SIZE) {
            $block = self::compressBlock(substr($data, $position, self::BLOCK_SIZE));
            $result .= pack('N', strlen($block)) . $block;
        }

        return $result;
    }

    /**
     * Decompresses a snappy payload, accepting both the xerial blocking format and a single raw block
     *
     * @throws CorruptMessageException when the stream is truncated or malformed
     */
    public static function decompress(string $data): string
    {
        if (!self::isXerialFramed($data)) {
            return self::decompressBlock($data);
        }

        $result     = '';
        $dataLength = strlen($data);
        $position   = strlen(self::XERIAL_HEADER);

        while ($position < $dataLength) {
            if ($position + 4 > $dataLength) {
                throw new CorruptMessageException(['error' => 'Truncated block length in the snappy stream']);
            }
            $blockLength = (int) unpack('Nlength', $data, $position)['length'];
            $position += 4;
            if ($blockLength < 0 || $position + $blockLength > $dataLength) {
                throw new CorruptMessageException(['error' => 'Truncated block in the snappy stream']);
            }
            $result .= self::decompressBlock(substr($data, $position, $blockLength));
            $position += $blockLength;
        }

        return $result;
    }

    /**
     * Tells whether the payload starts with the header of the xerial blocking format
     */
    public static function isXerialFramed(string $data): bool
    {
        $headerLength = strlen(self::XERIAL_HEADER);

        return strlen($data) >= $headerLength && substr($data, 0, $headerLength) === self::XERIAL_HEADER;
    }

    /**
     * Compresses a single raw snappy block, without any framing
     */
    public static function compressBlock(string $data): string
    {
        if (extension_loaded('snappy')) {
            /** @var callable(string): (string|false) $compressor */
            $compressor = 'snappy_compress';
            $compressed = $compressor($data);
            if (is_string($compressed)) {
                return $compressed;
            }
        }

        return self::compressBlockInPhp($data);
    }

    /**
     * Decompresses a single raw snappy block, without any framing
     *
     * @throws CorruptMessageException when the block is truncated or malformed
     */
    public static function decompressBlock(string $data): string
    {
        if (extension_loaded('snappy')) {
            /** @var callable(string): (string|false) $decompressor */
            $decompressor = 'snappy_uncompress';
            $decompressed = $decompressor($data);
            if (is_string($decompressed)) {
                return $decompressed;
            }

            throw new CorruptMessageException(['error' => 'Unable to decompress the snappy block']);
        }

        return self::decompressBlockInPhp($data);
    }

    /**
     * Pure-PHP snappy encoder.
     *
     * The block is scanned with a hash table of the four-byte sequences seen so far; a sequence that was seen before
     * within the back-reference window becomes a copy element, everything else accumulates into a literal element.
     * The output is a valid snappy block that any decoder, the broker included, accepts.
     */
    private static function compressBlockInPhp(string $data): string
    {
        $dataLength = strlen($data);
        $result     = self::putVarint($dataLength);
        if ($dataLength === 0) {
            return $result;
        }

        /** @var array<string, int> $candidates position of the last occurrence of every four-byte sequence */
        $candidates    = [];
        $position      = 0;
        $literalStart  = 0;
        $lastHashable  = $dataLength - 4;

        while ($position <= $lastHashable) {
            $sequence  = substr($data, $position, 4);
            $candidate = $candidates[$sequence] ?? null;
            $candidates[$sequence] = $position;

            if ($candidate === null || $position - $candidate > self::MAX_COPY_OFFSET) {
                $position++;
                continue;
            }

            if ($position > $literalStart) {
                $result .= self::emitLiteral(substr($data, $literalStart, $position - $literalStart));
            }

            $matchLength    = 4;
            $maxMatchLength = min(self::MAX_COPY_LENGTH, $dataLength - $position);
            while ($matchLength < $maxMatchLength && $data[$candidate + $matchLength] === $data[$position + $matchLength]) {
                $matchLength++;
            }
            $result .= self::emitCopy($position - $candidate, $matchLength);

            // Every position covered by the match is still worth indexing for the sequences that follow it
            $matchEnd = $position + $matchLength;
            for ($inner = $position + 1; $inner < $matchEnd && $inner <= $lastHashable; $inner++) {
                $candidates[substr($data, $inner, 4)] = $inner;
            }

            $position     = $matchEnd;
            $literalStart = $matchEnd;
        }

        if ($literalStart < $dataLength) {
            $result .= self::emitLiteral(substr($data, $literalStart));
        }

        return $result;
    }

    /**
     * Pure-PHP snappy decoder
     *
     * @throws CorruptMessageException when the block is truncated or malformed
     */
    private static function decompressBlockInPhp(string $data): string
    {
        $dataLength = strlen($data);
        $position   = 0;
        $expected   = self::readVarint($data, $position);
        $result     = '';

        while ($position < $dataLength) {
            $tag = ord($data[$position++]);
            if (($tag & 0x03) === self::TAG_LITERAL) {
                $literalLength = $tag >> 2;
                if ($literalLength >= 60) {
                    $extraBytes    = $literalLength - 59;
                    $literalLength = self::readLittleEndian($data, $position, $extraBytes);
                    $position += $extraBytes;
                }
                $literalLength++;
                if ($position + $literalLength > $dataLength) {
                    throw new CorruptMessageException(['error' => 'Truncated literal in the snappy block']);
                }
                $result .= substr($data, $position, $literalLength);
                $position += $literalLength;

                continue;
            }

            [$copyLength, $offset] = self::readCopy($data, $position, $tag);

            $resultLength = strlen($result);
            if ($offset <= 0 || $offset > $resultLength) {
                throw new CorruptMessageException(['error' => 'Invalid back reference in the snappy block']);
            }
            if ($offset >= $copyLength) {
                $result .= substr($result, $resultLength - $offset, $copyLength);

                continue;
            }
            // An overlapping copy repeats the tail of the output and has to be expanded byte by byte
            $copyStart = $resultLength - $offset;
            for ($copied = 0; $copied < $copyLength; $copied++) {
                $result .= $result[$copyStart + $copied];
            }
        }

        if (strlen($result) !== $expected) {
            throw new CorruptMessageException([
                'error'    => 'The snappy block does not hold the amount of data it announced',
                'expected' => $expected,
                'actual'   => strlen($result),
            ]);
        }

        return $result;
    }

    /**
     * Reads the length and the back reference of a copy element, advancing the position past its operand
     *
     * @param int $position Position right after the tag byte, advanced by this call
     *
     * @return array{0: int, 1: int} Length of the match and the distance back to it
     */
    private static function readCopy(string $data, int &$position, int $tag): array
    {
        $type = $tag & 0x03;
        if ($type === self::TAG_COPY_1_BYTE) {
            $copyLength = 4 + (($tag >> 2) & 0x07);
            $offset     = (($tag >> 5) << 8) | self::readLittleEndian($data, $position, 1);
            $position += 1;

            return [$copyLength, $offset];
        }

        $operandSize = $type === self::TAG_COPY_2_BYTE ? 2 : 4;
        $copyLength  = ($tag >> 2) + 1;
        $offset      = self::readLittleEndian($data, $position, $operandSize);
        $position += $operandSize;

        return [$copyLength, $offset];
    }

    /**
     * Encodes a literal element: its length as a 6-bit or extended value, followed by the bytes themselves
     */
    private static function emitLiteral(string $literal): string
    {
        $length = strlen($literal) - 1;
        if ($length < 60) {
            return chr($length << 2) . $literal;
        }
        if ($length < 0x100) {
            return chr(60 << 2) . chr($length) . $literal;
        }
        if ($length < 0x10000) {
            return chr(61 << 2) . pack('v', $length) . $literal;
        }
        if ($length < 0x1000000) {
            return chr(62 << 2) . substr(pack('V', $length), 0, 3) . $literal;
        }

        return chr(63 << 2) . pack('V', $length) . $literal;
    }

    /**
     * Encodes a copy element, preferring the compact one-byte-offset form when the match fits into it
     */
    private static function emitCopy(int $offset, int $length): string
    {
        if ($length <= 11 && $offset <= 0x7FF) {
            return chr((($offset >> 8) << 5) | (($length - 4) << 2) | self::TAG_COPY_1_BYTE) . chr($offset & 0xFF);
        }

        return chr((($length - 1) << 2) | self::TAG_COPY_2_BYTE) . pack('v', $offset);
    }

    /**
     * Encodes an integer as a little-endian base-128 varint, the preamble of every snappy block
     */
    private static function putVarint(int $value): string
    {
        $result = '';
        while ($value >= 0x80) {
            $result .= chr(($value & 0x7F) | 0x80);
            $value >>= 7;
        }

        return $result . chr($value);
    }

    /**
     * Reads the uncompressed length out of the preamble of a snappy block
     *
     * @param int $position Position to read from, advanced by this call
     *
     * @throws CorruptMessageException when the varint is truncated or too long to be a length
     */
    private static function readVarint(string $data, int &$position): int
    {
        $dataLength = strlen($data);
        $result     = 0;
        $shift      = 0;

        while (true) {
            if ($position >= $dataLength) {
                throw new CorruptMessageException(['error' => 'Truncated length preamble in the snappy block']);
            }
            $byte = ord($data[$position++]);
            $result |= ($byte & 0x7F) << $shift;
            if (($byte & 0x80) === 0) {
                return $result;
            }
            $shift += 7;
            if ($shift > 28) {
                throw new CorruptMessageException(['error' => 'Malformed length preamble in the snappy block']);
            }
        }
    }

    /**
     * Reads an unsigned little-endian integer of the given width
     *
     * @throws CorruptMessageException when the block ends in the middle of the value
     */
    private static function readLittleEndian(string $data, int $position, int $size): int
    {
        if ($position + $size > strlen($data)) {
            throw new CorruptMessageException(['error' => 'Truncated element in the snappy block']);
        }

        $result = 0;
        for ($index = 0; $index < $size; $index++) {
            $result |= ord($data[$position + $index]) << (8 * $index);
        }

        return $result;
    }
}
