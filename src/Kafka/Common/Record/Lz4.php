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
 * LZ4 codec (`Attributes & 0x07 == 3`) in the framing that Kafka speaks, implemented in pure PHP.
 *
 * A message with the LZ4 codec carries the message set it wraps as an **LZ4 frame** (the v1.5.1 frame format), as
 * `KafkaLZ4BlockOutputStream` writes it:
 *
 * <pre>
 *   +-------------+-----+----+----+---------------+---------------+-----+----------+
 *   | Magic       | FLG | BD | HC | Block1 length | Block1 data   | ... | EndMark  |
 *   | 04 22 4d 18 |  60 | 40 | xx |  int32 LE     | lz4 block     |     | 00000000 |
 *   +-------------+-----+----+----+---------------+---------------+-----+----------+
 * </pre>
 *
 *  * `FLG` = version 1, independent blocks, no block checksum, no content size, no content checksum (`0x60`);
 *  * `BD` = the block maximum size, 4 = 64 KiB (`0x40`), which is what the Java client writes;
 *  * `HC` is the second byte of the xxHash32 of the frame descriptor, i.e. `(xxh32(FLG BD) >> 8) & 0xff`;
 *  * the highest bit of a block length marks a block that is stored **uncompressed** because compressing it made it
 *    bigger, and the length of `0` is the end mark of the frame.
 *
 * **The KAFKA-3160 quirk.** Until Kafka 0.10.0 the client computed the `HC` byte over the magic number *and* the
 * frame descriptor instead of over the frame descriptor alone. The fix could not be applied to the messages that
 * old clients still have to read, so the broken checksum became part of the format of **message format v0**: a v0
 * message carries the broken `HC`, a v1 message the correct one (`useBrokenFlagDescriptorChecksum = magic == 0` in
 * `MemoryRecordsBuilder.wrapForOutput()`). This reader accepts either of them, whatever the magic of the message is,
 * which is what makes a set that a 0.8 or 0.9 producer wrote readable.
 *
 * The LZ4 block format itself - sequences of a token, literals, a two-byte back reference and a match length - is
 * implemented below: `lz4` is not a PHP extension that can be relied upon, and the broker only ever sees the
 * compressed bytes, so a correct (rather than a fast) encoder is enough.
 *
 * @see https://github.com/lz4/lz4/blob/dev/doc/lz4_Frame_format.md
 * @see https://github.com/lz4/lz4/blob/dev/doc/lz4_Block_format.md
 * @see org/apache/kafka/common/record/KafkaLZ4Block{Input,Output}Stream.java @ 0.10.2.2
 * @see https://issues.apache.org/jira/browse/KAFKA-3160
 */
final class Lz4
{
    /**
     * Magic number of an LZ4 frame, 0x184D2204 written as a little-endian int32
     */
    public const string MAGIC = "\x04\x22\x4d\x18";

    /**
     * FLG byte of the frames that Kafka writes: version 1, independent blocks, no checksums, no content size
     */
    public const int FLG = 0x60;

    /**
     * BD byte of the frames that Kafka writes: block maximum size 4, i.e. 64 KiB
     */
    public const int BD = 0x40;

    /**
     * Value of the block size field that stands for a block maximum size of 64 KiB
     */
    public const int BLOCK_SIZE_64KB = 4;

    /**
     * Highest bit of a block length, set when the block is stored uncompressed
     */
    public const int INCOMPRESSIBLE_MASK = 0x80000000;

    /**
     * The shortest match that the block format can express
     */
    private const int MIN_MATCH = 4;

    /**
     * The last five bytes of a block are always literals
     */
    private const int LAST_LITERALS = 5;

    /**
     * No match may start in the last twelve bytes of a block
     */
    private const int MATCH_FIND_LIMIT = 12;

    /**
     * The back reference of a match is a two-byte value, so a match never reaches further back than this
     */
    private const int MAX_DISTANCE = 0xFFFF;

    /**
     * Value of a token nibble that announces an extended length
     */
    private const int EXTENDED_LENGTH = 15;

    /**
     * This class is a namespace for the codec functions and is never instantiated
     */
    private function __construct() {}

    /**
     * Compresses a message set into the LZ4 frame that a wrapper message of the given magic carries
     *
     * @param string $data  Serialized message set to compress
     * @param int    $magic Magic byte of the wrapper message, which decides whether the frame descriptor carries the
     *                      broken checksum of KAFKA-3160 ({@see Message::MAGIC_V0}) or the correct one
     */
    public static function compress(string $data, int $magic = Message::MAGIC_V1): string
    {
        $frame          = self::frameDescriptor($magic === Message::MAGIC_V0);
        $blockMaximum   = self::blockMaximumSize(self::BLOCK_SIZE_64KB);
        $dataLength     = strlen($data);

        for ($position = 0; $position < $dataLength; $position += $blockMaximum) {
            $block      = substr($data, $position, $blockMaximum);
            $compressed = self::compressBlock($block);
            // A block that grew is stored as it is, with the highest bit of its length set
            if (strlen($compressed) >= strlen($block)) {
                $frame .= pack('V', strlen($block) | self::INCOMPRESSIBLE_MASK) . $block;

                continue;
            }
            $frame .= pack('V', strlen($compressed)) . $compressed;
        }

        return $frame . pack('V', 0);
    }

    /**
     * Decompresses the LZ4 frame that a wrapper message carries, accepting both frame descriptor checksums
     *
     * @throws CorruptMessageException when the frame is truncated, malformed or not an LZ4 frame at all
     */
    public static function decompress(string $data): string
    {
        $dataLength = strlen($data);
        if ($dataLength < 7 || !str_starts_with($data, self::MAGIC)) {
            throw new CorruptMessageException(['error' => 'The payload does not start with an LZ4 frame magic']);
        }

        $flg = ord($data[4]);
        $bd  = ord($data[5]);
        self::validateFrameDescriptor($flg, $bd);

        $descriptorLength = 2;
        // An optional content size sits between the BD byte and the header checksum
        if (($flg & 0x08) !== 0) {
            $descriptorLength += 8;
        }
        $descriptor = substr($data, 4, $descriptorLength);
        if (strlen($descriptor) !== $descriptorLength || $dataLength < 4 + $descriptorLength + 1) {
            throw new CorruptMessageException(['error' => 'The LZ4 frame descriptor is truncated']);
        }
        self::validateDescriptorChecksum($descriptor, ord($data[4 + $descriptorLength]));

        $blockMaximum = self::blockMaximumSize(($bd >> 4) & 0x07);
        $position     = 4 + $descriptorLength + 1;
        $result       = '';

        while (true) {
            if ($position + 4 > $dataLength) {
                throw new CorruptMessageException(['error' => 'The LZ4 frame ended before its end mark']);
            }
            $blockLength = (int) unpack('Vlength', $data, $position)['length'];
            $position += 4;

            $isCompressed = ($blockLength & self::INCOMPRESSIBLE_MASK) === 0;
            $blockLength &= ~self::INCOMPRESSIBLE_MASK;

            if ($blockLength === 0) {
                // End mark of the frame, optionally followed by the checksum of the content
                return $result;
            }
            if ($blockLength > $blockMaximum || $position + $blockLength > $dataLength) {
                throw new CorruptMessageException([
                    'error'       => 'An LZ4 block announces more bytes than the frame holds',
                    'blockLength' => $blockLength,
                ]);
            }

            $block = substr($data, $position, $blockLength);
            $position += $blockLength;
            // The block checksum, when the frame carries one, follows the block itself
            if (($flg & 0x10) !== 0) {
                $position += 4;
            }

            $result .= $isCompressed ? self::decompressBlock($block) : $block;
        }
    }

    /**
     * Compresses a single LZ4 block, without any framing.
     *
     * The block is scanned with a table of the four-byte sequences seen so far, exactly like {@see Snappy}: a
     * sequence that was seen before within the back-reference window becomes a match, everything else accumulates
     * into the literals of the next sequence. The two rules that every LZ4 block has to obey are respected: the last
     * five bytes of a block are literals, and no match starts in its last twelve bytes.
     */
    public static function compressBlock(string $data): string
    {
        $dataLength = strlen($data);
        if ($dataLength <= self::MATCH_FIND_LIMIT) {
            // Too short for any match: the whole block is a single sequence of literals
            return self::emitLastLiterals($data);
        }

        /** @var array<string, int> $candidates position of the last occurrence of every four-byte sequence */
        $candidates = [];
        $result     = '';
        $position   = 0;
        $anchor     = 0;
        $findLimit  = $dataLength - self::MATCH_FIND_LIMIT;
        $matchLimit = $dataLength - self::LAST_LITERALS;

        while ($position < $findLimit) {
            $sequence              = substr($data, $position, self::MIN_MATCH);
            $candidate             = $candidates[$sequence] ?? null;
            $candidates[$sequence] = $position;

            if ($candidate === null || $position - $candidate > self::MAX_DISTANCE) {
                $position++;

                continue;
            }

            $matchLength = self::MIN_MATCH;
            while (
                $position + $matchLength < $matchLimit
                && $data[$candidate + $matchLength] === $data[$position + $matchLength]
            ) {
                $matchLength++;
            }

            $result .= self::emitSequence(
                substr($data, $anchor, $position - $anchor),
                $position - $candidate,
                $matchLength
            );

            // Every position the match covers is still worth indexing for the sequences that follow it
            $matchEnd = $position + $matchLength;
            for ($inner = $position + 1; $inner < $matchEnd && $inner < $findLimit; $inner++) {
                $candidates[substr($data, $inner, self::MIN_MATCH)] = $inner;
            }

            $position = $matchEnd;
            $anchor   = $matchEnd;
        }

        return $result . self::emitLastLiterals(substr($data, $anchor));
    }

    /**
     * Decompresses a single LZ4 block, without any framing
     *
     * @throws CorruptMessageException when the block is truncated or malformed
     */
    public static function decompressBlock(string $data): string
    {
        $dataLength = strlen($data);
        $position   = 0;
        $result     = '';

        while ($position < $dataLength) {
            $token         = ord($data[$position++]);
            $literalLength = $token >> 4;
            if ($literalLength === self::EXTENDED_LENGTH) {
                $literalLength += self::readExtendedLength($data, $position);
            }
            if ($position + $literalLength > $dataLength) {
                throw new CorruptMessageException(['error' => 'Truncated literals in the LZ4 block']);
            }
            $result .= substr($data, $position, $literalLength);
            $position += $literalLength;

            // The last sequence of a block holds literals only and ends the block right here
            if ($position === $dataLength) {
                return $result;
            }
            if ($position + 2 > $dataLength) {
                throw new CorruptMessageException(['error' => 'Truncated back reference in the LZ4 block']);
            }

            $offset = (int) unpack('voffset', $data, $position)['offset'];
            $position += 2;

            $matchLength = $token & 0x0F;
            if ($matchLength === self::EXTENDED_LENGTH) {
                $matchLength += self::readExtendedLength($data, $position);
            }
            $matchLength += self::MIN_MATCH;

            $resultLength = strlen($result);
            if ($offset <= 0 || $offset > $resultLength) {
                throw new CorruptMessageException(['error' => 'Invalid back reference in the LZ4 block']);
            }
            if ($offset >= $matchLength) {
                $result .= substr($result, $resultLength - $offset, $matchLength);

                continue;
            }
            // An overlapping match repeats the tail of the output and has to be expanded byte by byte
            $matchStart = $resultLength - $offset;
            for ($copied = 0; $copied < $matchLength; $copied++) {
                $result .= $result[$matchStart + $copied];
            }
        }

        return $result;
    }

    /**
     * Returns the magic number, the frame descriptor and its checksum, the header of every Kafka LZ4 frame
     *
     * @param bool $useBrokenFlagDescriptorChecksum Whether to compute the checksum over the magic number as well,
     *                                              the way the clients before Kafka 0.10.0 did (KAFKA-3160)
     */
    public static function frameDescriptor(bool $useBrokenFlagDescriptorChecksum = false): string
    {
        $descriptor = chr(self::FLG) . chr(self::BD);
        $checksum   = self::descriptorChecksum($descriptor, $useBrokenFlagDescriptorChecksum);

        return self::MAGIC . $descriptor . chr($checksum);
    }

    /**
     * Returns the HC byte of a frame descriptor: the second byte of its xxHash32
     *
     * @param string $descriptor                     Frame descriptor, i.e. the FLG and BD bytes and an optional
     *                                               content size
     * @param bool   $useBrokenFlagDescriptorChecksum Whether the magic number is part of the hashed bytes, which is
     *                                               the KAFKA-3160 behaviour of the clients before Kafka 0.10.0
     */
    public static function descriptorChecksum(string $descriptor, bool $useBrokenFlagDescriptorChecksum): int
    {
        $hashed = $useBrokenFlagDescriptorChecksum ? self::MAGIC . $descriptor : $descriptor;
        $hash   = (int) hexdec(hash('xxh32', $hashed));

        return ($hash >> 8) & 0xFF;
    }

    /**
     * Returns the maximum size of a block for the given value of the BD field
     */
    public static function blockMaximumSize(int $blockSizeValue): int
    {
        if ($blockSizeValue < 4 || $blockSizeValue > 7) {
            throw new CorruptMessageException([
                'error'          => 'The LZ4 frame announces a block size outside of the allowed range',
                'blockSizeValue' => $blockSizeValue,
            ]);
        }

        return 1 << (($blockSizeValue * 2) + 8);
    }

    /**
     * Rejects a frame whose FLG or BD byte says something this codec can not read
     *
     * @throws CorruptMessageException for a frame of another version or with dependent blocks
     */
    private static function validateFrameDescriptor(int $flg, int $bd): void
    {
        if (($flg >> 6) !== 1) {
            throw new CorruptMessageException(['error' => 'Unsupported version of the LZ4 frame format']);
        }
        if ((($flg >> 5) & 1) !== 1) {
            throw new CorruptMessageException(['error' => 'An LZ4 frame with dependent blocks can not be read']);
        }
        if (($flg & 0x03) !== 0 || ($bd & 0x8F) !== 0) {
            throw new CorruptMessageException(['error' => 'The reserved bits of the LZ4 frame descriptor are not 0']);
        }
    }

    /**
     * Accepts the checksum of a frame descriptor, in its correct and in its KAFKA-3160 form
     *
     * @throws CorruptMessageException when the byte matches neither of the two
     */
    private static function validateDescriptorChecksum(string $descriptor, int $checksum): void
    {
        if ($checksum === self::descriptorChecksum($descriptor, false)) {
            return;
        }
        // Every message of format v0 that a Kafka client wrote carries the broken checksum of KAFKA-3160
        if ($checksum === self::descriptorChecksum($descriptor, true)) {
            return;
        }

        throw new CorruptMessageException(['error' => 'The checksum of the LZ4 frame descriptor does not match']);
    }

    /**
     * Encodes one sequence of the block format: a token, its literals, the back reference and the match length
     */
    private static function emitSequence(string $literals, int $offset, int $matchLength): string
    {
        $literalLength = strlen($literals);
        $matchCode     = $matchLength - self::MIN_MATCH;

        $token = (min($literalLength, self::EXTENDED_LENGTH) << 4) | min($matchCode, self::EXTENDED_LENGTH);

        $result = chr($token);
        if ($literalLength >= self::EXTENDED_LENGTH) {
            $result .= self::encodeExtendedLength($literalLength - self::EXTENDED_LENGTH);
        }
        $result .= $literals . pack('v', $offset);
        if ($matchCode >= self::EXTENDED_LENGTH) {
            $result .= self::encodeExtendedLength($matchCode - self::EXTENDED_LENGTH);
        }

        return $result;
    }

    /**
     * Encodes the last sequence of a block, which carries literals and no match at all
     */
    private static function emitLastLiterals(string $literals): string
    {
        $literalLength = strlen($literals);
        $result        = chr(min($literalLength, self::EXTENDED_LENGTH) << 4);
        if ($literalLength >= self::EXTENDED_LENGTH) {
            $result .= self::encodeExtendedLength($literalLength - self::EXTENDED_LENGTH);
        }

        return $result . $literals;
    }

    /**
     * Encodes the part of a length that does not fit into the four bits of a token: 255 for every full byte
     */
    private static function encodeExtendedLength(int $length): string
    {
        $result = '';
        while ($length >= 0xFF) {
            $result .= "\xFF";
            $length -= 0xFF;
        }

        return $result . chr($length);
    }

    /**
     * Reads the extended part of a length, advancing the position past the bytes it is made of
     *
     * @param int $position Position of the first length byte, advanced by this call
     *
     * @throws CorruptMessageException when the block ends inside the length
     */
    private static function readExtendedLength(string $data, int &$position): int
    {
        $dataLength = strlen($data);
        $length     = 0;

        do {
            if ($position >= $dataLength) {
                throw new CorruptMessageException(['error' => 'Truncated length in the LZ4 block']);
            }
            $byte = ord($data[$position++]);
            $length += $byte;
        } while ($byte === 0xFF);

        return $length;
    }
}
