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

use Protocol\Kafka\Common\Errors\NetworkException;

/**
 * Common implementation of the Kafka protocol primitive types on top of pack()/unpack().
 *
 * The varint primitives share one byte loop: {@see self::readVarint()} and {@see self::readUnsignedVarint()} read
 * the same groups of 7 bits, and the zigzag step that turns them into a signed value belongs to
 * {@see \Protocol\Kafka\Common\Utils\ByteUtils}, not here. `readUnsignedVarint()` therefore stops after five
 * bytes exactly as `readVarint()` does (`ByteUtils.readUnsignedVarint` @ 2.8.2 refuses a sixth one too), which is
 * the width of every compact length of KIP-482.
 *
 * A concrete stream only has to implement {@see AbstractStream::read()}, {@see AbstractStream::write()},
 * {@see Stream::isConnected()} and {@see Stream::isEmpty()}.
 */
abstract class AbstractStream implements Stream
{
    /**
     * Length prefix that encodes a null string (int16 -1)
     */
    public const string NULL_STRING = "\xFF\xFF";

    /**
     * Length prefix that encodes a null byte array (int32 -1)
     */
    public const string NULL_BYTES = "\xFF\xFF\xFF\xFF";

    public function readString(): string
    {
        $stringLength = $this->readInt16();
        if ($stringLength < 0) {
            throw new \UnexpectedValueException('Received -1 length for not nullable string');
        }

        return $this->readRaw($stringLength);
    }

    public function writeString(string $string): void
    {
        $this->writeInt16(strlen($string));
        $this->writeBuffer($string);
    }

    /**
     * Reads a byte array, honouring the -1 length that the specification defines as null.
     *
     * Unlike the implementation on the `main` branch, a null byte array is a valid value here: the "bytes" primitive
     * of the specification is nullable, and the Key and Value of a Message rely on it.
     */
    public function readByteArray(): ?string
    {
        $dataLength = $this->readInt32();
        if ($dataLength < 0) {
            return null;
        }

        return $this->readRaw($dataLength);
    }

    public function writeByteArray(?string $data): void
    {
        if ($data === null) {
            $this->writeInt32(-1);

            return;
        }

        $this->writeInt32(strlen($data));
        $this->writeBuffer($data);
    }

    public function readVarint(): int
    {
        return $this->readRawVarint(28);
    }

    public function readVarlong(): int
    {
        return $this->readRawVarint(63);
    }

    public function readUnsignedVarint(): int
    {
        return $this->readRawVarint(28);
    }

    public function writeVarint(int $value): void
    {
        $this->writeRawVarint($value);
    }

    public function writeUnsignedVarint(int $value): void
    {
        $this->writeRawVarint($value);
    }

    public function writeVarlong(int $value): void
    {
        $this->writeRawVarint($value);
    }

    public function writeBuffer(?string $buffer): void
    {
        if ($buffer === null || $buffer === '') {
            return;
        }

        $this->write('a' . strlen($buffer), $buffer);
    }

    /**
     * Reads the groups of 7 bits of a varint until the byte without the continuation bit, refusing one that runs
     * past the size of its type (`ByteUtils.readVarint`/`readVarlong` @ 0.11.0.3 throw an IllegalArgumentException)
     *
     * @param int $maxShift 28 for a varint (5 bytes), 63 for a varlong (10 bytes)
     */
    private function readRawVarint(int $maxShift): int
    {
        $value = 0;
        $shift = 0;
        while ((($byte = $this->read('Cbyte')['byte']) & 0x80) !== 0) {
            $value |= ($byte & 0x7F) << $shift;
            $shift += 7;
            if ($shift > $maxShift) {
                throw new NetworkException(['error' => 'A varint of the stream is longer than its type allows']);
            }
        }

        return $value | ($byte << $shift);
    }

    /**
     * Writes an unsigned value 7 bits per byte, least significant group first, the high bit set on every byte but
     * the last (`ByteUtils.writeVarint`/`writeVarlong` @ 0.11.0.3 after their zigzag step)
     */
    private function writeRawVarint(int $value): void
    {
        while (($value & ~0x7F) !== 0) {
            $this->write('C', ($value & 0x7F) | 0x80);
            $value = ($value >> 7) & (PHP_INT_MAX >> 6);
        }
        $this->write('C', $value);
    }

    /**
     * Reads exactly the given amount of raw bytes from the stream
     */
    public function readRaw(int $length): string
    {
        if ($length < 0) {
            throw new \InvalidArgumentException("Length should not be negative, {$length} given");
        }
        if ($length === 0) {
            return '';
        }

        return (string) $this->read("a{$length}data")['data'];
    }

    /**
     * Reads a signed 8-bit integer (int8)
     */
    public function readInt8(): int
    {
        return (int) $this->read('cvalue')['value'];
    }

    /**
     * Reads a signed big-endian 16-bit integer (int16)
     */
    public function readInt16(): int
    {
        $value = (int) $this->read('nvalue')['value'];

        return $value >= 0x8000 ? $value - 0x10000 : $value;
    }

    /**
     * Reads a signed big-endian 32-bit integer (int32)
     */
    public function readInt32(): int
    {
        $value = (int) $this->read('Nvalue')['value'];

        return $value >= 0x80000000 ? $value - 0x100000000 : $value;
    }

    /**
     * Reads a signed big-endian 64-bit integer (int64).
     *
     * "J" is the unsigned 64-bit format, but a PHP integer is a signed 64-bit value, so the two's complement
     * representation round-trips as is.
     */
    public function readInt64(): int
    {
        return (int) $this->read('Jvalue')['value'];
    }

    /**
     * Writes a signed 8-bit integer (int8)
     */
    public function writeInt8(int $value): void
    {
        $this->write('c', $value);
    }

    /**
     * Writes a signed big-endian 16-bit integer (int16)
     */
    public function writeInt16(int $value): void
    {
        $this->write('n', $value);
    }

    /**
     * Writes a signed big-endian 32-bit integer (int32)
     */
    public function writeInt32(int $value): void
    {
        $this->write('N', $value);
    }

    /**
     * Writes a signed big-endian 64-bit integer (int64)
     */
    public function writeInt64(int $value): void
    {
        $this->write('J', $value);
    }

    /**
     * Calculates the format size for the unpack() operation
     */
    protected static function packetSize(string $format): int
    {
        static $tableSize = [
            'a' => 1,
            'A' => 1,
            'c' => 1,
            'C' => 1,
            's' => 2,
            'S' => 2,
            'n' => 2,
            'v' => 2,
            'i' => PHP_INT_SIZE,
            'I' => PHP_INT_SIZE,
            'l' => 4,
            'L' => 4,
            'N' => 4,
            'V' => 4,
            'q' => 8,
            'Q' => 8,
            'J' => 8,
            'P' => 8,
        ];
        static $cache = [];
        if (isset($cache[$format])) {
            return $cache[$format];
        }

        $numMatches = preg_match_all('/(?:\/|^)(\w)(\d*)/', $format, $matches);
        if (empty($numMatches)) {
            throw new \InvalidArgumentException("Unknown format specified: {$format}");
        }
        $size = 0;
        for ($matchIndex = 0; $matchIndex < $numMatches; $matchIndex++) {
            [$modifier, $repetition] = [$matches[1][$matchIndex], $matches[2][$matchIndex]];
            if (!isset($tableSize[$modifier])) {
                throw new \InvalidArgumentException("Unknown modifier specified: {$modifier}");
            }
            $size += $tableSize[$modifier] * ($repetition !== '' ? (int) $repetition : 1);
        }

        $cache[$format] = $size;

        return $size;
    }
}
