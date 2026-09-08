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
 * Common implementation of the Kafka protocol primitive types on top of raw byte access.
 *
 * Concrete streams only have to provide {@see AbstractStream::readRaw()} and {@see AbstractStream::writeRaw()}.
 */
abstract class AbstractStream implements Stream
{
    /**
     * Length prefix that encodes a null string (int16 -1).
     */
    public const string NULL_STRING = "\xFF\xFF";

    /**
     * Length prefix that encodes a null byte array (int32 -1).
     */
    public const string NULL_BYTES = "\xFF\xFF\xFF\xFF";

    public function readInt8(): int
    {
        /** @var array{value: int} $unpacked */
        $unpacked = unpack('cvalue', $this->readRaw(1));

        return $unpacked['value'];
    }

    public function readInt16(): int
    {
        /** @var array{value: int} $unpacked */
        $unpacked = unpack('nvalue', $this->readRaw(2));
        $value    = $unpacked['value'];

        return $value >= 0x8000 ? $value - 0x10000 : $value;
    }

    public function readInt32(): int
    {
        /** @var array{value: int} $unpacked */
        $unpacked = unpack('Nvalue', $this->readRaw(4));
        $value    = $unpacked['value'];

        return $value >= 0x80000000 ? $value - 0x100000000 : $value;
    }

    public function readInt64(): int
    {
        // "J" is the unsigned 64-bit big-endian format, but a PHP integer is a signed 64-bit value,
        // therefore the two's complement representation round-trips as is.
        /** @var array{value: int} $unpacked */
        $unpacked = unpack('Jvalue', $this->readRaw(8));

        return $unpacked['value'];
    }

    public function writeInt8(int $value): void
    {
        $this->writeRaw(pack('c', $value));
    }

    public function writeInt16(int $value): void
    {
        $this->writeRaw(pack('n', $value));
    }

    public function writeInt32(int $value): void
    {
        $this->writeRaw(pack('N', $value));
    }

    public function writeInt64(int $value): void
    {
        $this->writeRaw(pack('J', $value));
    }

    public function readString(): ?string
    {
        $length = $this->readInt16();
        if ($length < 0) {
            return null;
        }

        return $this->readRaw($length);
    }

    public function writeString(?string $value): void
    {
        if ($value === null) {
            $this->writeRaw(self::NULL_STRING);

            return;
        }

        $this->writeInt16(strlen($value));
        $this->writeRaw($value);
    }

    public function readBytes(): ?string
    {
        $length = $this->readInt32();
        if ($length < 0) {
            return null;
        }

        return $this->readRaw($length);
    }

    public function writeBytes(?string $data): void
    {
        if ($data === null) {
            $this->writeRaw(self::NULL_BYTES);

            return;
        }

        $this->writeInt32(strlen($data));
        $this->writeRaw($data);
    }

    public function readArray(callable $elementReader): array
    {
        $numberOfItems = $this->readInt32();
        $items         = [];
        for ($index = 0; $index < $numberOfItems; $index++) {
            $items[] = $elementReader($this);
        }

        return $items;
    }

    public function writeArray(iterable $items, callable $elementWriter): void
    {
        $items = is_array($items) ? $items : iterator_to_array($items, false);

        $this->writeInt32(count($items));
        foreach ($items as $item) {
            $elementWriter($this, $item);
        }
    }

    public function read(string $format): array
    {
        $unpacked = unpack($format, $this->readRaw(self::packetSize($format)));
        if ($unpacked === false) {
            throw new \InvalidArgumentException("Can not unpack the data with the format: {$format}");
        }

        return $unpacked;
    }

    public function write(string $format, mixed ...$arguments): void
    {
        $this->writeRaw(pack($format, ...$arguments));
    }

    public function readByteArray(): ?string
    {
        return $this->readBytes();
    }

    public function writeByteArray(?string $data): void
    {
        $this->writeBytes($data);
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
