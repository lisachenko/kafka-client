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

namespace Protocol\Kafka\Tests\Fixture;

/**
 * Builds message set bytes straight from the specification, without using any class of the library.
 *
 * <pre>
 *   MessageSet => [Offset MessageSize Message]
 *     Offset      => int64
 *     MessageSize => int32
 *
 *   Message => Crc MagicByte Attributes Key Value
 *     Crc        => int32
 *     MagicByte  => int8 (0 in the 0.8 line)
 *     Attributes => int8
 *     Key        => bytes
 *     Value      => bytes
 * </pre>
 *
 * The Produce API only carries these bytes around, so the protocol classes are tested against an independently
 * built message set instead of against the class that also produces one.
 *
 * @see docs/protocol/0.9.0.md, section "MessageSet and Message"
 */
final class SpecMessageSet
{
    /**
     * Magic byte of the only message format that Kafka 0.8.2.2 knows
     */
    public const int MAGIC_BYTE_V0 = 0;

    /**
     * Encodes a single message: the CRC-32 of everything that follows it, then the message itself
     */
    public static function message(?string $key, ?string $value, int $attributes = 0): string
    {
        $payload = pack('cc', self::MAGIC_BYTE_V0, $attributes) . self::bytes($key) . self::bytes($value);

        return pack('N', crc32($payload)) . $payload;
    }

    /**
     * Encodes one entry of a message set: the offset, the size of the message and the message itself
     */
    public static function entry(int $offset, string $message): string
    {
        return pack('JN', $offset, strlen($message)) . $message;
    }

    /**
     * Encodes a whole message set out of the given key => value pairs
     *
     * @param array<array{0: string|null, 1: string|null}> $keyValues   Messages as [key, value] pairs
     * @param int                                          $firstOffset Offset of the first entry
     */
    public static function of(array $keyValues, int $firstOffset = 0): string
    {
        $buffer = '';
        foreach (array_values($keyValues) as $index => [$key, $value]) {
            $buffer .= self::entry($firstOffset + $index, self::message($key, $value));
        }

        return $buffer;
    }

    /**
     * Encodes a `bytes` field: an int32 length followed by the content, -1 as length means null
     */
    private static function bytes(?string $value): string
    {
        if ($value === null) {
            return pack('N', -1);
        }

        return pack('N', strlen($value)) . $value;
    }
}
