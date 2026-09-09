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

namespace Protocol\Kafka\Tests\Unit\Common\Utils;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\Utils\ByteUtils;

/**
 * The zigzag and size helpers mirror `ByteUtils` @ 0.11.0.3, the checksum `Crc32C` (RFC 3720 section B.4)
 */
#[CoversClass(ByteUtils::class)]
final class ByteUtilsTest extends TestCase
{
    /**
     * @return iterable<string, array{int, int, int}>  value, base, zigzag-encoded value
     */
    public static function zigzagProvider(): iterable
    {
        yield 'zero'          => [0, 32, 0];
        yield 'minus one'     => [-1, 32, 1];
        yield 'one'           => [1, 32, 2];
        yield 'minus two'     => [-2, 32, 3];
        yield '63'            => [63, 32, 126];
        yield 'minus 64'      => [-64, 32, 127];
        yield 'int32 max'     => [2147483647, 32, 4294967294];
        yield 'int32 min'     => [-2147483648, 32, 4294967295];
        yield 'long 300'      => [300, 64, 600];
        yield 'long 2^31'     => [2147483648, 64, 4294967296];
        yield 'long max'      => [PHP_INT_MAX, 64, -2];
        yield 'long min'      => [PHP_INT_MIN, 64, -1];
    }

    #[DataProvider('zigzagProvider')]
    public function testZigzagEncodesAndDecodes(int $value, int $base, int $encoded): void
    {
        self::assertSame($encoded, ByteUtils::encodeZigZag($value, $base));
        self::assertSame($value, ByteUtils::decodeZigZag($encoded));
    }

    /**
     * @return iterable<string, array{int, int, int}>  value, varint size, varlong size
     */
    public static function sizeProvider(): iterable
    {
        yield 'zero'        => [0, 1, 1];
        yield 'minus one'   => [-1, 1, 1];
        yield '63'          => [63, 1, 1];
        yield '64'          => [64, 2, 2];
        yield 'minus 65'    => [-65, 2, 2];
        yield '8191'        => [8191, 2, 2];
        yield '8192'        => [8192, 3, 3];
        yield 'int32 max'   => [2147483647, 5, 5];
        yield 'int32 min'   => [-2147483648, 5, 5];
        yield '2^35'        => [2 ** 35, 5, 6];
        yield 'long max'    => [PHP_INT_MAX, 5, 10];
        yield 'long min'    => [PHP_INT_MIN, 1, 10];
    }

    #[DataProvider('sizeProvider')]
    public function testSizeOfVarintAndVarlong(int $value, int $varintSize, int $varlongSize): void
    {
        self::assertSame($varlongSize, ByteUtils::sizeOfVarlong($value));
        if ($value >= -2147483648 && $value <= 2147483647) {
            self::assertSame($varintSize, ByteUtils::sizeOfVarint($value));
        }
    }

    public function testSizeOfUnsignedVarintCountsSevenBitGroups(): void
    {
        self::assertSame(1, ByteUtils::sizeOfUnsignedVarint(0));
        self::assertSame(1, ByteUtils::sizeOfUnsignedVarint(127));
        self::assertSame(2, ByteUtils::sizeOfUnsignedVarint(128));
        self::assertSame(5, ByteUtils::sizeOfUnsignedVarint(0xFFFFFFFF));
        self::assertSame(10, ByteUtils::sizeOfUnsignedVarint(-1));
    }

    /**
     * Check values of RFC 3720 section B.4 and the well-known CRC-32C of "123456789"
     */
    public function testCrc32cIsTheCastagnoliChecksum(): void
    {
        self::assertSame(0xE3069283, ByteUtils::crc32c('123456789'));
        self::assertSame(0x8A9136AA, ByteUtils::crc32c(str_repeat("\x00", 32)));
        self::assertSame(0x62A8AB43, ByteUtils::crc32c(str_repeat("\xff", 32)));
        self::assertSame(0x00000000, ByteUtils::crc32c(''));
    }
}
