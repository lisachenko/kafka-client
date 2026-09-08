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

namespace Protocol\Kafka\Tests\Unit\IO;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\Errors\NetworkException;
use Protocol\Kafka\IO\AbstractStream;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\IO\StringStream;

/**
 * Byte-exact tests for the protocol primitive types.
 *
 * @see docs/protocol/0.8.2.md, section "Protocol Primitive Types"
 */
#[CoversClass(StringStream::class)]
#[CoversClass(AbstractStream::class)]
final class StringStreamTest extends TestCase
{
    /**
     * @return iterable<string, array{int, string}>
     */
    public static function int8Vectors(): iterable
    {
        yield 'zero'    => [0, '00'];
        yield 'one'     => [1, '01'];
        yield 'max'     => [127, '7f'];
        yield 'minus 1' => [-1, 'ff'];
        yield 'min'     => [-128, '80'];
    }

    /**
     * @return iterable<string, array{int, string}>
     */
    public static function int16Vectors(): iterable
    {
        yield 'zero'         => [0, '0000'];
        yield 'api key 3'    => [3, '0003'];
        yield 'max'          => [32767, '7fff'];
        yield 'minus 1'      => [-1, 'ffff'];
        yield 'minus 2'      => [-2, 'fffe'];
        yield 'min'          => [-32768, '8000'];
        yield 'error code 6' => [6, '0006'];
    }

    /**
     * @return iterable<string, array{int, string}>
     */
    public static function int32Vectors(): iterable
    {
        yield 'zero'    => [0, '00000000'];
        yield 'one'     => [1, '00000001'];
        yield 'max'     => [2147483647, '7fffffff'];
        yield 'minus 1' => [-1, 'ffffffff'];
        yield 'minus 2' => [-2, 'fffffffe'];
        yield 'min'     => [-2147483648, '80000000'];
    }

    /**
     * @return iterable<string, array{int, string}>
     */
    public static function int64Vectors(): iterable
    {
        yield 'zero'         => [0, '0000000000000000'];
        yield 'one'          => [1, '0000000000000001'];
        yield 'latest time'  => [-1, 'ffffffffffffffff'];
        yield 'earliest'     => [-2, 'fffffffffffffffe'];
        yield 'max'          => [PHP_INT_MAX, '7fffffffffffffff'];
        yield 'min'          => [PHP_INT_MIN, '8000000000000000'];
        yield 'above 32 bit' => [4294967296, '0000000100000000'];
    }

    /**
     * @return iterable<string, array{?string, string}>
     */
    public static function stringVectors(): iterable
    {
        yield 'null'   => [null, 'ffff'];
        yield 'empty'  => ['', '0000'];
        yield 'test'   => ['test', '000474657374'];
        yield 'binary' => ["\x00\x01", '00020001'];
    }

    /**
     * @return iterable<string, array{?string, string}>
     */
    public static function bytesVectors(): iterable
    {
        yield 'null'   => [null, 'ffffffff'];
        yield 'empty'  => ['', '00000000'];
        yield 'test'   => ['test', '0000000474657374'];
        yield 'binary' => ["\xDE\xAD\xBE\xEF", '00000004deadbeef'];
    }

    #[DataProvider('int8Vectors')]
    public function testInt8IsWrittenAndReadBackByteExact(int $value, string $expectedHex): void
    {
        $this->assertPrimitiveRoundTrip(
            $expectedHex,
            static fn(Stream $stream) => $stream->writeInt8($value),
            static fn(Stream $stream) => $stream->readInt8(),
            $value
        );
    }

    #[DataProvider('int16Vectors')]
    public function testInt16IsWrittenAndReadBackByteExact(int $value, string $expectedHex): void
    {
        $this->assertPrimitiveRoundTrip(
            $expectedHex,
            static fn(Stream $stream) => $stream->writeInt16($value),
            static fn(Stream $stream) => $stream->readInt16(),
            $value
        );
    }

    #[DataProvider('int32Vectors')]
    public function testInt32IsWrittenAndReadBackByteExact(int $value, string $expectedHex): void
    {
        $this->assertPrimitiveRoundTrip(
            $expectedHex,
            static fn(Stream $stream) => $stream->writeInt32($value),
            static fn(Stream $stream) => $stream->readInt32(),
            $value
        );
    }

    #[DataProvider('int64Vectors')]
    public function testInt64IsWrittenAndReadBackByteExact(int $value, string $expectedHex): void
    {
        $this->assertPrimitiveRoundTrip(
            $expectedHex,
            static fn(Stream $stream) => $stream->writeInt64($value),
            static fn(Stream $stream) => $stream->readInt64(),
            $value
        );
    }

    #[DataProvider('stringVectors')]
    public function testStringIsWrittenAndReadBackByteExact(?string $value, string $expectedHex): void
    {
        $this->assertPrimitiveRoundTrip(
            $expectedHex,
            static fn(Stream $stream) => $stream->writeString($value),
            static fn(Stream $stream) => $stream->readString(),
            $value
        );
    }

    #[DataProvider('bytesVectors')]
    public function testBytesAreWrittenAndReadBackByteExact(?string $value, string $expectedHex): void
    {
        $this->assertPrimitiveRoundTrip(
            $expectedHex,
            static fn(Stream $stream) => $stream->writeBytes($value),
            static fn(Stream $stream) => $stream->readBytes(),
            $value
        );
    }

    public function testNullStringPrefixIsMinusOne(): void
    {
        self::assertSame("\xFF\xFF", AbstractStream::NULL_STRING);
        self::assertNull(StringStream::fromString("\xFF\xFF")->readString());
    }

    public function testNullBytesPrefixIsMinusOne(): void
    {
        self::assertSame("\xFF\xFF\xFF\xFF", AbstractStream::NULL_BYTES);
        self::assertNull(StringStream::fromString("\xFF\xFF\xFF\xFF")->readBytes());
    }

    public function testEmptyArrayIsEncodedAsZeroCount(): void
    {
        $buffer = '';
        $stream = new StringStream($buffer);
        $stream->writeArray([], static fn(Stream $stream, string $item) => $stream->writeString($item));

        self::assertSame('00000000', bin2hex($buffer));
        self::assertSame([], StringStream::fromString($buffer)->readArray(
            static fn(Stream $stream) => $stream->readString()
        ));
    }

    public function testArrayOfStringsIsPrefixedWithInt32Count(): void
    {
        $buffer = '';
        $stream = new StringStream($buffer);
        $stream->writeArray(['foo', 'bar'], static fn(Stream $stream, string $item) => $stream->writeString($item));

        self::assertSame('00000002' . '0003666f6f' . '0003626172', bin2hex($buffer));

        $decoded = StringStream::fromString($buffer)->readArray(static fn(Stream $stream) => $stream->readString());
        self::assertSame(['foo', 'bar'], $decoded);
    }

    public function testArrayOfStructuresIsReadElementByElement(): void
    {
        // [Partition Offset] as used by the Offsets API
        $buffer = '';
        $stream = new StringStream($buffer);
        $stream->writeArray(
            [[0, 42], [1, -1]],
            static function (Stream $stream, array $item): void {
                $stream->writeInt32($item[0]);
                $stream->writeInt64($item[1]);
            }
        );

        self::assertSame(
            '00000002' . '00000000' . '000000000000002a' . '00000001' . 'ffffffffffffffff',
            bin2hex($buffer)
        );

        $decoded = StringStream::fromString($buffer)->readArray(
            static fn(Stream $stream) => [$stream->readInt32(), $stream->readInt64()]
        );
        self::assertSame([[0, 42], [1, -1]], $decoded);
    }

    public function testWriteArrayAcceptsTraversable(): void
    {
        $buffer = '';
        $stream = new StringStream($buffer);
        $stream->writeArray(
            new \ArrayIterator(['a', 'b']),
            static fn(Stream $stream, string $item) => $stream->writeString($item)
        );

        self::assertSame('00000002' . '000161' . '000162', bin2hex($buffer));
    }

    public function testReadRawReturnsExactAmountOfBytesAndAdvancesThePointer(): void
    {
        $stream = StringStream::fromString('abcdef');

        self::assertSame(6, $stream->remaining());
        self::assertFalse($stream->isEmpty());
        self::assertSame('abc', $stream->readRaw(3));
        self::assertSame(3, $stream->remaining());
        self::assertSame('', $stream->readRaw(0));
        self::assertSame('def', $stream->readRaw(3));
        self::assertTrue($stream->isEmpty());
    }

    public function testReadRawThrowsWhenTheBufferIsExhausted(): void
    {
        $stream = StringStream::fromString('ab');

        $this->expectException(NetworkException::class);
        $stream->readRaw(3);
    }

    public function testReadRawRejectsNegativeLength(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        StringStream::fromString('ab')->readRaw(-1);
    }

    public function testWritesAreVisibleInTheReferencedBuffer(): void
    {
        $buffer = '';
        $stream = new StringStream($buffer);
        $stream->writeInt16(-1);

        self::assertSame("\xFF\xFF", $buffer);
    }

    public function testLegacyPackFormatApiIsStillSupported(): void
    {
        $buffer = '';
        $stream = new StringStream($buffer);
        $stream->write('nN', 3, 1);

        self::assertSame('000300000001', bin2hex($buffer));
        self::assertSame(
            ['apiKey' => 3, 'correlationId' => 1],
            StringStream::fromString($buffer)->read('napiKey/NcorrelationId')
        );
    }

    public function testDeprecatedByteArrayAliasesDelegateToBytes(): void
    {
        $buffer = '';
        $stream = new StringStream($buffer);
        $stream->writeByteArray(null);
        $stream->writeByteArray('ok');

        self::assertSame('ffffffff' . '000000026f6b', bin2hex($buffer));

        $reader = StringStream::fromString($buffer);
        self::assertNull($reader->readByteArray());
        self::assertSame('ok', $reader->readByteArray());
    }

    /**
     * @param callable(Stream): void  $writer
     * @param callable(Stream): mixed $reader
     */
    private function assertPrimitiveRoundTrip(
        string $expectedHex,
        callable $writer,
        callable $reader,
        mixed $expectedValue
    ): void {
        $buffer = '';
        $stream = new StringStream($buffer);
        $writer($stream);

        self::assertSame($expectedHex, bin2hex($buffer), 'Unexpected binary representation');
        self::assertSame($expectedValue, $reader(StringStream::fromString($buffer)), 'Value did not round-trip');
    }
}
