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

namespace Protocol\Kafka\Tests\Unit\Protocol;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Tests\Fixture\BrokerRecord;

/**
 * Byte-exact tests for the schema engine and the protocol primitive types.
 *
 * @see docs/protocol/0.10.2.md, section "Protocol Primitive Types"
 */
#[CoversClass(BinarySchema::class)]
final class BinarySchemaTest extends TestCase
{
    /**
     * @return array<string, array{int, mixed}>
     */
    public static function primitiveTypeProvider(): array
    {
        return [
            'int8 positive'  => [BinarySchema::TYPE_INT8, 42],
            'int8 negative'  => [BinarySchema::TYPE_INT8, -1],
            'int16 positive' => [BinarySchema::TYPE_INT16, 1000],
            'int16 negative' => [BinarySchema::TYPE_INT16, -1000],
            'int32 positive' => [BinarySchema::TYPE_INT32, 100_000],
            'int32 negative' => [BinarySchema::TYPE_INT32, -100_000],
            'int64'          => [BinarySchema::TYPE_INT64, 9_000_000_000],
            'int64 negative' => [BinarySchema::TYPE_INT64, -9_000_000_000],
            'string'         => [BinarySchema::TYPE_STRING, 'hello'],
            'string empty'   => [BinarySchema::TYPE_STRING, ''],
            'bytearray'      => [BinarySchema::TYPE_BYTEARRAY, "\x01\x02\x03"],
            'bytearray null' => [BinarySchema::TYPE_BYTEARRAY, null],
        ];
    }

    /**
     * Hex vectors taken straight from the grammar
     *
     * @return iterable<string, array{int, mixed, string}>
     */
    public static function byteVectorProvider(): iterable
    {
        yield 'int8 zero'          => [BinarySchema::TYPE_INT8, 0, '00'];
        yield 'int8 max'           => [BinarySchema::TYPE_INT8, 127, '7f'];
        yield 'int8 minus one'     => [BinarySchema::TYPE_INT8, -1, 'ff'];
        yield 'int8 min'           => [BinarySchema::TYPE_INT8, -128, '80'];

        yield 'int16 api key 3'    => [BinarySchema::TYPE_INT16, 3, '0003'];
        yield 'int16 max'          => [BinarySchema::TYPE_INT16, 32767, '7fff'];
        yield 'int16 minus one'    => [BinarySchema::TYPE_INT16, -1, 'ffff'];
        yield 'int16 min'          => [BinarySchema::TYPE_INT16, -32768, '8000'];

        yield 'int32 one'          => [BinarySchema::TYPE_INT32, 1, '00000001'];
        yield 'int32 max'          => [BinarySchema::TYPE_INT32, 2147483647, '7fffffff'];
        yield 'int32 minus one'    => [BinarySchema::TYPE_INT32, -1, 'ffffffff'];
        yield 'int32 min'          => [BinarySchema::TYPE_INT32, -2147483648, '80000000'];

        yield 'int64 zero'         => [BinarySchema::TYPE_INT64, 0, '0000000000000000'];
        yield 'int64 latest time'  => [BinarySchema::TYPE_INT64, -1, 'ffffffffffffffff'];
        yield 'int64 earliest'     => [BinarySchema::TYPE_INT64, -2, 'fffffffffffffffe'];
        yield 'int64 max'          => [BinarySchema::TYPE_INT64, PHP_INT_MAX, '7fffffffffffffff'];
        yield 'int64 min'          => [BinarySchema::TYPE_INT64, PHP_INT_MIN, '8000000000000000'];
        yield 'int64 above 32 bit' => [BinarySchema::TYPE_INT64, 4294967296, '0000000100000000'];

        yield 'string empty'       => [BinarySchema::TYPE_STRING, '', '0000'];
        yield 'string test'        => [BinarySchema::TYPE_STRING, 'test', '000474657374'];

        yield 'nullable string'    => [BinarySchema::TYPE_NULLABLE_STRING, null, 'ffff'];
        yield 'nullable present'   => [BinarySchema::TYPE_NULLABLE_STRING, 'test', '000474657374'];
        yield 'nullable empty'     => [BinarySchema::TYPE_NULLABLE_STRING, '', '0000'];

        yield 'bytes empty'        => [BinarySchema::TYPE_BYTEARRAY, '', '00000000'];
        yield 'bytes value'        => [BinarySchema::TYPE_BYTEARRAY, "\xDE\xAD\xBE\xEF", '00000004deadbeef'];
        yield 'bytes null'         => [BinarySchema::TYPE_BYTEARRAY, null, 'ffffffff'];
    }

    #[DataProvider('primitiveTypeProvider')]
    public function testWriteThenReadRoundTrips(int $type, mixed $value): void
    {
        $writeStream = new StringStream();
        BinarySchema::writeSingleType($type, $value, $writeStream);

        $readStream = new StringStream($writeStream->getBuffer());

        self::assertSame($value, BinarySchema::readSingleType($type, $readStream));
        self::assertTrue($readStream->isEmpty(), 'The reader must consume exactly the written bytes');
    }

    #[DataProvider('byteVectorProvider')]
    public function testSingleTypeIsEncodedByteExact(int $type, mixed $value, string $expectedHex): void
    {
        $stream = new StringStream();
        BinarySchema::writeSingleType($type, $value, $stream);

        self::assertSame($expectedHex, bin2hex($stream->getBuffer()));
        self::assertSame($value, BinarySchema::readSingleType($type, new StringStream($stream->getBuffer())));
    }

    #[DataProvider('byteVectorProvider')]
    public function testSingleTypeSizeMatchesTheWrittenBytes(int $type, mixed $value, string $expectedHex): void
    {
        self::assertSame(intdiv(strlen($expectedHex), 2), BinarySchema::getSingleTypeSize($type, $value));
    }

    public function testInt8IsReadAsASignedValue(): void
    {
        // The Attributes byte of a Message and every error code of the protocol are signed
        self::assertSame(-1, BinarySchema::readSingleType(BinarySchema::TYPE_INT8, new StringStream("\xFF")));
        self::assertSame(-128, BinarySchema::readSingleType(BinarySchema::TYPE_INT8, new StringStream("\x80")));
    }

    public function testNullableStringRoundTripsNull(): void
    {
        $writeStream = new StringStream();
        BinarySchema::writeSingleType(BinarySchema::TYPE_NULLABLE_STRING, null, $writeStream);

        self::assertSame('ffff', bin2hex($writeStream->getBuffer()));
        self::assertNull(
            BinarySchema::readSingleType(BinarySchema::TYPE_NULLABLE_STRING, new StringStream($writeStream->getBuffer()))
        );
    }

    public function testNullableStringRoundTripsValue(): void
    {
        $writeStream = new StringStream();
        BinarySchema::writeSingleType(BinarySchema::TYPE_NULLABLE_STRING, 'present', $writeStream);

        $readStream = new StringStream($writeStream->getBuffer());
        self::assertSame('present', BinarySchema::readSingleType(BinarySchema::TYPE_NULLABLE_STRING, $readStream));
    }

    public function testNullByteArrayIsWrittenAsMinusOne(): void
    {
        $stream = new StringStream();
        BinarySchema::writeSingleType(BinarySchema::TYPE_BYTEARRAY, null, $stream);

        self::assertSame('ffffffff', bin2hex($stream->getBuffer()));
        self::assertNull(BinarySchema::readSingleType(BinarySchema::TYPE_BYTEARRAY, new StringStream("\xFF\xFF\xFF\xFF")));
    }

    public function testNonNullableStringRejectsAMinusOneLength(): void
    {
        $this->expectException(\UnexpectedValueException::class);
        BinarySchema::readSingleType(BinarySchema::TYPE_STRING, new StringStream("\xFF\xFF"));
    }

    public function testEmptyArrayIsEncodedAsZeroCount(): void
    {
        $stream = new StringStream();
        BinarySchema::writeSingleType([BinarySchema::TYPE_STRING], [], $stream);

        self::assertSame('00000000', bin2hex($stream->getBuffer()));
        self::assertSame(
            [],
            BinarySchema::readSingleType([BinarySchema::TYPE_STRING], new StringStream($stream->getBuffer()))
        );
    }

    public function testArrayOfStringsIsPrefixedWithAnInt32Count(): void
    {
        $stream = new StringStream();
        BinarySchema::writeSingleType([BinarySchema::TYPE_STRING], ['foo', 'bar'], $stream);

        self::assertSame('00000002' . '0003666f6f' . '0003626172', bin2hex($stream->getBuffer()));
        self::assertSame(
            ['foo', 'bar'],
            BinarySchema::readSingleType([BinarySchema::TYPE_STRING], new StringStream($stream->getBuffer()))
        );
        self::assertSame(
            4 + 5 + 5,
            BinarySchema::getSingleTypeSize([BinarySchema::TYPE_STRING], ['foo', 'bar'])
        );
    }

    public function testNullableArrayRoundTripsNull(): void
    {
        $scheme = [BinarySchema::TYPE_STRING, BinarySchema::FLAG_NULLABLE => true];

        $stream = new StringStream();
        BinarySchema::writeSingleType($scheme, null, $stream);

        self::assertSame('ffffffff', bin2hex($stream->getBuffer()));
        self::assertNull(BinarySchema::readSingleType($scheme, new StringStream($stream->getBuffer())));
        self::assertSame(4, BinarySchema::getSingleTypeSize($scheme, null));
    }

    public function testNotNullableArrayRejectsNull(): void
    {
        $this->expectException(\UnexpectedValueException::class);
        BinarySchema::writeSingleType([BinarySchema::TYPE_STRING], null, new StringStream());
    }

    public function testNestedObjectIsWrittenFieldByField(): void
    {
        $stream = new StringStream();
        BinarySchema::writeObjectToStream(BrokerRecord::of(0, '127.0.0.1', 9092), $stream);

        self::assertSame('00000000' . '0009' . bin2hex('127.0.0.1') . '00002384', bin2hex($stream->getBuffer()));

        $broker = BinarySchema::readObjectFromStream(BrokerRecord::class, new StringStream($stream->getBuffer()));
        self::assertSame(0, $broker->nodeId);
        self::assertSame('127.0.0.1', $broker->host);
        self::assertSame(9092, $broker->port);
    }

    public function testArrayOfObjectsCanBeIndexedByAFieldOfTheItem(): void
    {
        $stream = new StringStream();
        BinarySchema::writeSingleType(
            [BrokerRecord::class],
            [BrokerRecord::of(1, 'kafka-1', 9092), BrokerRecord::of(2, 'kafka-2', 9093)],
            $stream
        );

        $brokers = BinarySchema::readSingleType(
            ['nodeId' => BrokerRecord::class],
            new StringStream($stream->getBuffer())
        );

        self::assertSame([1, 2], array_keys($brokers));
        self::assertSame('kafka-2', $brokers[2]->host);
    }

    public function testObjectSizeMatchesTheWrittenBytes(): void
    {
        $broker = BrokerRecord::of(0, '127.0.0.1', 9092);

        $stream = new StringStream();
        BinarySchema::writeObjectToStream($broker, $stream);

        self::assertSame(BinarySchema::getObjectTypeSize($broker), strlen($stream->getBuffer()));
    }

    public function testUnknownSchemeTypeIsRejected(): void
    {
        $this->expectException(\RuntimeException::class);
        BinarySchema::writeSingleType(9999, 1, new StringStream());
    }
}
