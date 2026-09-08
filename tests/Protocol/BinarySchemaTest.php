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

namespace Protocol\Kafka\Tests\Protocol;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\BinarySchema;

final class BinarySchemaTest extends TestCase
{
    #[DataProvider('primitiveTypeProvider')]
    public function testWriteThenReadRoundTrips(int $type, mixed $value): void
    {
        $writeStream = new StringStream();
        BinarySchema::writeSingleType($type, $value, $writeStream);

        $readStream = new StringStream($writeStream->getBuffer());
        $result = BinarySchema::readSingleType($type, $readStream);

        self::assertSame($value, $result);
    }

    public static function primitiveTypeProvider(): array
    {
        return [
            'int8 positive' => [BinarySchema::TYPE_INT8, 42],
            'int16 positive' => [BinarySchema::TYPE_INT16, 1000],
            'int16 negative' => [BinarySchema::TYPE_INT16, -1000],
            'int32 positive' => [BinarySchema::TYPE_INT32, 100_000],
            'int32 negative' => [BinarySchema::TYPE_INT32, -100_000],
            'int64' => [BinarySchema::TYPE_INT64, 9_000_000_000],
            'varint' => [BinarySchema::TYPE_VARINT, 300],
            'varint zigzag positive' => [BinarySchema::TYPE_VARINT_ZIGZAG, 12345],
            'varint zigzag negative' => [BinarySchema::TYPE_VARINT_ZIGZAG, -12345],
            'varlong zigzag' => [BinarySchema::TYPE_VARLONG_ZIGZAG, -987654321],
            'varchar' => [BinarySchema::TYPE_VARCHAR, 'hello kafka'],
            'varchar zigzag' => [BinarySchema::TYPE_VARCHAR_ZIGZAG, 'hello kafka'],
            'string' => [BinarySchema::TYPE_STRING, 'hello'],
            'bytearray' => [BinarySchema::TYPE_BYTEARRAY, "\x01\x02\x03"],
        ];
    }

    public function testNullableStringRoundTripsNull(): void
    {
        $writeStream = new StringStream();
        BinarySchema::writeSingleType(BinarySchema::TYPE_NULLABLE_STRING, null, $writeStream);

        $readStream = new StringStream($writeStream->getBuffer());
        self::assertNull(BinarySchema::readSingleType(BinarySchema::TYPE_NULLABLE_STRING, $readStream));
    }

    public function testNullableStringRoundTripsValue(): void
    {
        $writeStream = new StringStream();
        BinarySchema::writeSingleType(BinarySchema::TYPE_NULLABLE_STRING, 'present', $writeStream);

        $readStream = new StringStream($writeStream->getBuffer());
        self::assertSame('present', BinarySchema::readSingleType(BinarySchema::TYPE_NULLABLE_STRING, $readStream));
    }

    public function testGetSingleTypeSizeMatchesActualWrittenBytes(): void
    {
        $value = 'hello kafka';
        $expectedSize = BinarySchema::getSingleTypeSize(BinarySchema::TYPE_VARCHAR, $value);

        $stream = new StringStream();
        BinarySchema::writeSingleType(BinarySchema::TYPE_VARCHAR, $value, $stream);

        self::assertSame($expectedSize, strlen($stream->getBuffer()));
    }
}
