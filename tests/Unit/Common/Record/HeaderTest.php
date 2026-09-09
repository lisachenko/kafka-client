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

namespace Protocol\Kafka\Tests\Unit\Common\Record;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\Record\Header;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\BinarySchema;

/**
 * The header of a record: two varint-prefixed byte arrays, the first of which is never null (KIP-82)
 *
 * @see docs/protocol/0.11.0.md, section "RecordBatch (message format v2)"
 */
#[CoversClass(Header::class)]
final class HeaderTest extends TestCase
{
    public function testWriteThenReadObjectRoundTrips(): void
    {
        $header = new Header('content-type', 'application/json');

        $writeStream = new StringStream();
        BinarySchema::writeObjectToStream($header, $writeStream);

        $readStream = new StringStream($writeStream->getBuffer());
        $result     = BinarySchema::readObjectFromStream(Header::class, $readStream);

        self::assertSame($header->key, $result->key);
        self::assertSame($header->value, $result->value);
    }

    public function testAHeaderIsWrittenAsTwoZigzagVarintByteArrays(): void
    {
        $stream = new StringStream();
        BinarySchema::writeObjectToStream(new Header('a', 'b'), $stream);

        // 02 is the zigzag varint 1, the length of "a"; 61 is "a"; then the same for the value "b"
        self::assertSame('02610262', bin2hex($stream->getBuffer()));
    }

    public function testAHeaderValueIsNullable(): void
    {
        $stream = new StringStream();
        BinarySchema::writeObjectToStream(new Header('flag', null), $stream);

        // 01 is the zigzag varint -1, which is the null byte array
        self::assertStringEndsWith('01', bin2hex($stream->getBuffer()));

        $header = BinarySchema::readObjectFromStream(Header::class, new StringStream($stream->getBuffer()));
        self::assertSame('flag', $header->key);
        self::assertNull($header->value);
    }

    public function testAnEmptyHeaderValueIsNotANullOne(): void
    {
        $stream = new StringStream();
        BinarySchema::writeObjectToStream(new Header('flag', ''), $stream);

        // 00 is the zigzag varint 0, the empty byte array
        self::assertStringEndsWith('00', bin2hex($stream->getBuffer()));

        $header = BinarySchema::readObjectFromStream(Header::class, new StringStream($stream->getBuffer()));
        self::assertSame('', $header->value);
    }

    public function testTheSizeOfAHeaderIsTheSizeOfItsTwoVarintByteArrays(): void
    {
        // 1 byte of length plus 12 bytes of key, 1 byte of length plus 4 bytes of value
        self::assertSame(18, BinarySchema::getObjectTypeSize(new Header('content-type', 'json')));
        // A null value is a single byte
        self::assertSame(6, BinarySchema::getObjectTypeSize(new Header('flag', null)));
    }
}
