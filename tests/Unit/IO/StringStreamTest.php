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
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\Errors\NetworkException;
use Protocol\Kafka\IO\AbstractStream;
use Protocol\Kafka\IO\StringStream;

/**
 * Tests the in-memory stream and the primitive helpers of the shared base class.
 *
 * The byte-exact vectors of every protocol type live in the BinarySchema test, this one covers the stream itself.
 */
#[CoversClass(StringStream::class)]
#[CoversClass(AbstractStream::class)]
final class StringStreamTest extends TestCase
{
    public function testWrittenBytesEndUpInTheBuffer(): void
    {
        $stream = new StringStream();
        $stream->write('nN', 3, 1);

        self::assertSame('000300000001', bin2hex($stream->getBuffer()));
    }

    public function testReadConsumesTheBufferInOrder(): void
    {
        $stream = new StringStream(hex2bin('0003' . '00000001'));

        self::assertSame(['apiKey' => 3], $stream->read('napiKey'));
        self::assertSame(4, $stream->remaining());
        self::assertSame(['correlationId' => 1], $stream->read('NcorrelationId'));
        self::assertTrue($stream->isEmpty());
    }

    public function testStreamStartsFromAnOptionalBuffer(): void
    {
        self::assertTrue(new StringStream()->isEmpty());
        self::assertTrue(new StringStream(null)->isEmpty());
        self::assertSame('abc', new StringStream('abc')->getBuffer());
        self::assertSame(3, new StringStream('abc')->remaining());
    }

    public function testStringStreamIsAlwaysConnected(): void
    {
        self::assertTrue(new StringStream()->isConnected());
    }

    public function testReadingPastTheEndOfTheBufferIsAnError(): void
    {
        $stream = new StringStream(hex2bin('0003'));

        $this->expectException(NetworkException::class);
        $stream->read('NcorrelationId');
    }

    public function testReadRawReturnsExactlyTheRequestedBytes(): void
    {
        $stream = new StringStream('abcdef');

        self::assertSame('', $stream->readRaw(0));
        self::assertSame('abc', $stream->readRaw(3));
        self::assertSame('def', $stream->readRaw(3));
        self::assertTrue($stream->isEmpty());
    }

    public function testReadRawRejectsANegativeLength(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        new StringStream('ab')->readRaw(-1);
    }

    public function testTypedIntegerHelpersAreSignedAndBigEndian(): void
    {
        $stream = new StringStream();
        $stream->writeInt8(-1);
        $stream->writeInt16(-2);
        $stream->writeInt32(-3);
        $stream->writeInt64(-4);

        self::assertSame('ff' . 'fffe' . 'fffffffd' . 'fffffffffffffffc', bin2hex($stream->getBuffer()));

        $reader = new StringStream($stream->getBuffer());
        self::assertSame(-1, $reader->readInt8());
        self::assertSame(-2, $reader->readInt16());
        self::assertSame(-3, $reader->readInt32());
        self::assertSame(-4, $reader->readInt64());
    }

    public function testStringIsWrittenWithAnInt16LengthPrefix(): void
    {
        $stream = new StringStream();
        $stream->writeString('test');
        $stream->writeString('');

        self::assertSame('000474657374' . '0000', bin2hex($stream->getBuffer()));

        $reader = new StringStream($stream->getBuffer());
        self::assertSame('test', $reader->readString());
        self::assertSame('', $reader->readString());
    }

    public function testReadStringRejectsTheNullLengthPrefix(): void
    {
        self::assertSame("\xFF\xFF", AbstractStream::NULL_STRING);

        $this->expectException(\UnexpectedValueException::class);
        new StringStream(AbstractStream::NULL_STRING)->readString();
    }

    public function testByteArrayIsWrittenWithAnInt32LengthPrefixAndIsNullable(): void
    {
        self::assertSame("\xFF\xFF\xFF\xFF", AbstractStream::NULL_BYTES);

        $stream = new StringStream();
        $stream->writeByteArray(null);
        $stream->writeByteArray('');
        $stream->writeByteArray('ok');

        self::assertSame('ffffffff' . '00000000' . '000000026f6b', bin2hex($stream->getBuffer()));

        $reader = new StringStream($stream->getBuffer());
        self::assertNull($reader->readByteArray());
        self::assertSame('', $reader->readByteArray());
        self::assertSame('ok', $reader->readByteArray());
    }

    public function testWriteBufferAppendsRawBytes(): void
    {
        $stream = new StringStream();
        $stream->writeBuffer("\x00\x01");
        $stream->writeBuffer(null);
        $stream->writeBuffer('');
        $stream->writeBuffer("\x02");

        self::assertSame('000102', bin2hex($stream->getBuffer()));
    }
}
