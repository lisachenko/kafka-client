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
use Protocol\Kafka\Common\Utils\ByteUtils;
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

    public function testRawVarintsRoundTripAndAreBounded(): void
    {
        $stream = new StringStream();
        $stream->writeVarint(300);
        $stream->writeVarlong(0xFFFFFFFF);
        $stream->writeVarlong(-1);
        self::assertSame('ac02ffffffff0fffffffffffffffffff01', bin2hex($stream->getBuffer()));

        self::assertSame(300, $stream->readVarint());
        self::assertSame(0xFFFFFFFF, $stream->readVarlong());
        self::assertSame(-1, $stream->readVarlong());
        self::assertTrue($stream->isEmpty());

        $this->expectException(NetworkException::class);
        new StringStream("\xff\xff\xff\xff\xff\x01")->readVarint();
    }

    /**
     * The unsigned varints of KIP-482, at every boundary where the encoding grows by a byte
     *
     * `ByteUtils.writeUnsignedVarint` @ 2.8.2 writes the value itself, 7 bits per byte, least significant group
     * first - no zigzag step, unlike the varints of a record batch - so 127 is one byte and 128 is two. The five
     * boundaries below are where a compact length prefix of the protocol changes its width, and the last one is the
     * widest an unsigned varint can be: `2^32 - 1` in five bytes.
     *
     * @return array<string, array{int, string}>
     */
    public static function unsignedVarintProvider(): array
    {
        return [
            'zero'                => [0, '00'],
            'one'                 => [1, '01'],
            'the last single byte' => [127, '7f'],
            'the first two bytes' => [128, '8001'],
            'the last two bytes'  => [16383, 'ff7f'],
            'the first three bytes' => [16384, '808001'],
            'the last four bytes' => [268435455, 'ffffff7f'],
            'the first five bytes' => [268435456, '8080808001'],
            'the largest uint32'  => [4294967295, 'ffffffff0f'],
        ];
    }

    #[DataProvider('unsignedVarintProvider')]
    public function testUnsignedVarintsAreWrittenSevenBitsPerByte(int $value, string $hex): void
    {
        $stream = new StringStream();
        $stream->writeUnsignedVarint($value);

        self::assertSame($hex, bin2hex($stream->getBuffer()));
        self::assertSame(strlen($hex) / 2, ByteUtils::sizeOfUnsignedVarint($value));
        self::assertSame($value, new StringStream((string) hex2bin($hex))->readUnsignedVarint());
    }

    /**
     * The unsigned varint and the varint of a record batch read the same bytes and mean different values
     */
    public function testTheUnsignedVarintIsTheRawVarintWithoutTheZigzagStep(): void
    {
        $stream = new StringStream("\xac\x02");

        self::assertSame(300, $stream->readUnsignedVarint());
        self::assertSame(300, new StringStream("\xac\x02")->readVarint(), 'the same bytes, read raw');
        self::assertSame(
            150,
            ByteUtils::decodeZigZag(new StringStream("\xac\x02")->readVarint()),
            'and 150 once the zigzag step of the record format is applied'
        );
    }

    /**
     * An unsigned varint is at most five bytes wide, like the varint it shares its loop with
     */
    public function testAnUnsignedVarintLongerThanFiveBytesIsRejected(): void
    {
        $this->expectException(NetworkException::class);

        new StringStream("\xff\xff\xff\xff\xff\x01")->readUnsignedVarint();
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
