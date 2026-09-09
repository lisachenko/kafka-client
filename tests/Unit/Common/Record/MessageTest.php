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
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\Errors\CorruptMessageException;
use Protocol\Kafka\Common\Record\CompressionCodec;
use Protocol\Kafka\Common\Record\Message;
use Protocol\Kafka\Common\Record\MessageSet;
use Protocol\Kafka\Common\Record\Record;

/**
 * Byte-exact specification of the message format v0.
 *
 * The checksums of the first two vectors are the ones that the broker itself computed for the same key and value:
 *
 *   $ echo bar | kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic t3-crc-probe
 *   $ kafka-run-class.sh kafka.tools.DumpLogSegments --files .../00000000000000000000.log --print-data-log
 *   offset: 0 ... crc: 520903 payload: bar                        # 520903     == 0x0007f2c7
 *   offset: 0 ... crc: 3099221847 keysize: 3 key: foo payload: bar # 3099221847 == 0xb8ba5f57
 *
 * @see docs/protocol/0.10.2.md, section "MessageSet and Message"
 */
#[CoversClass(Message::class)]
final class MessageTest extends TestCase
{
    /**
     * @return iterable<string, array{0: ?string, 1: ?string, 2: int, 3: string}>
     */
    public static function messageVectors(): iterable
    {
        //                        value   key     attrs   crc        magic attrs keySize   key     valueSize value
        yield 'key and value'  => ['bar', 'foo',  0, 'b8ba5f57' . '00' . '00' . '00000003' . '666f6f' . '00000003' . '626172'];
        yield 'null key'       => ['bar', null,   0, '0007f2c7' . '00' . '00' . 'ffffffff' . '' . '00000003' . '626172'];
        yield 'null value'     => [null,  'foo',  0, '55e6454e' . '00' . '00' . '00000003' . '666f6f' . 'ffffffff' . ''];
        yield 'empty value'    => ['',    'foo',  0, '8b5d65ad' . '00' . '00' . '00000003' . '666f6f' . '00000000' . ''];
        yield 'empty key'      => ['bar', '',     0, '947fbcd4' . '00' . '00' . '00000000' . '' . '00000003' . '626172'];
        yield 'null key/value' => [null,  null,   0, 'a7ec6803' . '00' . '00' . 'ffffffff' . '' . 'ffffffff' . ''];
        yield 'gzip attribute' => ['bar', null,   1, '9ba2bea8' . '00' . '01' . 'ffffffff' . '' . '00000003' . '626172'];
    }

    #[DataProvider('messageVectors')]
    public function testMessageIsSerializedByteForByte(?string $value, ?string $key, int $attributes, string $expectedHex): void
    {
        $message = new Message($value, $key, $attributes);

        self::assertSame($expectedHex, bin2hex($message->toBuffer()));
        self::assertSame(strlen($message->toBuffer()), $message->sizeInBytes());
    }

    #[DataProvider('messageVectors')]
    public function testMessageIsParsedBackFromItsOwnBytes(?string $value, ?string $key, int $attributes, string $expectedHex): void
    {
        $message = Message::fromBuffer((string) hex2bin($expectedHex));

        self::assertSame($value, $message->value);
        self::assertSame($key, $message->key);
        self::assertSame($attributes, $message->attributes);
        self::assertSame(Message::MAGIC_V0, $message->magicByte);
    }

    public function testChecksumCoversEverythingAfterTheCrcField(): void
    {
        $message = new Message('bar', 'foo');
        $payload = substr($message->toBuffer(), 4);

        self::assertSame(crc32($payload), $message->crc);
        self::assertSame(0xB8BA5F57, $message->crc, 'the checksum the broker computed for the same message');
    }

    public function testChecksumIsTheUnsignedValueOfTheInt32Field(): void
    {
        // 0xb8ba5f57 has its highest bit set, and the engine reads an int32 as a signed value
        $message = Message::fromBuffer(new Message('bar', 'foo')->toBuffer());

        self::assertSame(0xB8BA5F57, $message->crc);
        self::assertGreaterThan(0, $message->crc);
    }

    public function testAMismatchingChecksumIsReportedAsACorruptMessage(): void
    {
        $buffer = new Message('bar', 'foo')->toBuffer();
        // Flip the last byte of the value, leaving the announced checksum in place
        $corrupted = substr($buffer, 0, -1) . 'X';

        $this->expectException(CorruptMessageException::class);
        $this->expectExceptionCode(CorruptMessageException::CORRUPT_MESSAGE);

        Message::fromBuffer($corrupted);
    }

    public function testChecksumValidationIsSkippedWhenTheClientDisablesIt(): void
    {
        $corrupted = substr(new Message('bar', 'foo')->toBuffer(), 0, -1) . 'X';

        $message = Message::fromBuffer($corrupted, false);

        self::assertSame('baX', $message->value);
        self::assertNotSame($message->computeCrc(), $message->crc);
    }

    public function testUpdatingTheChecksumAfterAChangeMakesTheMessageValidAgain(): void
    {
        $message = new Message('bar', 'foo');

        $message->value = 'changed';
        $message->updateCrc();
        $message->validateCrc();

        self::assertSame('changed', Message::fromBuffer($message->toBuffer())->value);
    }

    public function testCompressionCodecIsReadFromTheThreeLowestBitsOfTheAttributes(): void
    {
        self::assertSame(CompressionCodec::NONE, new Message('bar')->getCompressionCodec());
        self::assertFalse(new Message('bar')->isCompressed());
        self::assertSame(CompressionCodec::GZIP, new Message('bar', null, CompressionCodec::GZIP)->getCompressionCodec());
        self::assertSame(CompressionCodec::SNAPPY, new Message('bar', null, CompressionCodec::SNAPPY)->getCompressionCodec());
        self::assertTrue(new Message('bar', null, CompressionCodec::SNAPPY)->isCompressed());
        // Bits above the codec mask belong to later message formats and are ignored here
        self::assertSame(CompressionCodec::GZIP, new Message('bar', null, 0x09)->getCompressionCodec());
    }

    public function testWrapperMessageCarriesTheCompressedMessageSetAsItsValue(): void
    {
        $inner   = MessageSet::fromRecords([new Record('bar', 'foo')])->toBuffer();
        $wrapper = Message::compressed($inner, CompressionCodec::GZIP);

        self::assertSame(CompressionCodec::GZIP, $wrapper->getCompressionCodec());
        self::assertNull($wrapper->key);
        self::assertSame($inner, $wrapper->decompressValue());
        self::assertSame($wrapper->computeCrc(), $wrapper->crc);
    }

    public function testMinimalMessageSizeIsTheSizeOfAMessageWithoutKeyAndValue(): void
    {
        self::assertSame(Message::MIN_SIZE, new Message(null, null)->sizeInBytes());
        self::assertSame(14, Message::MIN_SIZE);
    }

    public function testMessageIsItsOwnBinaryRepresentation(): void
    {
        $message = new Message('bar', 'foo');

        self::assertSame($message->toBuffer(), (string) $message);
    }
}
