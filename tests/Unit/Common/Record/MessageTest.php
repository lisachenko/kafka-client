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
use Protocol\Kafka\Common\Record\MessageV0;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\TimestampType;

/**
 * Byte-exact specification of the message formats v0 and v1.
 *
 * The checksums of the first two v0 vectors are the ones that the broker itself computed for the same key and value:
 *
 *   $ echo bar | kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic t3-crc-probe
 *   $ kafka-run-class.sh kafka.tools.DumpLogSegments --files .../00000000000000000000.log --print-data-log
 *   offset: 0 ... crc: 520903 payload: bar                        # 520903     == 0x0007f2c7
 *   offset: 0 ... crc: 3099221847 keysize: 3 key: foo payload: bar # 3099221847 == 0xb8ba5f57
 *
 * The v1 vectors carry the fixed timestamp 1489324800000 (2017-03-12T12:00:00Z, inside the 0.10.2 era) and were
 * built straight from the grammar; the message format v1 of a real broker is in
 * `docs/protocol/vectors/message-format.json`, replayed by tests/Compliance.
 *
 * @see docs/protocol/2.8.md, section "MessageSet and Message"
 */
#[CoversClass(Message::class)]
#[CoversClass(MessageV0::class)]
#[CoversClass(TimestampType::class)]
final class MessageTest extends TestCase
{
    /**
     * @return iterable<string, array{0: ?string, 1: ?string, 2: int, 3: string}>
     */
    public static function messageVectorsV0(): iterable
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

    /**
     * The very same messages in message format v1, with a fixed timestamp in the middle of the header
     *
     * @return iterable<string, array{0: ?string, 1: ?string, 2: int, 3: int, 4: string}>
     */
    public static function messageVectorsV1(): iterable
    {
        //                             value  key    attrs ts             crc        magic attrs timestamp          keySize    key      valueSize  value
        yield 'key and value'       => ['bar', 'foo', 0, 1489324800000, 'd050e196' . '01' . '00' . '0000015ac2acf800' . '00000003' . '666f6f' . '00000003' . '626172'];
        yield 'null key'            => ['bar', null,  0, 1489324800000, '6264fde5' . '01' . '00' . '0000015ac2acf800' . 'ffffffff' . '' . '00000003' . '626172'];
        yield 'no timestamp'        => ['bar', 'foo', 0, Message::NO_TIMESTAMP, 'b9d7c5a8' . '01' . '00' . 'ffffffffffffffff' . '00000003' . '666f6f' . '00000003' . '626172'];
        yield 'log append time'     => ['bar', null,  TimestampType::MASK, 1489324800000, 'ffbf2b57' . '01' . '08' . '0000015ac2acf800' . 'ffffffff' . '' . '00000003' . '626172'];
        yield 'gzip and createtime' => ['bar', null,  CompressionCodec::GZIP, 1489324800000, '0703c6a3' . '01' . '01' . '0000015ac2acf800' . 'ffffffff' . '' . '00000003' . '626172'];
    }

    #[DataProvider('messageVectorsV0')]
    public function testAMessageOfFormatV0IsSerializedByteForByte(?string $value, ?string $key, int $attributes, string $expectedHex): void
    {
        $message = new MessageV0($value, $key, $attributes);

        self::assertSame($expectedHex, bin2hex($message->toBuffer()));
        self::assertSame(strlen($message->toBuffer()), $message->sizeInBytes());
        self::assertSame(Message::MAGIC_V0, $message->magicByte);
    }

    #[DataProvider('messageVectorsV1')]
    public function testAMessageOfFormatV1IsSerializedByteForByte(?string $value, ?string $key, int $attributes, int $timestamp, string $expectedHex): void
    {
        $message = new Message($value, $key, $attributes, $timestamp);

        self::assertSame($expectedHex, bin2hex($message->toBuffer()));
        self::assertSame(strlen($message->toBuffer()), $message->sizeInBytes());
        self::assertSame(Message::MAGIC_V1, $message->magicByte);
    }

    #[DataProvider('messageVectorsV0')]
    public function testAMessageOfFormatV0IsParsedBackFromItsOwnBytes(?string $value, ?string $key, int $attributes, string $expectedHex): void
    {
        $message = Message::fromBuffer((string) hex2bin($expectedHex));

        self::assertInstanceOf(MessageV0::class, $message, 'the magic byte selects the class of the message');
        self::assertSame($value, $message->value);
        self::assertSame($key, $message->key);
        self::assertSame($attributes, $message->attributes);
        self::assertSame(Message::MAGIC_V0, $message->magicByte);
        self::assertNull($message->getTimestamp(), 'message format v0 has no timestamp field');
        self::assertSame(TimestampType::NO_TIMESTAMP_TYPE, $message->getTimestampType());
    }

    #[DataProvider('messageVectorsV1')]
    public function testAMessageOfFormatV1IsParsedBackFromItsOwnBytes(?string $value, ?string $key, int $attributes, int $timestamp, string $expectedHex): void
    {
        $message = Message::fromBuffer((string) hex2bin($expectedHex));

        self::assertSame(Message::class, $message::class, 'the magic byte selects the class of the message');
        self::assertSame($value, $message->value);
        self::assertSame($key, $message->key);
        self::assertSame($attributes, $message->attributes);
        self::assertSame(Message::MAGIC_V1, $message->magicByte);
        self::assertSame($timestamp, $message->timestamp);
        self::assertSame(
            $timestamp === Message::NO_TIMESTAMP ? null : $timestamp,
            $message->getTimestamp(),
            'a timestamp of -1 means that the message carries none'
        );
    }

    public function testTheTimestampTypeIsBitThreeOfTheAttributes(): void
    {
        $createTime    = new Message('bar', null, 0, 1489324800000);
        $logAppendTime = new Message('bar', null, TimestampType::MASK, 1489324800000);

        self::assertSame(TimestampType::CREATE_TIME, $createTime->getTimestampType());
        self::assertSame(TimestampType::LOG_APPEND_TIME, $logAppendTime->getTimestampType());
        // The codec keeps the three lowest bits, whatever the timestamp type is
        self::assertSame(CompressionCodec::NONE, $logAppendTime->getCompressionCodec());
        self::assertSame(
            CompressionCodec::SNAPPY,
            new Message('bar', null, TimestampType::MASK | CompressionCodec::SNAPPY)->getCompressionCodec()
        );
        // A message of format v0 has no timestamp type at all, even when the bit happens to be set
        self::assertSame(TimestampType::NO_TIMESTAMP_TYPE, new MessageV0('bar', null, 0x08)->getTimestampType());
    }

    public function testAMessageOfFormatV0NeverCarriesATimestamp(): void
    {
        $message = new MessageV0('bar', 'foo', 0, 1489324800000);

        self::assertSame(Message::NO_TIMESTAMP, $message->timestamp);
        self::assertNull($message->getTimestamp());
        self::assertSame(Message::MIN_SIZE_V0 + 6, $message->sizeInBytes(), 'no eight bytes of timestamp are written');
    }

    public function testAnUnknownMagicByteIsReportedAsACorruptMessage(): void
    {
        $buffer = substr_replace(new Message('bar')->toBuffer(), "\x07", 4, 1);

        $this->expectException(CorruptMessageException::class);
        $this->expectExceptionMessageMatches('/Kafka 0\.11\.0\.3 does not know/');

        Message::fromBuffer($buffer, false);
    }

    public function testTheMagicByteOfARecordBatchTellsTheCallerWhichReaderToUse(): void
    {
        // The message format v2 is the RecordBatch of Kafka 0.11, whose fields after the magic byte are not the
        // ones of a message; a byte region that holds one is read by MemoryRecords
        $buffer = substr_replace(new Message('bar')->toBuffer(), "\x02", 4, 1);

        $this->expectException(CorruptMessageException::class);
        $this->expectExceptionMessageMatches('/MemoryRecords/');

        Message::fromBuffer($buffer, false);
    }

    public function testChecksumCoversEverythingAfterTheCrcField(): void
    {
        $message = new MessageV0('bar', 'foo');
        $payload = substr($message->toBuffer(), 4);

        self::assertSame(crc32($payload), $message->crc);
        self::assertSame(0xB8BA5F57, $message->crc, 'the checksum the broker computed for the same message');
    }

    public function testChecksumOfFormatV1CoversTheTimestampAsWell(): void
    {
        $withTimestamp    = new Message('bar', 'foo', 0, 1489324800000);
        $withoutTimestamp = new Message('bar', 'foo', 0, 1489324800001);

        self::assertSame(crc32(substr($withTimestamp->toBuffer(), 4)), $withTimestamp->crc);
        self::assertNotSame($withTimestamp->crc, $withoutTimestamp->crc, 'the timestamp is part of the checksum');
    }

    public function testChecksumIsTheUnsignedValueOfTheInt32Field(): void
    {
        // 0xb8ba5f57 has its highest bit set, and the engine reads an int32 as a signed value
        $message = Message::fromBuffer(new MessageV0('bar', 'foo')->toBuffer());

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
        self::assertSame(CompressionCodec::LZ4, new Message('bar', null, CompressionCodec::LZ4)->getCompressionCodec());
        self::assertTrue(new Message('bar', null, CompressionCodec::SNAPPY)->isCompressed());
        // The timestamp type bit above the codec mask never changes the codec
        self::assertSame(CompressionCodec::GZIP, new Message('bar', null, 0x09)->getCompressionCodec());
    }

    public function testWrapperMessageCarriesTheCompressedMessageSetAsItsValue(): void
    {
        $inner   = MessageSet::fromRecords([new Record('bar', 'foo')])->toBuffer();
        $wrapper = Message::compressed($inner, CompressionCodec::GZIP, 1489324800000);

        self::assertSame(CompressionCodec::GZIP, $wrapper->getCompressionCodec());
        self::assertNull($wrapper->key);
        self::assertSame($inner, $wrapper->decompressValue());
        self::assertSame($wrapper->computeCrc(), $wrapper->crc);
        self::assertSame(1489324800000, $wrapper->timestamp, 'the wrapper carries the timestamp of the whole set');
        self::assertSame(TimestampType::CREATE_TIME, $wrapper->getTimestampType());
    }

    public function testAWrapperMessageOfFormatV0CarriesNeitherTimestampNorTimestampType(): void
    {
        $inner   = MessageSet::fromRecords([new Record('bar', 'foo')], CompressionCodec::NONE, Message::MAGIC_V0);
        $wrapper = MessageV0::compressed($inner->toBuffer(), CompressionCodec::GZIP, 1489324800000);

        self::assertSame(CompressionCodec::GZIP, $wrapper->attributes, 'no timestamp type bit in message format v0');
        self::assertSame(Message::NO_TIMESTAMP, $wrapper->timestamp);
        self::assertSame($inner->toBuffer(), $wrapper->decompressValue());
    }

    public function testMinimalMessageSizeIsTheSizeOfAMessageWithoutKeyAndValue(): void
    {
        self::assertSame(Message::MIN_SIZE, new Message(null, null)->sizeInBytes());
        self::assertSame(MessageV0::MIN_SIZE, new MessageV0(null, null)->sizeInBytes());
        self::assertSame(14, Message::MIN_SIZE_V0);
        self::assertSame(22, Message::MIN_SIZE_V1);
        self::assertSame(14, Message::minSizeOfMagic(Message::MAGIC_V0));
        self::assertSame(22, Message::minSizeOfMagic(Message::MAGIC_V1));
    }

    public function testTheMagicByteSelectsTheClassAndTheScheme(): void
    {
        self::assertSame(MessageV0::class, Message::classOfMagic(Message::MAGIC_V0));
        self::assertSame(Message::class, Message::classOfMagic(Message::MAGIC_V1));
        self::assertInstanceOf(MessageV0::class, Message::ofMagic(Message::MAGIC_V0, 'bar'));
        self::assertInstanceOf(Message::class, Message::ofMagic(Message::MAGIC_V1, 'bar'));

        self::assertSame(
            ['crc', 'magicByte', 'attributes', 'key', 'value'],
            array_keys(MessageV0::getScheme())
        );
        self::assertSame(
            ['crc', 'magicByte', 'attributes', 'timestamp', 'key', 'value'],
            array_keys(Message::getScheme()),
            'the Timestamp field sits between the attributes and the key'
        );
    }

    public function testMessageIsItsOwnBinaryRepresentation(): void
    {
        $message = new Message('bar', 'foo');

        self::assertSame($message->toBuffer(), (string) $message);
    }
}
