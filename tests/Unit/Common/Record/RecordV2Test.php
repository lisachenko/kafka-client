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
use Protocol\Kafka\Common\Record\Header;
use Protocol\Kafka\Common\Record\RecordV2;
use Protocol\Kafka\IO\StringStream;

/**
 * Byte-exact specification of a single record of the message format v2.
 *
 * The reference bytes are the ones the 0.11.0.3 broker wrote for the vector `messageformat.v2.none.createtime`:
 * the record `alpha` without a key, without headers and at the offset and the timestamp of its batch is
 * `16 00 00 00 01 0a 61 6c 70 68 61 00`.
 *
 * @see docs/protocol/1.1.md, section "RecordBatch (message format v2)"
 */
#[CoversClass(RecordV2::class)]
final class RecordV2Test extends TestCase
{
    /**
     * The first record of the vector `messageformat.v2.none.createtime`, as the broker stored it
     */
    private const string BROKER_RECORD = '1600000001' . '0a616c706861' . '00';

    public function testARecordIsWrittenAsTheBrokerWritesIt(): void
    {
        $record = new RecordV2('alpha');

        self::assertSame(self::BROKER_RECORD, bin2hex($record->toBuffer()));
        self::assertSame(11, $record->length, 'the Length counts every byte that follows it');
        self::assertSame(12, $record->sizeInBytes());
    }

    public function testARecordOfTheBrokerIsReadBackIntoItsFields(): void
    {
        $record = RecordV2::fromBuffer((string) hex2bin(self::BROKER_RECORD));

        self::assertSame(11, $record->length);
        self::assertSame(0, $record->attributes);
        self::assertSame(0, $record->timestampDelta);
        self::assertSame(0, $record->offsetDelta);
        self::assertNull($record->key);
        self::assertSame('alpha', $record->value);
        self::assertSame([], $record->headers);
        self::assertSame(self::BROKER_RECORD, bin2hex($record->toBuffer()));
    }

    /**
     * @return iterable<string, array{0: RecordV2}>
     */
    public static function records(): iterable
    {
        yield 'without a key and a value'   => [new RecordV2()];
        yield 'with an empty key'           => [new RecordV2('value', '')];
        yield 'with an empty value'         => [new RecordV2('', 'key')];
        yield 'with a header'               => [new RecordV2('v', 'k', [new Header('h', 'value')])];
        yield 'with a null header value'    => [new RecordV2('v', 'k', [new Header('h', null)])];
        yield 'with two headers'            => [new RecordV2('v', null, [new Header('a', 'b'), new Header('c', '')])];
        yield 'with a large offset delta'   => [new RecordV2('v', null, [], 0, 0, 1000000)];
        yield 'with a negative delta'       => [new RecordV2('v', null, [], 0, -5000, 0)];
        yield 'with a huge timestamp delta' => [new RecordV2('v', null, [], 0, 1600000000000, 3)];
        yield 'with a binary value'         => [new RecordV2("\x00\xff\x7f", "\x00")];
    }

    #[DataProvider('records')]
    public function testEveryRecordSurvivesAWriteAndReadRoundTrip(RecordV2 $record): void
    {
        $buffer = $record->toBuffer();

        $read = RecordV2::fromBuffer($buffer);

        self::assertSame(bin2hex($buffer), bin2hex($read->toBuffer()));
        self::assertSame($record->key, $read->key);
        self::assertSame($record->value, $read->value);
        self::assertSame($record->offsetDelta, $read->offsetDelta);
        self::assertSame($record->timestampDelta, $read->timestampDelta);
        self::assertCount(count($record->headers), $read->headers);
        self::assertSame(strlen($buffer), $read->sizeInBytes());
    }

    public function testTheLengthGrowsBeyondASingleVarintByte(): void
    {
        $record = new RecordV2(str_repeat('x', 1000));

        // 1000 bytes of value plus its 2-byte varint length, the attributes, the two deltas, the null key and the
        // header count, one byte each
        self::assertSame(1007, $record->length);
        self::assertSame(1009, $record->sizeInBytes(), 'the Length itself takes two varint bytes');
        self::assertSame(1009, strlen($record->toBuffer()));
    }

    public function testARecordThatAnnouncesMoreBytesThanItHoldsIsRefused(): void
    {
        $record = new RecordV2('alpha');
        $buffer = $record->toBuffer();
        // Raise the length from 11 (0x16 zigzag) to 13 (0x1a zigzag) without adding the bytes
        $buffer[0] = "\x1a";

        $this->expectException(CorruptMessageException::class);
        RecordV2::fromBuffer($buffer);
    }

    public function testARecordThatAnnouncesFewerBytesThanItHoldsIsRefused(): void
    {
        $buffer    = new RecordV2('alpha')->toBuffer();
        $buffer[0] = "\x18"; // zigzag varint 12 instead of 11

        $this->expectException(CorruptMessageException::class);
        RecordV2::fromBuffer($buffer);
    }

    public function testARecordWithANegativeLengthIsRefused(): void
    {
        $this->expectException(CorruptMessageException::class);
        RecordV2::fromBuffer("\x01");
    }

    public function testARecordWithANegativeHeaderCountIsRefused(): void
    {
        // Length 6, attributes 0, timestampDelta 0, offsetDelta 0, key null, value null, header count -1
        $this->expectException(CorruptMessageException::class);
        RecordV2::fromBuffer((string) hex2bin('0c000000010101'));
    }

    public function testAHeaderWithoutAKeyIsRefused(): void
    {
        // Length 7, no key, no value, one header whose key length is -1 and whose value is null
        $this->expectException(CorruptMessageException::class);
        RecordV2::fromBuffer((string) hex2bin('100000000101020101'));
    }

    public function testAnUnfinishedRecordIsRefused(): void
    {
        $this->expectException(CorruptMessageException::class);
        RecordV2::unpackFrom(new StringStream((string) hex2bin('1600')));
    }
}
