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
 * Byte-exact specification of the message set, in both message formats of the 0.10 line.
 *
 * The two compressed message format v0 fixtures are the log segments that the console producer of a 0.8.2.2 broker
 * wrote:
 *
 *   $ printf 'alpha\nbravo\ncharlie\n' | kafka-console-producer.sh --broker-list 127.0.0.1:9092 \
 *         --topic t3-gzip-probe --compression-codec gzip --batch-size 3
 *   $ base64 -w0 /tmp/kafka-logs/t3-gzip-probe-0/00000000000000000000.log
 *
 * A log segment is a message set, so the bytes below are exactly what a Fetch returns for that partition. The
 * message format v1 counterparts, with their relative inner offsets, are captured from the 0.10.2.2 broker in
 * `docs/protocol/vectors/message-format.json` and replayed by tests/Compliance.
 *
 * @see docs/protocol/0.11.0.md, section "MessageSet and Message"
 */
#[CoversClass(MessageSet::class)]
final class MessageSetTest extends TestCase
{
    /**
     * Timestamp of the records below, 2017-03-12T12:00:00Z
     */
    private const int CREATE_TIME = 1489324800000;

    /**
     * Message set of three gzip-compressed messages, written by the console producer of Kafka 0.8.2.2
     */
    private const string BROKER_GZIP_SET = 'AAAAAAAAAAIAAABa3wTRVgAB/////wAAAEwfiwgAAAAAAAAAY2CAA+HE8KdxDAz/gQDIY03MKchIhEox'
        . 'gqR36KTsRUgnFSWW5UOlmYBYVGLjvBS4NHtyRmJRTmYqAHt5OXBfAAAA';

    /**
     * Message set of three snappy-compressed messages, written by the console producer of Kafka 0.8.2.2
     */
    private const string BROKER_SNAPPY_SET = 'AAAAAAAAAAIAAABnLJbGagAC/////wAAAFmCU05BUFBZAAAAAAEAAAABAAAARV8AABkBTBNhV+VeAAD/'
        . '////AAAABWFscGhhDR4gAQAAABO4LGS9GR8QYnJhdm8NHyACAAAAFRixnmQVHxwHY2hhcmxpZQ==';

    public function testAMessageSetOfFormatV1IsAPlainSequenceOfOffsetSizeAndMessage(): void
    {
        $set = MessageSet::fromRecords([
            new Record('bar', 'foo', 0, null, self::CREATE_TIME),
            new Record('v2', null, 0, null, self::CREATE_TIME + 1),
        ]);

        self::assertSame(
            // offset 0, size 28, message(magic 1, createtime, key foo, value bar)
            '0000000000000000' . '0000001c' . 'd050e196' . '0100' . '0000015ac2acf800' . '00000003' . '666f6f' . '00000003' . '626172'
            // offset 1, size 24, message(magic 1, createtime + 1, no key, value v2)
            . '0000000000000001' . '00000018' . 'aa0a28e4' . '0100' . '0000015ac2acf801' . 'ffffffff' . '00000002' . '7632',
            bin2hex($set->toBuffer())
        );
        self::assertSame(strlen($set->toBuffer()), $set->sizeInBytes());
        self::assertSame(76, $set->sizeInBytes());
        self::assertSame($set->toBuffer(), (string) $set);
        self::assertSame(Message::MAGIC_V1, $set->getMagic());
    }

    public function testAMessageSetOfFormatV0IsWrittenWithoutTimestamps(): void
    {
        $set = MessageSet::fromRecords(
            [new Record('bar', 'foo', 0, null, self::CREATE_TIME), new Record('v2')],
            CompressionCodec::NONE,
            Message::MAGIC_V0
        );

        self::assertSame(
            // offset 0, size 20, message(magic 0, key foo, value bar)
            '0000000000000000' . '00000014' . 'b8ba5f57' . '0000' . '00000003' . '666f6f' . '00000003' . '626172'
            // offset 1, size 16, message(magic 0, no key, value v2)
            . '0000000000000001' . '00000010' . 'd5960a78' . '0000' . 'ffffffff' . '00000002' . '7632',
            bin2hex($set->toBuffer()),
            'the timestamp of the record is dropped, message format v0 has no field for it'
        );
        self::assertSame(60, $set->sizeInBytes());
        self::assertSame(Message::MAGIC_V0, $set->getMagic());
    }

    public function testEmptySetSerializesIntoAnEmptyByteRegion(): void
    {
        $set = MessageSet::fromRecords([]);

        self::assertSame('', $set->toBuffer());
        self::assertSame(0, $set->sizeInBytes());
        self::assertTrue($set->isEmpty());
        self::assertSame([], $set->getRecords());
        self::assertNull($set->getMagic());
        self::assertFalse(MessageSet::fromBuffer('')->hasPartialTrailingMessage());
    }

    public function testRecordsComeBackWithTheOffsetsOfTheirMessages(): void
    {
        $set = MessageSet::fromBuffer(MessageSet::fromRecords([
            new Record('first', 'key'),
            new Record(null),
            new Record(''),
        ])->toBuffer());

        $records = $set->getRecords();

        self::assertCount(3, $records);
        self::assertSame(3, $set->count());
        self::assertSame([0, 1, 2], array_map(static fn(Record $record): ?int => $record->offset, $records));
        self::assertSame(['first', null, ''], array_map(static fn(Record $record): ?string => $record->value, $records));
        self::assertSame(['key', null, null], array_map(static fn(Record $record): ?string => $record->key, $records));
    }

    public function testRecordsComeBackWithTheTimestampAndTheTimestampTypeOfTheirMessages(): void
    {
        $set = MessageSet::fromBuffer(MessageSet::fromRecords([
            new Record('first', 'key', 0, null, self::CREATE_TIME),
            new Record('second', null, 0, null, self::CREATE_TIME + 5),
            new Record('without a timestamp'),
        ])->toBuffer());

        $records = $set->getRecords();

        self::assertSame(
            [self::CREATE_TIME, self::CREATE_TIME + 5, null],
            array_map(static fn(Record $record): ?int => $record->timestamp, $records)
        );
        self::assertSame(
            [TimestampType::CREATE_TIME, TimestampType::CREATE_TIME, TimestampType::CREATE_TIME],
            array_map(static fn(Record $record): int => $record->timestampType, $records)
        );
    }

    public function testRecordsOfAFormatV0SetCarryNoTimestampAtAll(): void
    {
        $buffer = MessageSet::fromRecords(
            [new Record('first', 'key', 0, null, self::CREATE_TIME)],
            CompressionCodec::NONE,
            Message::MAGIC_V0
        )->toBuffer();

        $record = MessageSet::fromBuffer($buffer)->getRecords()[0];

        self::assertNull($record->timestamp);
        self::assertSame(TimestampType::NO_TIMESTAMP_TYPE, $record->timestampType);
    }

    public function testRawEntriesExposeTheOffsetAndTheMessageItself(): void
    {
        $entries = MessageSet::fromRecords([new Record('bar', 'foo')])->getMessages();

        self::assertCount(1, $entries);
        [$offset, $message] = $entries[0];
        self::assertSame(0, $offset);
        self::assertInstanceOf(Message::class, $message);
        self::assertSame('bar', $message->value);
    }

    /**
     * @return iterable<string, array{0: int}>
     */
    public static function truncationLengths(): iterable
    {
        yield 'inside the value of the last message'  => [3];
        yield 'inside the header of the last message' => [20];
        yield 'inside the size field of the entry'    => [34];
        yield 'one byte short of the offset field'    => [35];
    }

    #[DataProvider('truncationLengths')]
    public function testAPartialTrailingMessageIsDroppedSilently(int $missingBytes): void
    {
        $complete = MessageSet::fromRecords([new Record('bar', 'foo'), new Record('v2')])->toBuffer();

        $set = MessageSet::fromBuffer(substr($complete, 0, strlen($complete) - $missingBytes));

        self::assertSame(1, $set->count(), 'the complete first message survives the truncation');
        self::assertSame('bar', $set->getRecords()[0]->value);
        self::assertTrue($set->hasPartialTrailingMessage());
    }

    public function testACompleteSetIsNotReportedAsPartial(): void
    {
        $set = MessageSet::fromBuffer(MessageSet::fromRecords([new Record('bar')])->toBuffer());

        self::assertFalse($set->hasPartialTrailingMessage());
    }

    public function testASingleMessageThatDoesNotFitIsReportedAsAnEmptyPartialSet(): void
    {
        // What a Fetch with a MaxBytes smaller than the first message of the partition returns
        $complete = MessageSet::fromRecords([new Record(str_repeat('x', 100))])->toBuffer();

        $set = MessageSet::fromBuffer(substr($complete, 0, 30));

        self::assertTrue($set->isEmpty());
        self::assertTrue($set->hasPartialTrailingMessage());
    }

    public function testAMessageWithACorruptChecksumIsRejected(): void
    {
        $buffer = MessageSet::fromRecords([new Record('bar', 'foo')])->toBuffer();

        $this->expectException(CorruptMessageException::class);

        MessageSet::fromBuffer(substr($buffer, 0, -1) . 'X');
    }

    public function testChecksumValidationOfTheWholeSetCanBeDisabled(): void
    {
        $buffer = MessageSet::fromRecords([new Record('bar', 'foo')])->toBuffer();

        $set = MessageSet::fromBuffer(substr($buffer, 0, -1) . 'X', false);

        self::assertSame('baX', $set->getRecords()[0]->value);
    }

    public function testAMessageSizeBelowTheMinimumIsRejected(): void
    {
        $this->expectException(CorruptMessageException::class);

        // The smallest message of any format is one of format v0 without a key and without a value
        MessageSet::fromBuffer(pack('JN', 0, Message::MIN_SIZE_V0 - 1) . str_repeat("\x00", 13));
    }

    /**
     * @return iterable<string, array{0: int}>
     */
    public static function compressionCodecs(): iterable
    {
        yield 'gzip'   => [CompressionCodec::GZIP];
        yield 'snappy' => [CompressionCodec::SNAPPY];
        yield 'lz4'    => [CompressionCodec::LZ4];
    }

    #[DataProvider('compressionCodecs')]
    public function testACompressedSetIsASingleWrapperMessageAroundTheWholeSet(int $codec): void
    {
        $records = [
            new Record('alpha', null, 0, null, self::CREATE_TIME),
            new Record('bravo', 'key', 0, null, self::CREATE_TIME + 10),
            new Record('charlie', null, 0, null, self::CREATE_TIME + 5),
        ];

        $compressed = MessageSet::fromRecords($records, $codec);
        $entries    = $compressed->getMessages();

        self::assertCount(1, $entries, 'a compressed set is one message on the wire');
        [$offset, $wrapper] = $entries[0];
        self::assertSame(2, $offset, 'the wrapper carries the offset of the last inner message, as the broker writes it');
        self::assertSame($codec, $wrapper->getCompressionCodec());
        self::assertNull($wrapper->key);
        self::assertSame(
            self::CREATE_TIME + 10,
            $wrapper->timestamp,
            'the wrapper of a CreateTime set carries the largest timestamp of its inner messages'
        );
        self::assertSame(TimestampType::CREATE_TIME, $wrapper->getTimestampType());
        self::assertSame(
            MessageSet::fromRecords($records)->toBuffer(),
            $wrapper->decompressValue(),
            'the value of the wrapper is the uncompressed inner message set'
        );
    }

    #[DataProvider('compressionCodecs')]
    public function testACompressedSetIsUnwrappedIntoItsInnerMessages(int $codec): void
    {
        $records = [new Record('alpha'), new Record('bravo', 'key'), new Record('charlie')];

        $set = MessageSet::fromBuffer(MessageSet::fromRecords($records, $codec)->toBuffer());

        self::assertSame(3, $set->count());
        self::assertSame(['alpha', 'bravo', 'charlie'], array_map(static fn(Record $r): ?string => $r->value, $set->getRecords()));
        self::assertSame([0, 1, 2], array_map(static fn(Record $r): ?int => $r->offset, $set->getRecords()));
        self::assertSame([null, 'key', null], array_map(static fn(Record $r): ?string => $r->key, $set->getRecords()));
        self::assertSame(CompressionCodec::NONE, $set->getMessages()[0][1]->getCompressionCodec());
    }

    public function testTheInnerOffsetsOfACompressedFormatV1SetAreRelativeToTheWrapper(): void
    {
        // The broker replaces the offset of the wrapper with the absolute offset of the last inner message and
        // leaves the inner offsets - 0, 1, 2 - untouched
        $wrapperOnTheWire = MessageSet::fromRecords(
            [new Record('alpha'), new Record('bravo'), new Record('charlie')],
            CompressionCodec::GZIP
        )->getMessages()[0][1];

        $innerSet = MessageSet::fromBuffer($wrapperOnTheWire->decompressValue());
        self::assertSame([0, 1, 2], array_map(static fn(Record $r): ?int => $r->offset, $innerSet->getRecords()));

        $stored = pack('JN', 42, $wrapperOnTheWire->sizeInBytes()) . $wrapperOnTheWire->toBuffer();

        self::assertSame(
            [40, 41, 42],
            array_map(static fn(Record $r): ?int => $r->offset, MessageSet::fromBuffer($stored)->getRecords()),
            'absolute = wrapperOffset - lastInnerOffset + innerOffset'
        );
    }

    public function testTheInnerOffsetsOfACompressedFormatV0SetAreTheAbsoluteOnes(): void
    {
        $inner = MessageSet::fromRecords(
            [new Record('alpha'), new Record('bravo'), new Record('charlie')],
            CompressionCodec::NONE,
            Message::MAGIC_V0
        );
        // A 0.9 broker rewrites the inner offsets of a stored set with the absolute ones it assigned
        $wrapper = MessageV0::compressed($inner->toBuffer(), CompressionCodec::GZIP);
        $stored  = pack('JN', 42, $wrapper->sizeInBytes()) . $wrapper->toBuffer();

        self::assertSame(
            [0, 1, 2],
            array_map(static fn(Record $r): ?int => $r->offset, MessageSet::fromBuffer($stored)->getRecords()),
            'message format v0 stores absolute offsets, which are taken as they are'
        );
    }

    public function testTheTimestampOfALogAppendTimeWrapperReplacesTheOnesOfItsInnerMessages(): void
    {
        // What the broker stores for a topic with message.timestamp.type=LogAppendTime: the timestamp type bit is
        // set on the wrapper only, and the timestamp of the wrapper is the append time of the whole batch
        $inner = MessageSet::fromRecords([
            new Record('alpha', null, 0, null, self::CREATE_TIME),
            new Record('bravo', null, 0, null, self::CREATE_TIME + 1),
        ]);
        $wrapper = Message::compressed(
            $inner->toBuffer(),
            CompressionCodec::GZIP,
            self::CREATE_TIME + 1000,
            TimestampType::LOG_APPEND_TIME
        );
        $stored = pack('JN', 1, $wrapper->sizeInBytes()) . $wrapper->toBuffer();

        $records = MessageSet::fromBuffer($stored)->getRecords();

        self::assertSame([self::CREATE_TIME + 1000, self::CREATE_TIME + 1000], array_map(
            static fn(Record $r): ?int => $r->timestamp,
            $records
        ));
        self::assertSame([TimestampType::LOG_APPEND_TIME, TimestampType::LOG_APPEND_TIME], array_map(
            static fn(Record $r): int => $r->timestampType,
            $records
        ));
    }

    public function testTheTimestampTypeOfACreateTimeWrapperAppliesToItsInnerMessages(): void
    {
        $set = MessageSet::fromBuffer(MessageSet::fromRecords(
            [new Record('alpha', null, 0, null, self::CREATE_TIME)],
            CompressionCodec::GZIP
        )->toBuffer());

        $record = $set->getRecords()[0];

        self::assertSame(self::CREATE_TIME, $record->timestamp, 'the inner message keeps its own CreateTime');
        self::assertSame(TimestampType::CREATE_TIME, $record->timestampType);
    }

    #[DataProvider('compressionCodecs')]
    public function testAShallowReadKeepsTheWrapperMessageAndItsBytes(int $codec): void
    {
        $buffer = MessageSet::fromRecords(
            [new Record('alpha'), new Record('bravo'), new Record('charlie')],
            $codec
        )->toBuffer();

        $shallow = MessageSet::shallowFromBuffer($buffer);

        self::assertSame(1, $shallow->count(), 'the compressed set is not unwrapped');
        self::assertSame($codec, $shallow->getMessages()[0][1]->getCompressionCodec());
        self::assertSame(
            bin2hex($buffer),
            bin2hex($shallow->toBuffer()),
            'a shallow read is the only one that writes the very same bytes back'
        );
        self::assertSame(3, MessageSet::fromBuffer($buffer)->count(), 'a deep read unwraps the same buffer');
    }

    /**
     * @return iterable<string, array{0: string}>
     */
    public static function brokerCompressedSets(): iterable
    {
        yield 'gzip written by the broker'   => [self::BROKER_GZIP_SET];
        yield 'snappy written by the broker' => [self::BROKER_SNAPPY_SET];
    }

    #[DataProvider('brokerCompressedSets')]
    public function testASetProducedByTheBrokerItselfIsDecoded(string $base64Fixture): void
    {
        $set = MessageSet::fromBuffer((string) base64_decode($base64Fixture, true));

        self::assertSame(3, $set->count());
        self::assertSame(['alpha', 'bravo', 'charlie'], array_map(static fn(Record $r): ?string => $r->value, $set->getRecords()));
        self::assertSame([0, 1, 2], array_map(static fn(Record $r): ?int => $r->offset, $set->getRecords()));
        self::assertFalse($set->hasPartialTrailingMessage());
    }

    public function testTheOffsetsOfACompressedSetAreTheInnerOnesTheBrokerAssigned(): void
    {
        // The wrapper of the fixture sits at offset 2, its inner messages at 0, 1 and 2
        $buffer = (string) base64_decode(self::BROKER_GZIP_SET, true);
        self::assertSame(2, (int) unpack('Joffset', $buffer)['offset']);

        self::assertSame([0, 1, 2], array_map(
            static fn(Record $record): ?int => $record->offset,
            MessageSet::fromBuffer($buffer)->getRecords()
        ));
    }

    public function testTheCodecBitsOfARecordNeverLeakIntoAnUncompressedMessage(): void
    {
        // A record never carries the codec bits itself: they belong to the wrapper message of a compressed set
        $set = MessageSet::fromRecords([new Record('bar', 'foo', CompressionCodec::GZIP)], CompressionCodec::NONE);

        self::assertSame(0, $set->getMessages()[0][1]->attributes);
    }

    public function testAnInnerMessageAlwaysAnnouncesTheCreateTimeTimestampType(): void
    {
        // "producers must set the inner message timestamp type to CreateTime, otherwise the messages will be
        // rejected by broker" - ByteBufferMessageSet.scala @ 0.10.2.2
        $set = MessageSet::fromRecords(
            [new Record('bar', 'foo', TimestampType::MASK, null, self::CREATE_TIME)],
            CompressionCodec::GZIP
        );

        $inner = MessageSet::fromBuffer($set->getMessages()[0][1]->decompressValue());

        self::assertSame(0, $inner->getMessages()[0][1]->attributes);
        self::assertSame(TimestampType::CREATE_TIME, $inner->getMessages()[0][1]->getTimestampType());
    }
}
