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
 * Byte-exact specification of the message set of the 0.8 protocol.
 *
 * The two compressed fixtures are the log segments that the console producer of the broker itself wrote:
 *
 *   $ printf 'alpha\nbravo\ncharlie\n' | kafka-console-producer.sh --broker-list 127.0.0.1:9092 \
 *         --topic t3-gzip-probe --compression-codec gzip --batch-size 3
 *   $ base64 -w0 /tmp/kafka-logs/t3-gzip-probe-0/00000000000000000000.log
 *
 * A log segment is a message set, so the bytes below are exactly what a Fetch returns for that partition.
 *
 * @see docs/protocol/0.8.2.md, section "MessageSet and Message"
 */
#[CoversClass(MessageSet::class)]
final class MessageSetTest extends TestCase
{
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

    public function testMessageSetIsAPlainSequenceOfOffsetSizeAndMessage(): void
    {
        $set = MessageSet::fromRecords([new Record('bar', 'foo'), new Record('v2')]);

        self::assertSame(
            // offset 0, size 20, message(key foo, value bar)
            '0000000000000000' . '00000014' . 'b8ba5f57' . '0000' . '00000003' . '666f6f' . '00000003' . '626172'
            // offset 1, size 16, message(no key, value v2)
            . '0000000000000001' . '00000010' . 'd5960a78' . '0000' . 'ffffffff' . '00000002' . '7632',
            bin2hex($set->toBuffer())
        );
        self::assertSame(strlen($set->toBuffer()), $set->sizeInBytes());
        self::assertSame(60, $set->sizeInBytes());
        self::assertSame($set->toBuffer(), (string) $set);
    }

    public function testEmptySetSerializesIntoAnEmptyByteRegion(): void
    {
        $set = MessageSet::fromRecords([]);

        self::assertSame('', $set->toBuffer());
        self::assertSame(0, $set->sizeInBytes());
        self::assertTrue($set->isEmpty());
        self::assertSame([], $set->getRecords());
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
        yield 'inside the size field of the entry'    => [26];
        yield 'one byte short of the offset field'    => [27];
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

        MessageSet::fromBuffer(pack('JN', 0, Message::MIN_SIZE - 1) . str_repeat("\x00", 13));
    }

    /**
     * @return iterable<string, array{0: int}>
     */
    public static function compressionCodecs(): iterable
    {
        yield 'gzip'   => [CompressionCodec::GZIP];
        yield 'snappy' => [CompressionCodec::SNAPPY];
    }

    #[DataProvider('compressionCodecs')]
    public function testACompressedSetIsASingleWrapperMessageAroundTheWholeSet(int $codec): void
    {
        $records = [new Record('alpha'), new Record('bravo', 'key'), new Record('charlie')];

        $compressed = MessageSet::fromRecords($records, $codec);
        $entries    = $compressed->getMessages();

        self::assertCount(1, $entries, 'a compressed set is one message on the wire');
        [$offset, $wrapper] = $entries[0];
        self::assertSame(2, $offset, 'the wrapper carries the offset of the last inner message, as the broker writes it');
        self::assertSame($codec, $wrapper->getCompressionCodec());
        self::assertNull($wrapper->key);
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
}
