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
use Protocol\Kafka\Common\Errors\CorruptMessageException;
use Protocol\Kafka\Common\Record\CompressionCodec;
use Protocol\Kafka\Common\Record\ControlRecordType;
use Protocol\Kafka\Common\Record\EndTransactionMarker;
use Protocol\Kafka\Common\Record\MemoryRecords;
use Protocol\Kafka\Common\Record\Message;
use Protocol\Kafka\Common\Record\MessageSet;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Common\Record\TimestampType;

/**
 * The reader of a byte region of an unknown message format, which is what a 0.11 broker can answer.
 *
 * A 0.11.0.3 broker serves all three formats: the log of a topic is in its `message.format.version` and a Fetch
 * below version 4 is answered down-converted, so a client has to look at the magic byte of every entry of a region
 * before it can read it. The magic sits at the offset 16 in all three formats, which is what this reader uses.
 *
 * @see docs/protocol/0.11.0.md, section "RecordBatch (message format v2)"
 */
#[CoversClass(MemoryRecords::class)]
final class MemoryRecordsTest extends TestCase
{
    /**
     * A fixed CreateTime for the records below, 2020-09-13T12:26:40Z
     */
    private const int CREATE_TIME = 1600000000000;

    public function testAnEmptyRegionHoldsNothing(): void
    {
        $region = MemoryRecords::fromBuffer('');

        self::assertTrue($region->isEmpty());
        self::assertSame(0, $region->count());
        self::assertSame([], $region->getRecords());
        self::assertSame([], $region->getBatches());
        self::assertNull($region->getMagic());
        self::assertSame(0, $region->sizeInBytes());
        self::assertFalse($region->hasPartialTrailingRecord());
    }

    public function testARegionOfRecordBatchesIsReadBatchByBatch(): void
    {
        $first  = RecordBatch::fromRecords([new Record('alpha')], CompressionCodec::NONE, 0);
        $second = RecordBatch::fromRecords([new Record('bravo'), new Record('charlie')], CompressionCodec::GZIP, 1);

        $region = MemoryRecords::fromBuffer($first->toBuffer() . $second->toBuffer());

        self::assertSame(2, $region->count(), 'the count of a region is its number of batches');
        self::assertCount(3, $region->getRecords());
        self::assertSame(RecordBatch::MAGIC, $region->getMagic());
        self::assertSame(
            bin2hex($first->toBuffer() . $second->toBuffer()),
            bin2hex($region->toBuffer()),
            'a region answers the bytes it was read from'
        );
        self::assertSame(strlen($first->toBuffer() . $second->toBuffer()), $region->sizeInBytes());
        self::assertSame([0, 1, 2], array_map(static fn(Record $r): ?int => $r->offset, $region->getRecords()));
    }

    public function testARegionOfALegacyMessageSetIsReadAsMessageSetEntries(): void
    {
        $set = MessageSet::fromRecords(
            [
                new Record('alpha', null, 0, null, self::CREATE_TIME, TimestampType::CREATE_TIME),
                new Record('bravo', 'key', 0, null, self::CREATE_TIME, TimestampType::CREATE_TIME),
            ],
            CompressionCodec::NONE,
            Message::MAGIC_V1
        );

        $region = MemoryRecords::fromBuffer($set->toBuffer());

        self::assertSame(Message::MAGIC_V1, $region->getMagic());
        self::assertSame(2, $region->count(), 'a legacy entry is one batch of its own');
        self::assertCount(2, $region->getRecords());
        self::assertContainsOnlyInstancesOf(MessageSet::class, $region->getBatches());
        self::assertSame(bin2hex($set->toBuffer()), bin2hex($region->toBuffer()));
    }

    public function testACompressedLegacySetIsOneBatchOfSeveralRecords(): void
    {
        $set = MessageSet::fromRecords(
            [
                new Record('alpha', null, 0, null, self::CREATE_TIME, TimestampType::CREATE_TIME),
                new Record('bravo', null, 0, null, self::CREATE_TIME, TimestampType::CREATE_TIME),
            ],
            CompressionCodec::GZIP,
            Message::MAGIC_V1
        );

        $region = MemoryRecords::fromBuffer($set->toBuffer());

        self::assertSame(1, $region->count(), 'the wrapper message is the batch');
        self::assertCount(2, $region->getRecords(), 'its records are the ones it compresses');
        self::assertSame(bin2hex($set->toBuffer()), bin2hex($region->toBuffer()));
    }

    public function testTheThreeFormatsCanStandNextToEachOtherInOneRegion(): void
    {
        $v0 = MessageSet::fromRecords([new Record('zero')], CompressionCodec::NONE, Message::MAGIC_V0);
        $v1 = MessageSet::fromRecords(
            [new Record('one', null, 0, null, self::CREATE_TIME, TimestampType::CREATE_TIME)],
            CompressionCodec::NONE,
            Message::MAGIC_V1
        );
        $v2 = RecordBatch::fromRecords([new Record('two')]);

        $region = MemoryRecords::fromBuffer($v0->toBuffer() . $v1->toBuffer() . $v2->toBuffer());

        self::assertSame(3, $region->count());
        self::assertSame(Message::MAGIC_V0, $region->getMagic(), 'the magic of a region is the one of its first batch');
        self::assertSame(
            ['zero', 'one', 'two'],
            array_map(static fn(Record $r): ?string => $r->value, $region->getRecords())
        );
    }

    public function testTheRecordsOfAControlBatchAreNotHandedOut(): void
    {
        $data    = RecordBatch::fromRecords([new Record('alpha'), new Record('bravo')], CompressionCodec::NONE, 0, 4711, 3, 0, true);
        $control = RecordBatch::fromEndTransactionMarker(
            new EndTransactionMarker(ControlRecordType::COMMIT, 0),
            4711,
            3,
            self::CREATE_TIME,
            2
        );

        $region = MemoryRecords::fromBuffer($data->toBuffer() . $control->toBuffer());

        self::assertSame(2, $region->count(), 'the control batch is one of the batches of the region');
        self::assertCount(2, $region->getRecords(), 'but its marker is not one of the records');
        self::assertSame(['alpha', 'bravo'], array_map(static fn(Record $r): ?string => $r->value, $region->getRecords()));

        $batches = $region->getBatches();
        self::assertInstanceOf(RecordBatch::class, $batches[1]);
        self::assertTrue($batches[1]->isControlBatch());
        self::assertCount(1, $batches[1]->getRecords(), 'the batch itself still answers its marker');
    }

    public function testABatchThatTheRegionCutsShortIsDroppedSilently(): void
    {
        $batch  = RecordBatch::fromRecords([new Record('alpha')]);
        $buffer = $batch->toBuffer() . substr($batch->toBuffer(), 0, 40);

        $region = MemoryRecords::fromBuffer($buffer);

        self::assertSame(1, $region->count());
        self::assertTrue($region->hasPartialTrailingRecord());
        self::assertCount(1, $region->getRecords());
    }

    public function testARegionThatEndsInsideTheEntryHeaderIsPartialAsWell(): void
    {
        $region = MemoryRecords::fromBuffer("\x00\x00\x00\x00\x00");

        self::assertTrue($region->hasPartialTrailingRecord());
        self::assertTrue($region->isEmpty());
    }

    public function testAnEntryWithAnImpossibleLengthIsRefused(): void
    {
        $this->expectException(CorruptMessageException::class);
        MemoryRecords::fromBuffer(pack('J', 0) . pack('N', 3) . str_repeat("\x00", 16));
    }

    public function testARegionOfAnUnknownMagicIsRefused(): void
    {
        $buffer                              = RecordBatch::fromRecords([new Record('alpha')])->toBuffer();
        $buffer[MemoryRecords::MAGIC_OFFSET] = "\x07";

        $this->expectException(CorruptMessageException::class);
        MemoryRecords::fromBuffer($buffer);
    }

    public function testASingleBatchIsWrappedForTheProduceSide(): void
    {
        $batch = RecordBatch::fromRecords([new Record('alpha'), new Record('bravo')]);

        $region = MemoryRecords::fromRecordBatch($batch);

        self::assertSame(1, $region->count());
        self::assertCount(2, $region->getRecords());
        self::assertSame(bin2hex($batch->toBuffer()), bin2hex($region->toBuffer()));
        self::assertSame(RecordBatch::MAGIC, $region->getMagic());
    }

    public function testALegacySetIsWrappedForTheProduceSide(): void
    {
        $set = MessageSet::fromRecords([new Record('alpha')], CompressionCodec::NONE, Message::MAGIC_V1);

        $region = MemoryRecords::fromMessageSet($set);

        self::assertSame(1, $region->count());
        self::assertCount(1, $region->getRecords());
        self::assertSame(bin2hex($set->toBuffer()), bin2hex($region->toBuffer()));
        self::assertSame(Message::MAGIC_V1, $region->getMagic());
    }

    public function testAnEmptyMessageSetIsAnEmptyRegion(): void
    {
        $region = MemoryRecords::fromMessageSet(MessageSet::fromRecords([]));

        self::assertTrue($region->isEmpty());
        self::assertSame('', $region->toBuffer());
    }

    public function testTheRegionIsItsOwnBuffer(): void
    {
        $batch = RecordBatch::fromRecords([new Record('alpha')]);

        self::assertSame($batch->toBuffer(), (string) MemoryRecords::fromBuffer($batch->toBuffer()));
    }
}
