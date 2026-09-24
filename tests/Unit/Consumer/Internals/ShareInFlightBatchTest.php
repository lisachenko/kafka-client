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

namespace Protocol\Kafka\Tests\Unit\Consumer\Internals;

use LogicException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Consumer\AcknowledgeType;
use Protocol\Kafka\Consumer\ConsumerRecord;
use Protocol\Kafka\Consumer\Internals\ShareInFlightBatch;
use Protocol\Kafka\Protocol\Data\ShareAcknowledgementBatch;

/**
 * The records of one topic-partition a share consumer holds (KIP-932): what the application said about them, how
 * that travels as acknowledgement batches, and the renewals of KIP-1222.
 *
 * @see docs/protocol/4.3.md, section "The share consumer (KIP-932)"
 */
#[CoversClass(ShareInFlightBatch::class)]
final class ShareInFlightBatchTest extends TestCase
{
    public function testRunsOfOneTypeBecomeOneBatchAndEveryChangeStartsANewOne(): void
    {
        $batches = ShareInFlightBatch::acknowledgementBatches([
            5 => ShareAcknowledgementBatch::ACCEPT,
            0 => ShareAcknowledgementBatch::ACCEPT,
            1 => ShareAcknowledgementBatch::ACCEPT,
            2 => ShareAcknowledgementBatch::RELEASE,
            3 => ShareAcknowledgementBatch::GAP,
            4 => ShareAcknowledgementBatch::GAP,
            7 => ShareAcknowledgementBatch::ACCEPT,
        ]);

        self::assertSame(
            [[0, 1, [1]], [2, 2, [2]], [3, 4, [0]], [5, 5, [1]], [7, 7, [1]]],
            array_map(
                static fn(ShareAcknowledgementBatch $batch): array => [$batch->firstOffset, $batch->lastOffset, $batch->acknowledgeTypes],
                $batches
            ),
            'a hole in the offsets ends a batch as well'
        );
        self::assertSame([], ShareInFlightBatch::acknowledgementBatches([]));
    }

    public function testTheImplicitAcceptanceKeepsWhatWasAcknowledgedAlready(): void
    {
        $batch = self::batchOf(0, 1, 2);
        $batch->acknowledge(1, AcknowledgeType::REJECT);
        self::assertFalse($batch->allInFlightAcknowledged());

        $batch->acknowledgeAll(AcknowledgeType::ACCEPT);

        self::assertTrue($batch->allInFlightAcknowledged());
        self::assertSame([0 => 1, 1 => 3, 2 => 1], $batch->takeAcknowledgements());
        self::assertSame([], $batch->inFlightRecords(), 'acknowledged records leave the flight');
        self::assertTrue($batch->isEmpty());
    }

    public function testARecordThatIsNotInFlightCannotBeAcknowledged(): void
    {
        $batch = self::batchOf(0);
        $batch->acknowledge(0, AcknowledgeType::ACCEPT);
        $batch->acknowledge(0, AcknowledgeType::RELEASE);
        self::assertSame([0 => 2], $batch->takeAcknowledgements(), 'the last acknowledgement before the send wins');

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('The record cannot be acknowledged.');
        $batch->acknowledge(0, AcknowledgeType::ACCEPT);
    }

    public function testARenewedRecordIsHeldUntilThePollThatReturnsItAgain(): void
    {
        $batch = self::batchOf(0, 1);
        $batch->acknowledge(0, AcknowledgeType::RENEW);
        $batch->acknowledge(1, AcknowledgeType::ACCEPT);

        self::assertTrue(ShareInFlightBatch::hasRenewAcknowledgement($batch->takeAcknowledgements()));
        self::assertTrue($batch->hasRenewals());
        self::assertSame([], $batch->inFlightRecords(), 'the renewal is on the wire');
        self::assertFalse($batch->isEmpty());

        $batch->completeRenewals(true);
        self::assertSame([], $batch->inFlightRecords(), 'held for the next poll()');
        self::assertSame([0], array_map(static fn(ConsumerRecord $record): ?int => $record->offset, $batch->takeRenewedRecords()));
        self::assertSame([0], array_map(static fn(ConsumerRecord $record): ?int => $record->offset, $batch->inFlightRecords()));
        self::assertFalse($batch->hasRenewals());
    }

    public function testARefusedRenewalEndsTheDelivery(): void
    {
        $batch = self::batchOf(0);
        $batch->acknowledge(0, AcknowledgeType::RENEW);
        $batch->takeAcknowledgements();

        $batch->completeRenewals(false);

        self::assertSame([], $batch->takeRenewedRecords());
        self::assertTrue($batch->isEmpty());
    }

    public function testGapsAndFailedRecordsAreAcknowledgedByTheConsumerItself(): void
    {
        $batch = self::batchOf(0);
        $batch->addGap(1);
        $batch->addFailedRecord(2);

        self::assertFalse($batch->isAcknowledgeable(1), 'a gap is no record');
        self::assertTrue($batch->isAcknowledgeable(2), 'a record a deserializer failed on may be acknowledged by offset');
        $batch->acknowledgeOffset(2, AcknowledgeType::REJECT);
        $batch->acknowledgeAll(AcknowledgeType::ACCEPT);

        self::assertSame([0 => 1, 1 => 0, 2 => 3], $batch->takeAcknowledgements());
        self::assertFalse($batch->isAcknowledgeable(2), 'sent');
    }

    private static function batchOf(int ...$offsets): ShareInFlightBatch
    {
        $batch = new ShareInFlightBatch(1, 'orders', str_repeat("\x01", 16), 0);
        foreach ($offsets as $offset) {
            $batch->addRecord(new ConsumerRecord('orders', 0, "v{$offset}", null, 0, $offset, deliveryCount: 1));
        }

        return $batch;
    }
}
