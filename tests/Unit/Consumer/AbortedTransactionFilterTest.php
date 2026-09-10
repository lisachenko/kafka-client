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

namespace Protocol\Kafka\Tests\Unit\Consumer;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Common\FetchedPartition;
use Protocol\Kafka\Common\Record\ControlRecordType;
use Protocol\Kafka\Common\Record\EndTransactionMarker;
use Protocol\Kafka\Common\Record\MemoryRecords;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Common\TopicPartition;
use Protocol\Kafka\Consumer\Internals\AbortedTransactionFilter;
use Protocol\Kafka\Protocol\Data\FetchResponseAbortedTransaction;

/**
 * The `read_committed` filter of the fetch loop, `Fetcher.PartitionRecords.nextFetchedRecord()` @ 0.11.0.3.
 *
 * The regions below are built here rather than captured, because what has to be exercised is the *algorithm* -
 * several transactions of several producers interleaved in one answer - and no single fetch of the container
 * produces all of those shapes. That the broker really answers a `read_committed` fetch with the aborted records
 * and the marker in it is the wire vector `fetch.response.v5.aborted-transactions` and the integration test
 * `ReadCommittedConsumerTest`.
 *
 * @see docs/protocol/1.1.md, section "Transactions"
 */
#[CoversClass(AbortedTransactionFilter::class)]
final class AbortedTransactionFilterTest extends TestCase
{
    private const string TOPIC = 'transactions';

    private const int TIMESTAMP = 1600000000000;

    public function testAPartitionWithoutATransactionIsHandedThroughUnchanged(): void
    {
        $partition = $this->partition([$this->dataBatch(0, RecordBatch::NO_PRODUCER_ID, false, ['one', 'two'])]);

        self::assertSame(
            ['one', 'two'],
            $this->valuesOf(AbortedTransactionFilter::committedRecords($partition))
        );
    }

    public function testACommittedTransactionKeepsItsRecordsAndLosesItsMarker(): void
    {
        $partition = $this->partition(
            [
                $this->dataBatch(0, 42, true, ['one', 'two']),
                $this->controlBatch(2, 42, ControlRecordType::COMMIT),
            ],
            []
        );

        self::assertSame(['one', 'two'], $this->valuesOf(AbortedTransactionFilter::committedRecords($partition)));
    }

    public function testAnAbortedTransactionLosesItsRecordsAndItsMarker(): void
    {
        $partition = $this->partition(
            [
                $this->dataBatch(0, 42, true, ['one', 'two']),
                $this->controlBatch(2, 42, ControlRecordType::ABORT),
            ],
            [new FetchResponseAbortedTransaction(42, 0)]
        );

        self::assertSame([], AbortedTransactionFilter::committedRecords($partition));
    }

    public function testTheSameProducerMayCommitAfterItAborted(): void
    {
        // The ABORT marker ends the aborted range of a producer id, so its next transaction is kept
        $partition = $this->partition(
            [
                $this->dataBatch(0, 42, true, ['rolled back']),
                $this->controlBatch(1, 42, ControlRecordType::ABORT),
                $this->dataBatch(2, 42, true, ['kept']),
                $this->controlBatch(3, 42, ControlRecordType::COMMIT),
            ],
            [new FetchResponseAbortedTransaction(42, 0)]
        );

        self::assertSame(['kept'], $this->valuesOf(AbortedTransactionFilter::committedRecords($partition)));
    }

    public function testTwoProducersAreFilteredIndependently(): void
    {
        $partition = $this->partition(
            [
                $this->dataBatch(0, 42, true, ['aborted by 42']),
                $this->dataBatch(1, 43, true, ['committed by 43']),
                $this->controlBatch(2, 42, ControlRecordType::ABORT),
                $this->controlBatch(3, 43, ControlRecordType::COMMIT),
            ],
            [new FetchResponseAbortedTransaction(42, 0)]
        );

        self::assertSame(
            ['committed by 43'],
            $this->valuesOf(AbortedTransactionFilter::committedRecords($partition))
        );
    }

    public function testAnAbortedTransactionThatStartsLaterDoesNotSwallowWhatCameBeforeIt(): void
    {
        // The aborted transaction of the producer 42 begins at the offset 2, so its own earlier batch is committed
        $partition = $this->partition(
            [
                $this->dataBatch(0, 42, true, ['committed']),
                $this->controlBatch(1, 42, ControlRecordType::COMMIT),
                $this->dataBatch(2, 42, true, ['aborted']),
                $this->controlBatch(3, 42, ControlRecordType::ABORT),
            ],
            [new FetchResponseAbortedTransaction(42, 2)]
        );

        self::assertSame(['committed'], $this->valuesOf(AbortedTransactionFilter::committedRecords($partition)));
    }

    public function testTheAbortedTransactionsAreConsumedInOffsetOrderWhateverOrderTheyArriveIn(): void
    {
        $partition = $this->partition(
            [
                $this->dataBatch(0, 42, true, ['first aborted']),
                $this->controlBatch(1, 42, ControlRecordType::ABORT),
                $this->dataBatch(2, 43, true, ['second aborted']),
                $this->controlBatch(3, 43, ControlRecordType::ABORT),
            ],
            // Deliberately not in offset order, as a broker is not required to sort them
            [new FetchResponseAbortedTransaction(43, 2), new FetchResponseAbortedTransaction(42, 0)]
        );

        self::assertSame([], AbortedTransactionFilter::committedRecords($partition));
    }

    public function testANonTransactionalBatchNextToAnAbortedOneSurvives(): void
    {
        $partition = $this->partition(
            [
                $this->dataBatch(0, 42, true, ['aborted']),
                $this->controlBatch(1, 42, ControlRecordType::ABORT),
                $this->dataBatch(2, RecordBatch::NO_PRODUCER_ID, false, ['plain']),
            ],
            [new FetchResponseAbortedTransaction(42, 0)]
        );

        self::assertSame(['plain'], $this->valuesOf(AbortedTransactionFilter::committedRecords($partition)));
    }

    public function testAnEmptyAbortedTransactionsArrayMeansEverythingIsCommitted(): void
    {
        // What a `read_committed` fetch of a partition without an aborted transaction answers: an empty array
        $partition = $this->partition(
            [
                $this->dataBatch(0, 42, true, ['one']),
                $this->controlBatch(1, 42, ControlRecordType::COMMIT),
            ],
            []
        );

        self::assertSame(['one'], $this->valuesOf(AbortedTransactionFilter::committedRecords($partition)));
    }

    /**
     * @param list<RecordBatch>                          $batches
     * @param list<FetchResponseAbortedTransaction>|null $abortedTransactions
     */
    private function partition(array $batches, ?array $abortedTransactions = null): FetchedPartition
    {
        $buffer = '';
        foreach ($batches as $batch) {
            $buffer .= $batch->toBuffer();
        }

        return new FetchedPartition(
            new TopicPartition(self::TOPIC, 0),
            0,
            0,
            count($batches),
            MemoryRecords::fromBuffer($buffer),
            false,
            0,
            count($batches),
            0,
            $abortedTransactions
        );
    }

    /**
     * @param list<string> $values
     */
    private function dataBatch(int $baseOffset, int $producerId, bool $transactional, array $values): RecordBatch
    {
        $records = array_map(
            static fn(string $value): Record => new Record($value)->withCreateTime(self::TIMESTAMP),
            $values
        );

        return RecordBatch::fromRecords($records, 0, $baseOffset, $producerId, 0, 0, $transactional);
    }

    private function controlBatch(int $offset, int $producerId, int $type): RecordBatch
    {
        return RecordBatch::fromEndTransactionMarker(
            new EndTransactionMarker($type),
            $producerId,
            0,
            self::TIMESTAMP,
            $offset
        );
    }

    /**
     * @param list<Record> $records
     *
     * @return list<string|null>
     */
    private function valuesOf(array $records): array
    {
        return array_map(static fn(Record $record): ?string => $record->value, $records);
    }
}
