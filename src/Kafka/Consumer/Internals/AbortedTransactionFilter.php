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

namespace Protocol\Kafka\Consumer\Internals;

use Protocol\Kafka\Common\FetchedPartition;
use Protocol\Kafka\Common\Record\ControlRecordType;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Protocol\Data\FetchResponseAbortedTransaction;

/**
 * Drops the records of aborted transactions out of a `read_committed` fetch answer.
 *
 * **The broker does not do this.** A `read_committed` Fetch (isolation level 1) is bounded by the *last stable
 * offset* of a partition, which is the only thing the broker guarantees: nothing above the first offset of a
 * transaction that is still open ever reaches the answer. Everything below it does, including the records of
 * transactions that were **aborted** and the ABORT control batches that mark them - verified against the 0.11.0.3
 * container, where a `read_committed` fetch of a partition with one aborted transaction of two records answers
 * both records and the marker. What the broker adds is the `aborted_transactions` array of the answer, one
 * `(producer_id, first_offset)` pair per aborted transaction that overlaps the range it returned, and the client
 * is expected to do the filtering with it.
 *
 * This is `Fetcher.PartitionRecords.nextFetchedRecord()` @ 0.11.0.3, walking the **batches** of the answer in the
 * order of the log:
 *
 * 1. the aborted transactions are kept sorted by their first offset; every one whose first offset is at or below
 *    the **last** offset of the current batch is taken off the list and its producer id is remembered as aborted;
 * 2. a **control batch** never reaches an application - it holds the marker and nothing else - and one that holds
 *    an {@see ControlRecordType::ABORT} marker also *ends* the aborted transaction of its producer, so that
 *    producer id is forgotten again and the batches it writes afterwards are kept;
 * 3. every other batch whose producer id is in the aborted set is skipped whole - a transaction is aborted or
 *    committed as a unit, never record by record;
 * 4. what is left are the records an application may see.
 *
 * The order matters: a producer id is only "aborted" between the transaction the array named and the ABORT marker
 * that closes it, so the same producer id may write a committed transaction right after an aborted one and both
 * are treated correctly.
 *
 * A batch of the legacy message formats v0 and v1 has no producer id at all and is always kept: transactions do
 * not exist below the message format v2.
 *
 * @see docs/protocol/1.1.md, section "Transactions"
 */
final class AbortedTransactionFilter
{
    /**
     * This class is a stateless algorithm and is never instantiated
     */
    private function __construct() {}

    /**
     * Returns the records of a fetched partition that a `read_committed` consumer is allowed to see
     *
     * @return list<Record>
     */
    public static function committedRecords(FetchedPartition $partition): array
    {
        $aborted = self::sortedByFirstOffset($partition->abortedTransactions ?? []);
        if ($aborted === [] && !self::holdsATransactionalBatch($partition)) {
            return $partition->getRecords();
        }

        $abortedProducerIds = [];
        $records            = [];

        foreach ($partition->getMemoryRecords()->getBatches() as $batch) {
            if (!$batch instanceof RecordBatch) {
                // A legacy message set: the message format v2 is the first one that knows a transaction at all
                foreach ($batch->getRecords() as $record) {
                    $records[] = $record;
                }
                continue;
            }

            if ($batch->getProducerId() !== RecordBatch::NO_PRODUCER_ID) {
                while ($aborted !== [] && $aborted[0]->firstOffset <= $batch->getLastOffset()) {
                    $abortedProducerIds[array_shift($aborted)->producerId] = true;
                }

                if (self::containsAbortMarker($batch)) {
                    unset($abortedProducerIds[$batch->getProducerId()]);
                } elseif (isset($abortedProducerIds[$batch->getProducerId()])) {
                    continue;
                }
            }

            // A control batch is a marker of the transaction protocol and never reaches an application
            if ($batch->isControlBatch()) {
                continue;
            }

            foreach ($batch->getRecords() as $record) {
                $records[] = $record;
            }
        }

        return $records;
    }

    /**
     * Tells whether a partition holds anything the filter could act on at all
     */
    private static function holdsATransactionalBatch(FetchedPartition $partition): bool
    {
        foreach ($partition->getMemoryRecords()->getBatches() as $batch) {
            if ($batch instanceof RecordBatch && $batch->isTransactional()) {
                return true;
            }
        }

        return false;
    }

    /**
     * Returns the aborted transactions of an answer in the order of their first offset
     *
     * @param list<FetchResponseAbortedTransaction> $abortedTransactions
     *
     * @return list<FetchResponseAbortedTransaction>
     */
    private static function sortedByFirstOffset(array $abortedTransactions): array
    {
        usort(
            $abortedTransactions,
            static fn(FetchResponseAbortedTransaction $left, FetchResponseAbortedTransaction $right): int
                => $left->firstOffset <=> $right->firstOffset
        );

        return $abortedTransactions;
    }

    /**
     * Tells whether a control batch is the ABORT marker of its transaction
     *
     * The marker is the **key** of the single record of the batch, an int16 version and an int16 type; a control
     * batch without a record is a corrupt one, which is why the Java client throws there - here it simply is not a
     * marker, and the batch is dropped like every other control batch.
     */
    private static function containsAbortMarker(RecordBatch $batch): bool
    {
        if (!$batch->isControlBatch()) {
            return false;
        }

        $records = $batch->getRecords();

        return $records !== [] && ControlRecordType::parse($records[0]->key) === ControlRecordType::ABORT;
    }
}
