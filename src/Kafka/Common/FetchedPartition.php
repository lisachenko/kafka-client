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

namespace Protocol\Kafka\Common;

use Protocol\Kafka\Common\Record\MemoryRecords;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Protocol\Data\FetchResponseAbortedTransaction;
use Protocol\Kafka\Protocol\Data\FetchResponsePartition;

/**
 * Everything one partition of a Fetch response says, not only the records it carried.
 *
 * A consumer needs more than the records to drive its fetch loop: the high water mark tells it how far behind the
 * end of the log it is, and a broker can answer without an error and without a single complete message when the
 * message at the fetch offset is larger than the `MaxBytes` that were asked for. Fetching that offset again would
 * spin forever, so {@see FetchedPartition::isSingleMessageTooLarge()} exists to make that state visible without a
 * second round trip to the broker.
 *
 * The records of a partition carry the timestamp and the {@see \Protocol\Kafka\Common\Record\TimestampType} of
 * the message format the answer was in, and their headers only exist in the message format v2. A Fetch request
 * below version 4 makes the broker convert its answer down - to message format v1 below version 4, to format v0
 * below version 2 - so a record read that way carries no headers at all, and one read below version 2 no timestamp
 * either, whatever the log itself holds.
 *
 * Version 1 of the Fetch API (Kafka 0.9) added the throttle time of the answer, which every partition of that answer
 * carries here; it is 0 unless the client exceeded a fetch quota of the broker. A `consumer_byte_rate` quota does
 * not shorten an answer and never fails a fetch: the broker holds the complete answer back for that many
 * milliseconds, so a fetch loop that ignores {@see FetchedPartition::$throttleTimeMs} still reads everything, it
 * only waits longer.
 *
 * The versions 4 and 5 (Kafka 0.11) added the three values of the transactional protocol and of KIP-107:
 * {@see FetchedPartition::$lastStableOffset}, {@see FetchedPartition::$abortedTransactions} and
 * {@see FetchedPartition::$logStartOffset}.
 *
 * @see \Protocol\Kafka\Client::fetchPartitions()
 * @see docs/protocol/2.8.md, sections "Fetch API (key 1, v0 to v12)" and "Quotas and throttle time"
 */
final class FetchedPartition
{
    /**
     * @param TopicPartition $topicPartition          Partition this answer belongs to
     * @param int            $fetchOffset             Offset the records were requested from
     * @param int            $errorCode               Error code the broker reported for this partition
     * @param int            $highWaterMarkOffset     Offset at the end of the log of this partition
     * @param MemoryRecords  $records                 Record region the broker returned, in whichever message
     *                                                format it answered with
     * @param bool           $isSingleMessageTooLarge Whether the first message does not fit into the fetch size
     * @param int            $throttleTimeMs          Milliseconds the broker delayed the answer because of a quota
     * @param int            $lastStableOffset        Last stable offset of the partition, the offset below which
     *                                                every transaction has been decided. Answered by a
     *                                                `read_committed` fetch of version 4 and above; a
     *                                                `read_uncommitted` one is answered with -1, and so is every
     *                                                version below 4. Without a single transaction on the
     *                                                partition it equals the high water mark.
     * @param int            $logStartOffset          Earliest offset that is still on disk, since version 5; -1
     *                                                when the answer did not carry the field
     * @param list<FetchResponseAbortedTransaction>|null $abortedTransactions Transactions that were aborted in the
     *                                                range this answer covers, `null` for a `read_uncommitted`
     *                                                fetch and for every version below 4. The empty array means
     *                                                "asked, and nothing was aborted here"; the records of an
     *                                                aborted transaction are still in the answer, dropping them is
     *                                                the job of a `read_committed` consumer.
     */
    public function __construct(
        public readonly TopicPartition $topicPartition,
        public readonly int $fetchOffset,
        public readonly int $errorCode,
        public readonly int $highWaterMarkOffset,
        private readonly MemoryRecords $records,
        private readonly bool $isSingleMessageTooLarge = false,
        public readonly int $throttleTimeMs = 0,
        public readonly int $lastStableOffset = FetchResponsePartition::INVALID_LAST_STABLE_OFFSET,
        public readonly int $logStartOffset = FetchResponsePartition::INVALID_LOG_START_OFFSET,
        public readonly ?array $abortedTransactions = null,
        /**
         * Replica this partition should be read from next, `-1` for "the leader itself" (KIP-392, Kafka 2.3).
         *
         * The leader answers it in every partition entry of a Fetch **v11** and it is the whole client-facing
         * half of KIP-392: a consumer that named its rack in
         * {@see \Protocol\Kafka\Consumer\ConsumerConfig::CLIENT_RACK} is told which broker to read this
         * partition from, and reads from it until an answer names another one.
         * {@see FetchResponsePartition::NO_PREFERRED_READ_REPLICA} is what a broker without a
         * `replica.selector.class` - and every answer below version 11 - reports.
         */
        public readonly int $preferredReadReplica = FetchResponsePartition::NO_PREFERRED_READ_REPLICA,
    ) {}

    /**
     * Returns the decoded record region of this partition, batches and all
     */
    public function getMemoryRecords(): MemoryRecords
    {
        return $this->records;
    }

    /**
     * Returns the records of this partition, with their offsets filled in.
     *
     * The markers of a control batch are not among them - they are part of the transaction protocol and never
     * reach an application, see {@see MemoryRecords::getRecords()}.
     *
     * @return list<Record>
     */
    public function getRecords(): array
    {
        return $this->records->getRecords();
    }

    /**
     * Checks whether the broker cut the last batch of the answer short.
     *
     * That is normal: the broker fills the answer up to `MaxBytes` and does not care about batch boundaries, the
     * partial batch is simply read again with the next fetch.
     */
    public function hasPartialTrailingRecord(): bool
    {
        return $this->records->hasPartialTrailingRecord();
    }

    /**
     * Checks whether the message at the fetch offset is larger than the requested fetch size.
     *
     * The partition carries no complete message although the log has more to offer, so fetching the same offset
     * again would return the very same answer: `max.partition.fetch.bytes` has to be raised instead.
     */
    public function isSingleMessageTooLarge(): bool
    {
        return $this->isSingleMessageTooLarge;
    }

    /**
     * Checks whether this answer carried no complete record at all
     */
    public function isEmpty(): bool
    {
        return $this->records->isEmpty();
    }

    /**
     * Returns how many records this answer carried, the control markers of a transaction not among them
     */
    public function count(): int
    {
        return count($this->getRecords());
    }

    /**
     * Returns the offset the next fetch of this partition has to start at.
     *
     * That is the offset behind the last complete record of the answer. A batch whose records an application never
     * sees still moves the fetch position: the **control batch** of a transaction holds nothing but a marker, and a
     * consumer that stopped at it would ask for the same offset forever, so the last offset of the last batch is
     * what counts and the records are only asked for a legacy message set, which knows no batch offsets. A
     * partition that returned nothing at all keeps its fetch offset.
     */
    public function getNextOffset(): int
    {
        $batches   = $this->records->getBatches();
        $lastBatch = $batches === [] ? null : $batches[count($batches) - 1];
        if ($lastBatch instanceof RecordBatch) {
            return $lastBatch->baseOffset + $lastBatch->lastOffsetDelta + 1;
        }

        $records = $this->getRecords();
        if ($records === []) {
            return $this->fetchOffset;
        }

        $lastRecord = $records[count($records) - 1];

        return ($lastRecord->offset ?? $this->fetchOffset) + 1;
    }
}
