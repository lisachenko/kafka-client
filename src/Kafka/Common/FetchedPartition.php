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

use Protocol\Kafka\Common\Record\MessageSet;
use Protocol\Kafka\Common\Record\Record;

/**
 * Everything one partition of a Fetch response says, not only the records it carried.
 *
 * A consumer needs more than the records to drive its fetch loop: the high water mark tells it how far behind the
 * end of the log it is, and a 0.8.2.2 broker can answer without an error and without a single complete message when
 * the message at the fetch offset is larger than the `MaxBytes` that were asked for. Fetching that offset again
 * would spin forever, so {@see FetchedPartition::isSingleMessageTooLarge()} exists to make that state visible
 * without a second round trip to the broker.
 *
 * @see \Protocol\Kafka\Client::fetchPartitions()
 * @see docs/protocol/0.9.0.md, section "Fetch API (key 1, v0)"
 */
final class FetchedPartition
{
    /**
     * @param TopicPartition $topicPartition          Partition this answer belongs to
     * @param int            $fetchOffset             Offset the records were requested from
     * @param int            $errorCode               Error code the broker reported for this partition
     * @param int            $highWaterMarkOffset     Offset at the end of the log of this partition
     * @param MessageSet     $messageSet              Records the broker returned
     * @param bool           $isSingleMessageTooLarge Whether the first message does not fit into the fetch size
     */
    public function __construct(
        public readonly TopicPartition $topicPartition,
        public readonly int $fetchOffset,
        public readonly int $errorCode,
        public readonly int $highWaterMarkOffset,
        private readonly MessageSet $messageSet,
        private readonly bool $isSingleMessageTooLarge = false
    ) {}

    /**
     * Returns the decoded message set of this partition
     */
    public function getMessageSet(): MessageSet
    {
        return $this->messageSet;
    }

    /**
     * Returns the records of this partition, with their offsets filled in
     *
     * @return list<Record>
     */
    public function getRecords(): array
    {
        return $this->messageSet->getRecords();
    }

    /**
     * Checks whether the broker cut the last message of the answer short.
     *
     * That is normal: the broker fills the answer up to `MaxBytes` and does not care about message boundaries, the
     * partial message is simply read again with the next fetch.
     */
    public function hasPartialTrailingMessage(): bool
    {
        return $this->messageSet->hasPartialTrailingMessage();
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
        return $this->messageSet->isEmpty();
    }

    /**
     * Returns how many records this answer carried
     */
    public function count(): int
    {
        return $this->messageSet->count();
    }

    /**
     * Returns the offset the next fetch of this partition has to start at.
     *
     * That is the offset behind the last complete record; a partition that returned nothing keeps its fetch offset.
     */
    public function getNextOffset(): int
    {
        $records = $this->getRecords();
        if ($records === []) {
            return $this->fetchOffset;
        }

        $lastRecord = $records[count($records) - 1];

        return ($lastRecord->offset ?? $this->fetchOffset) + 1;
    }
}
