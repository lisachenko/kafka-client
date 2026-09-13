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

namespace Protocol\Kafka\Admin;

/**
 * One producer a partition still remembers, as {@see AdminClient::describeProducers()} reports it
 *
 * `ProducerState` of the Java admin client. The state is what `ProducerStateManager` keeps for the idempotent
 * producer: the last sequence number and timestamp of a producer id, and the first offset of a transaction of it
 * that is still open in this partition.
 *
 * @see docs/protocol/2.8.md, section "DescribeProducers API (key 61, v0)"
 */
final class ProducerState
{
    public function __construct(
        public readonly int $producerId,
        public readonly int $producerEpoch,
        public readonly int $lastSequence,
        public readonly int $lastTimestamp,
        public readonly int $coordinatorEpoch,
        public readonly ?int $currentTransactionStartOffset
    ) {}

    /**
     * Returns whether this producer has a transaction open in the partition that reported it
     */
    public function hasOpenTransaction(): bool
    {
        return $this->currentTransactionStartOffset !== null;
    }
}
