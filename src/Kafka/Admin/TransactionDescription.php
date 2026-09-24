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

use Protocol\Kafka\Common\TopicPartition;

/**
 * The state of one transactional id, as {@see AdminClient::describeTransactions()} reports it
 *
 * `TransactionDescription` of the Java admin client, with the same fields: the broker that coordinates the id,
 * the state, the producer id and epoch the coordinator handed out, the transaction timeout of the producer, the
 * moment the current transaction started, and the partitions that belong to it.
 *
 * `transactionStartTimeMs` is **null** while no transaction of this id is in flight - the wire carries the -1 of
 * `TransactionMetadata.txnStartTimestamp` then, which the Java client models as an empty `OptionalLong`.
 *
 * @see docs/protocol/4.3.md, section "DescribeTransactions API (key 65, v0)"
 */
final class TransactionDescription
{
    /**
     * @param int                 $coordinatorId          Node id of the broker that coordinates this id
     * @param TransactionState    $state                  State the coordinator holds the id in
     * @param int                 $producerId             Producer id of this transactional id
     * @param int                 $producerEpoch          Epoch of that producer id
     * @param int                 $transactionTimeoutMs   `transaction.timeout.ms` of the producer that owns it
     * @param int|null            $transactionStartTimeMs Start of the current transaction, null without one
     * @param list<TopicPartition> $topicPartitions       Partitions of the current transaction
     */
    public function __construct(
        public readonly int $coordinatorId,
        public readonly TransactionState $state,
        public readonly int $producerId,
        public readonly int $producerEpoch,
        public readonly int $transactionTimeoutMs,
        public readonly ?int $transactionStartTimeMs,
        public readonly array $topicPartitions
    ) {}

    /**
     * Returns whether a transaction of this id is in flight, markers included
     */
    public function hasOpenTransaction(): bool
    {
        return $this->state->isInFlight();
    }
}
