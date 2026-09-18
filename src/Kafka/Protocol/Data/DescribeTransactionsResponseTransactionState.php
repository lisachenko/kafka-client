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

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * The state of one transactional id in a DescribeTransactions answer (key 65, Kafka 3.0)
 *
 * <pre>
 *   TransactionState => ErrorCode TransactionalId TransactionState TransactionTimeoutMs TransactionStartTimeMs
 *                       ProducerId ProducerEpoch Topics
 *     ErrorCode              => INT16
 *     TransactionalId        => COMPACT_STRING
 *     TransactionState       => COMPACT_STRING   (`Empty`, `Ongoing`, `PrepareCommit`, …)
 *     TransactionTimeoutMs   => INT32
 *     TransactionStartTimeMs => INT64            (-1 while no transaction of this id has started)
 *     ProducerId             => INT64
 *     ProducerEpoch          => INT16
 *     Topics                 => COMPACT_ARRAY of {@see DescribeTransactionsResponseTopic}
 * </pre>
 *
 * `TransactionState` of `DescribeTransactionsResponse.json` @ 3.0.2. An entry whose `error_code` is not 0 carries
 * the transactional id and **nothing else** - the defaults of the specification, i.e. the empty state name, the
 * timeout 0, the start time 0, the producer id 0 and the epoch 0 - so the code has to be read before the numbers.
 *
 * The `topics` array is the set of partitions the coordinator has added to the **current** transaction; a
 * transaction that is preparing to commit or abort lists only the partitions whose marker is still missing, and a
 * transactional id between two transactions lists none at all.
 *
 * **`producer_epoch` is an int16 here**, unlike the int32 of {@see ProducerState} in a DescribeProducers answer.
 *
 * @see docs/protocol/3.9.md, section "DescribeTransactions API (key 65, v0)"
 */
class DescribeTransactionsResponseTransactionState implements BinarySchemaInterface
{
    /**
     * Error of this transactional id, 0 when its state could be read
     */
    public int $errorCode;

    /**
     * Transactional id this entry belongs to, the one field that is filled even for an error
     */
    public string $transactionalId;

    /**
     * Name of the state the coordinator holds this id in, one of the names of
     * {@see \Protocol\Kafka\Admin\TransactionState}
     */
    public string $transactionState = '';

    /**
     * Transaction timeout of this id in milliseconds, the `transaction.timeout.ms` of the producer that owns it
     */
    public int $transactionTimeoutMs = 0;

    /**
     * Wall-clock time the current transaction started at, -1 while none has ever started
     */
    public int $transactionStartTimeMs = -1;

    /**
     * Producer id the coordinator handed out for this transactional id
     */
    public int $producerId = -1;

    /**
     * Epoch of that producer id
     */
    public int $producerEpoch = -1;

    /**
     * Topics of the current transaction, indexed by the topic name
     *
     * @var array<string, DescribeTransactionsResponseTopic>
     */
    public array $topics = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'errorCode'              => BinarySchema::TYPE_INT16,
            'transactionalId'        => BinarySchema::TYPE_STRING,
            'transactionState'       => BinarySchema::TYPE_STRING,
            'transactionTimeoutMs'   => BinarySchema::TYPE_INT32,
            'transactionStartTimeMs' => BinarySchema::TYPE_INT64,
            'producerId'             => BinarySchema::TYPE_INT64,
            'producerEpoch'          => BinarySchema::TYPE_INT16,
            'topics'                 => ['topic' => DescribeTransactionsResponseTopic::class],
        ];
    }
}
