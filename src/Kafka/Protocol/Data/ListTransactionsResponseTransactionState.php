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
 * One transaction of a ListTransactions answer (key 66, Kafka 3.0)
 *
 * <pre>
 *   TransactionState => TransactionalId ProducerId TransactionState
 *     TransactionalId  => COMPACT_STRING
 *     ProducerId       => INT64
 *     TransactionState => COMPACT_STRING
 * </pre>
 *
 * `TransactionState` of `ListTransactionsResponse.json` @ 3.0.2 - three fields, where the entry of a
 * {@see DescribeTransactionsResponseTransactionState} has eight: the listing carries no epoch, no timeout, no
 * start time and no partitions, and a caller that needs them asks the coordinator of the id with
 * {@see \Protocol\Kafka\Protocol\Request\DescribeTransactionsRequest}. There is no per-entry error code either -
 * the whole answer of a broker carries one.
 *
 * @see docs/protocol/4.3.md, section "ListTransactions API (key 66, v0 to v2)"
 */
class ListTransactionsResponseTransactionState implements BinarySchemaInterface
{
    /**
     * Transactional id of this transaction
     */
    public string $transactionalId;

    /**
     * Producer id the coordinator handed out for it
     */
    public int $producerId;

    /**
     * Name of the state the coordinator holds it in, one of the names of
     * {@see \Protocol\Kafka\Admin\TransactionState}
     */
    public string $transactionState;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'transactionalId'  => BinarySchema::TYPE_STRING,
            'producerId'       => BinarySchema::TYPE_INT64,
            'transactionState' => BinarySchema::TYPE_STRING,
        ];
    }
}
