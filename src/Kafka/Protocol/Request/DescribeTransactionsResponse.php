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

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\DescribeTransactionsResponseTransactionState;

/**
 * DescribeTransactions response object, version 0 (key 65, Kafka 3.0)
 *
 * <pre>
 *   DescribeTransactions Response (Version: 0) => throttle_time_ms [transaction_states]
 *     throttle_time_ms  => INT32
 *     transaction_states => error_code transactional_id transaction_state transaction_timeout_ms
 *                           transaction_start_time_ms producer_id producer_epoch [topics]
 * </pre>
 *
 * **There is no top-level error code**: every transactional id of the request carries one of its own, exactly as
 * every partition of a {@see DescribeProducersResponse} does. An id the coordinator has no state for is the code
 * **105** (`TransactionalIdNotFound`), which is the error code Kafka 3.0 added for this api.
 *
 * @see docs/protocol/4.3.md, section "DescribeTransactions API (key 65, v0)"
 */
class DescribeTransactionsResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas
     */
    public int $throttleTimeMs = 0;

    /**
     * State of every transactional id of the request, indexed by that id
     *
     * @var array<string, DescribeTransactionsResponseTransactionState>
     */
    public array $transactionStates = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'throttleTimeMs'    => BinarySchema::TYPE_INT32,
            'transactionStates' => ['transactionalId' => DescribeTransactionsResponseTransactionState::class],
        ];
    }
}
