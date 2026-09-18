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
use Protocol\Kafka\Protocol\Data\ListTransactionsResponseTransactionState;

/**
 * ListTransactions response object, version 1 (key 66, Kafka 3.0)
 *
 * <pre>
 *   ListTransactions Response (Version: 0 to 1) => throttle_time_ms error_code [unknown_state_filters]
 *                                             [transaction_states]
 *     throttle_time_ms      => INT32
 *     error_code            => INT16
 *     unknown_state_filters => COMPACT_STRING
 *     transaction_states    => transactional_id producer_id transaction_state
 * </pre>
 *
 * Unlike {@see DescribeTransactionsResponse} this api has a **top-level error code**, and it stands *behind* the
 * throttle time: the whole answer of a broker fails together (`COORDINATOR_LOAD_IN_PROGRESS` while a
 * `__transaction_state` partition is being read, `COORDINATOR_NOT_AVAILABLE` while the coordinator is stopping),
 * because the entries are not requested one by one.
 *
 * `unknown_state_filters` names the state filters of the request the coordinator could not resolve
 * (`TransactionState.fromName` @ 3.9.2 returned nothing for them). It is a *report*, not an error: the error code
 * stays 0, and the filter is applied with the states it did understand.
 *
 * **Version 1 (Kafka 3.8, KIP-994) writes the same bytes as version 0** - "Version 1 is the same as version 0
 * (KIP-994)" says `ListTransactionsResponse.json` @ 3.8.1 - because the `duration_filter` of the request changed
 * what the coordinator selects, not what it reports; {@see ListTransactionsResponseV0} is that identical frame
 * read at the version below.
 *
 * @see docs/protocol/3.9.md, section "ListTransactions API (key 66, v0 and v1)"
 */
class ListTransactionsResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas
     */
    public int $throttleTimeMs = 0;

    /**
     * Error of the whole answer, 0 when the broker could list its transactions
     */
    public int $errorCode = 0;

    /**
     * State names of the request that this coordinator does not know
     *
     * @var list<string>
     */
    public array $unknownStateFilters = [];

    /**
     * Transactions of this broker that passed the filters, indexed by the transactional id
     *
     * @var array<string, ListTransactionsResponseTransactionState>
     */
    public array $transactionStates = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'throttleTimeMs'      => BinarySchema::TYPE_INT32,
            'errorCode'           => BinarySchema::TYPE_INT16,
            'unknownStateFilters' => [BinarySchema::TYPE_STRING],
            'transactionStates'   => ['transactionalId' => ListTransactionsResponseTransactionState::class],
        ];
    }
}
