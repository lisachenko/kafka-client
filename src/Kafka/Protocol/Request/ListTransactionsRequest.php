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

use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;

/**
 * ListTransactions, version 0: the transactional ids a broker coordinates (ApiKey 66, Kafka 3.0)
 *
 * <pre>
 *   ListTransactions Request (Version: 0) => [state_filters] [producer_id_filters]
 *     state_filters       => COMPACT_STRING
 *     producer_id_filters => INT64
 * </pre>
 *
 * `ListTransactionsRequest.json` @ 3.0.2 declares the two filters, both flexible from the version 0. An **empty**
 * filter is "everything": `TransactionStateManager.listTransactionStates` @ 3.9.2 only applies a filter that is
 * non-empty, and the two are combined with AND when both are given. A state name the coordinator does not know is
 * not an error - it comes back in the `unknown_state_filters` of the answer, and because the filter itself stays
 * non-empty, a request that names **only** unknown states is answered with an empty list.
 *
 * **Every broker answers for itself**: the api lists the transactional ids of the `__transaction_state`
 * partitions the broker coordinates, so a client that wants the transactions of the whole cluster asks every
 * broker, as it does with {@see ListGroupsRequest}. The `Dead` state is never listed - it is the transient state
 * of an id whose metadata is being expired.
 *
 * **Version 1** (Kafka 3.8, KIP-994) adds a `duration_filter` and is not part of this milestone.
 *
 * @see docs/protocol/3.9.md, section "ListTransactions API (key 66, v0)"
 */
class ListTransactionsRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::LIST_TRANSACTIONS;

    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * @param list<string> $stateFilters      States to list, empty for every state
     * @param list<int>    $producerIdFilters Producer ids to list, empty for every producer
     * @param string       $clientId          A user specified identifier for the client
     * @param int          $correlationId     A value the broker passes back unmodified
     */
    public function __construct(
        /**
         * States the answer is bounded to, empty for every state.
         *
         * @var list<string>
         */
        protected readonly array $stateFilters = [],
        /**
         * Producer ids the answer is bounded to, empty for every producer id.
         *
         * @var list<int>
         */
        protected readonly array $producerIdFilters = [],
        string $clientId = '',
        int $correlationId = 0
    ) {
        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'stateFilters'      => [BinarySchema::TYPE_STRING],
            'producerIdFilters' => [BinarySchema::TYPE_INT64],
        ];
    }

    /**
     * Returns the states this request bounds the answer to, empty for every state
     *
     * @return list<string>
     */
    public function getStateFilters(): array
    {
        return $this->stateFilters;
    }

    /**
     * Returns the producer ids this request bounds the answer to, empty for every producer id
     *
     * @return list<int>
     */
    public function getProducerIdFilters(): array
    {
        return $this->producerIdFilters;
    }
}
