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
 * ListTransactions, version 2: the transactional ids a broker coordinates (ApiKey 66, Kafka 3.0)
 *
 * <pre>
 *   ListTransactions Request (Version: 0 to 2) => [state_filters] [producer_id_filters] duration_filter
 *                                                transactional_id_pattern
 *     state_filters            => COMPACT_STRING
 *     producer_id_filters      => INT64
 *     duration_filter          => INT64                    -- since version 1, -1 for "every transaction"
 *     transactional_id_pattern => COMPACT_NULLABLE_STRING  -- since version 2, null for "every id"
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
 * **Version 1 (Kafka 3.8, KIP-994) added the `duration_filter`**: "Version 1: adds DurationFilter to list
 * transactions older than specified duration" is the comment above its `validVersions` in
 * `ListTransactionsRequest.json` @ 3.8.1. It is an age in **milliseconds** measured against the
 * `txnStartTimestamp` of the transaction, and `TransactionStateManager.listTransactionStates` @ 3.9.2 drops every
 * transaction for which `(now - txnStartTimestamp) <= duration_filter`, so the filter is strictly "older than".
 * A negative value - the `"default": -1` of the field - is "every transaction", which is what a version 0 frame
 * means implicitly. The three filters are ANDed. {@see ListTransactionsRequestV0} is the frame below it, which
 * cannot ask at all.
 *
 * **Version 2 (Kafka 4.1, KIP-1152) appended the `transactional_id_pattern`**: "Version 2: adds
 * TransactionalIdPattern to list transactions with the same pattern(KIP-1152)" stands above the `validVersions` of
 * `ListTransactionsRequest.json` @ 4.1.0. It is a regular expression of **RE2/J** (`com.google.re2j.Pattern` in
 * `TransactionStateManager.listTransactionStates` @ 4.1.0, the engine the regex subscription of KIP-848 uses too,
 * not `java.util.regex`) that the **whole** transactional id has to match - `matcher(id).matches()`, not `find()`;
 * a null or empty pattern is "every id", and a pattern the coordinator cannot compile is the **128**
 * (`InvalidRegularExpression`) of Kafka 4.0, the one error the answer gained with it. It is the fourth filter, ANDed with the others. {@see ListTransactionsRequestV1} is the frame
 * below it.
 *
 * @see docs/protocol/4.3.md, sections "ListTransactions API (key 66, v0 to v2)" and "The transactional id pattern
 *      of KIP-1152 (v2)"
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
    public const int VERSION = 2;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Value of the `duration_filter` that asks for every transaction, whatever its age
     *
     * @since Version 1 of protocol (Kafka 3.8, KIP-994)
     */
    public const int NO_DURATION_FILTER = -1;

    /**
     * @param list<string> $stateFilters      States to list, empty for every state
     * @param list<int>    $producerIdFilters Producer ids to list, empty for every producer
     * @param string       $clientId          A user specified identifier for the client
     * @param int          $correlationId     A value the broker passes back unmodified
     * @param int          $durationFilter    Age in milliseconds a transaction has to exceed, -1 for every one
     * @param string|null  $transactionalIdPattern RE2/J regular expression the whole transactional id has to
     *        match, null for every id (KIP-1152, version 2)
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
        int $correlationId = 0,
        /**
         * Age in milliseconds a transaction has to be older than, -1 for every transaction.
         *
         * @since Version 1 of protocol (Kafka 3.8, KIP-994)
         */
        protected readonly int $durationFilter = self::NO_DURATION_FILTER,
        /**
         * RE2/J regular expression the whole transactional id has to match, null for every id.
         *
         * @since Version 2 of protocol (Kafka 4.1, KIP-1152)
         */
        protected readonly ?string $transactionalIdPattern = null
    ) {
        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = [
            'stateFilters'      => [BinarySchema::TYPE_STRING],
            'producerIdFilters' => [BinarySchema::TYPE_INT64],
        ];
        if (static::VERSION >= 1) {
            $body['durationFilter'] = BinarySchema::TYPE_INT64;
        }
        if (static::VERSION >= 2) {
            $body['transactionalIdPattern'] = BinarySchema::TYPE_NULLABLE_STRING;
        }

        return $header + $body;
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

    /**
     * Returns the age in milliseconds a transaction has to exceed to be listed, -1 for every transaction
     * (KIP-994, version 1)
     */
    public function getDurationFilter(): int
    {
        return $this->durationFilter;
    }

    /**
     * Returns the regular expression the transactional ids are filtered by, null for every id (KIP-1152, version 2)
     */
    public function getTransactionalIdPattern(): ?string
    {
        return $this->transactionalIdPattern;
    }
}
