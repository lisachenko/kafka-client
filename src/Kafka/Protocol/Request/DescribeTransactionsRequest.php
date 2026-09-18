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
 * DescribeTransactions, version 0: the state of a transactional id (ApiKey 65, Kafka 3.0)
 *
 * <pre>
 *   DescribeTransactions Request (Version: 0) => [transactional_ids]
 *     transactional_ids => COMPACT_STRING
 * </pre>
 *
 * `DescribeTransactionsRequest.json` @ 3.0.2 declares the api with a single field - the transactional ids to
 * describe - and it is flexible from its version 0, as every api key Kafka 3.x added is. An **empty** array is a
 * request about nothing and is answered with an empty `transaction_states` array; the field is not nullable.
 *
 * **The request goes to the transaction coordinator of each id**, the broker a {@see GroupCoordinatorRequest} of
 * the type {@see GroupCoordinatorRequest::COORDINATOR_TYPE_TRANSACTION} points at: the state lives in the
 * `__transaction_state` partition of that id, and a broker that does not coordinate it answers the code **16**
 * (`NotCoordinator`) in the entry of that id.
 * {@see \Protocol\Kafka\Admin\AdminClient::describeTransactions()} groups the ids by their coordinator and sends
 * one request per broker.
 *
 * @see docs/protocol/3.9.md, section "DescribeTransactions API (key 65, v0)"
 */
class DescribeTransactionsRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::DESCRIBE_TRANSACTIONS;

    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * @param list<string> $transactionalIds Transactional ids to describe, empty for none
     * @param string       $clientId         A user specified identifier for the client
     * @param int          $correlationId    A value the broker passes back unmodified
     */
    public function __construct(
        /**
         * Transactional ids this request asks about.
         *
         * @var list<string>
         */
        protected readonly array $transactionalIds = [],
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
            'transactionalIds' => [BinarySchema::TYPE_STRING],
        ];
    }

    /**
     * Returns the transactional ids this request asks about
     *
     * @return list<string>
     */
    public function getTransactionalIds(): array
    {
        return $this->transactionalIds;
    }
}
