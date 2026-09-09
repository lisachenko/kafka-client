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

/**
 * EndTxn response object, version 0 (key 26)
 *
 * <pre>
 *   EndTxn Response (Version: 0) => throttle_time_ms error_code
 *     throttle_time_ms => INT32
 *     error_code       => INT16
 * </pre>
 *
 * The api was born in Kafka 0.11, after KIP-124 made `throttle_time_ms` the first field of every new answer.
 *
 * The error code 0 means that the coordinator has **decided** the outcome and written it into
 * `__transaction_state`, not that the markers are in the partitions already; see {@see EndTxnRequest}.
 *
 * | Code | Name                               | Meaning                                                          |
 * |------|------------------------------------|------------------------------------------------------------------|
 * | 0    | None                               | The transaction is being committed or aborted                     |
 * | 15   | GroupCoordinatorNotAvailable       | The transaction coordinator is not available on this broker       |
 * | 16   | NotCoordinatorForGroup             | Another broker coordinates this transactional id                  |
 * | 47   | InvalidProducerEpoch               | The epoch is below the one `__transaction_state` holds - fenced   |
 * | 48   | InvalidTxnState                    | There is no open transaction for this id                          |
 * | 49   | InvalidProducerIdMapping           | The producer id is not the one the coordinator holds for the id   |
 * | 51   | ConcurrentTransactions             | The previous transaction of the id is still being completed       |
 * | 53   | TransactionalIdAuthorizationFailed | The client may not `Write` the transactional id                   |
 *
 * @see docs/protocol/0.11.0.md, section "EndTxn API (key 26, v0)"
 */
class EndTxnResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation
     */
    public int $throttleTimeMs = 0;

    /**
     * Error code of the answer, 0 when the coordinator accepted the outcome of the transaction
     */
    public int $errorCode = 0;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'throttleTimeMs' => BinarySchema::TYPE_INT32,
            'errorCode'      => BinarySchema::TYPE_INT16,
        ];
    }
}
