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

use Protocol\Kafka\Common\Record\RecordBatch;
use Protocol\Kafka\Protocol\BinarySchema;

/**
 * InitProducerId response object, version 0 (key 22)
 *
 * <pre>
 *   InitProducerId Response (Version: 0) => throttle_time_ms error_code producer_id producer_epoch
 *     throttle_time_ms => INT32
 *     error_code       => INT16
 *     producer_id      => INT64
 *     producer_epoch   => INT16
 * </pre>
 *
 * The api was born in Kafka 0.11, after KIP-124 made `throttle_time_ms` the first field of every new answer, so
 * there is no version of it without one.
 *
 * Error codes a 0.11.0.3 broker reports, all of them at the top level:
 *
 * | Code | Name                             | Meaning                                                            |
 * |------|----------------------------------|--------------------------------------------------------------------|
 * | 0    | None                             | `producerId` and `producerEpoch` are the ones to write batches with |
 * | 15   | GroupCoordinatorNotAvailable     | `__transaction_state` is still being created by this very lookup    |
 * | 16   | NotCoordinatorForGroup           | Another broker owns the partition of `__transaction_state` that the transactional id hashes to |
 * | 42   | InvalidRequest                   | The transactional id is the **empty string**                        |
 * | 50   | InvalidTransactionTimeout        | `transactionTimeoutMs` is above the broker's `transaction.max.timeout.ms` (900000) |
 * | 51   | ConcurrentTransactions           | The coordinator is fencing the previous epoch of this id and asks for a retry |
 * | 53   | TransactionalIdAuthorizationFailed | The client may not `Write` the transactional id                   |
 * | 31   | ClusterAuthorizationFailed       | A `null` transactional id without the `IdempotentWrite` permission on the cluster |
 *
 * An answer that carries an error code carries **-1 as the producer id and -1 as the epoch**
 * ({@see RecordBatch::NO_PRODUCER_ID}, {@see RecordBatch::NO_PRODUCER_EPOCH}), which is exactly the "no producer
 * state" that a record batch writes.
 *
 * @see docs/protocol/2.8.md, section "InitProducerId API (key 22, v0)"
 */
class InitProducerIdResponse extends AbstractResponse
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
     * Error code of the answer, 0 when the producer id was handed out
     */
    public int $errorCode = 0;

    /**
     * Producer id of this producer, -1 in an answer that carries an error
     */
    public int $producerId = RecordBatch::NO_PRODUCER_ID;

    /**
     * Epoch of that producer id, 0 for a request without a transactional id and -1 in an answer that carries an error
     */
    public int $producerEpoch = RecordBatch::NO_PRODUCER_EPOCH;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'throttleTimeMs' => BinarySchema::TYPE_INT32,
            'errorCode'      => BinarySchema::TYPE_INT16,
            'producerId'     => BinarySchema::TYPE_INT64,
            'producerEpoch'  => BinarySchema::TYPE_INT16,
        ];
    }
}
