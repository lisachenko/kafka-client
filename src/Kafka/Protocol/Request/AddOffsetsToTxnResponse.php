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
 * AddOffsetsToTxn response object, version 1 (key 25)
 *
 * <pre>
 *   AddOffsetsToTxn Response (Version: 0 and 1) => throttle_time_ms error_code
 *     throttle_time_ms => INT32
 *     error_code       => INT16
 * </pre>
 *
 * The api was born in Kafka 0.11, after KIP-124 made `throttle_time_ms` the first field of every new answer.
 *
 * Error codes a 0.11.0.3 coordinator reports, all of them at the top level - unlike
 * {@see AddPartitionsToTxnResponse}, this api enrols exactly one partition and therefore has one error code:
 *
 * | Code | Name                               | Meaning                                                          |
 * |------|------------------------------------|------------------------------------------------------------------|
 * | 0    | None                               | `__consumer_offsets` of that group is part of the transaction     |
 * | 15   | GroupCoordinatorNotAvailable       | The transaction coordinator is not available on this broker       |
 * | 16   | NotCoordinatorForGroup             | Another broker coordinates this transactional id                  |
 * | 47   | InvalidProducerEpoch               | The epoch is below the one `__transaction_state` holds - fenced   |
 * | 48   | InvalidTxnState                    | The id is in a state that may not add partitions                  |
 * | 49   | InvalidProducerIdMapping           | The producer id is not the one the coordinator holds for the id   |
 * | 51   | ConcurrentTransactions             | The previous transaction of the id is still being completed       |
 * | 30   | GroupAuthorizationFailed           | The client may not `Read` the consumer group                      |
 * | 53   | TransactionalIdAuthorizationFailed | The client may not `Write` the transactional id                   |
 *
 * **Kafka 2.0 added version 1** and changed nothing about the bytes: `ADD_OFFSETS_TO_TXN_RESPONSE_V1 =
 * ADD_OFFSETS_TO_TXN_RESPONSE_V0` in `Protocol.java` @ 2.0.1. The higher version is the client's promise of KIP-219 -
 * that it honours `throttle_time_ms` itself - and a 2.8.2 broker acts on it by answering a throttled request
 * FIRST and muting the channel afterwards, instead of holding the answer back
 * (`RequestHandlerHelper.sendResponseMaybeThrottle` @ 2.8.2).
 * {@see AddOffsetsToTxnResponseV0} is the same frame with the version field of Kafka 0.11.
 *
 * @see docs/protocol/2.8.md, section "AddOffsetsToTxn API (key 25, v0 to v3)"
 */
class AddOffsetsToTxnResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;

    /**
     * The version 3 of Kafka 2.8 is the first flexible one of this api (KIP-482)
     */
    public const int FLEXIBLE_VERSION = 3;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation
     */
    public int $throttleTimeMs = 0;

    /**
     * Error code of the answer, 0 when the offsets topic of the group is part of the transaction
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
