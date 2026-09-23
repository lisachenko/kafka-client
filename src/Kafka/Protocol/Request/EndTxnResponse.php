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
 * EndTxn response object, version 5 (key 26)
 *
 * <pre>
 *   EndTxn Response (Version: 0 and 1) => throttle_time_ms error_code
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
 * **Kafka 2.0 added version 1** and changed nothing about the bytes: `END_TXN_RESPONSE_V1 =
 * END_TXN_RESPONSE_V0` in `Protocol.java` @ 2.0.1. The higher version is the client's promise of KIP-219 -
 * that it honours `throttle_time_ms` itself - and a 2.8.2 broker acts on it by answering a throttled request
 * FIRST and muting the channel afterwards, instead of holding the answer back
 * (`RequestHandlerHelper.sendResponseMaybeThrottle` @ 2.8.2).
 * {@see EndTxnResponseV0} is the same frame with the version field of Kafka 0.11.
 *
 * **Kafka 3.8 added the version 4** (KIP-890) and gave the answer no field. A 3.9.2 coordinator still answers an
 * abort of a committed transaction with the **48** and a fenced producer with the **90**: the end of a
 * transaction verifies no partition, so the code 120 never reaches this api either - measured at both versions.
 * {@see EndTxnResponseV3} is the frame of Kafka 2.8.
 *
 * **Kafka 4.0 added the version 5** (KIP-890 part 2): *"Version 5 enables bumping epoch on every transaction
 * (KIP-890 Part 2), so producer ID and epoch are included in the response"* (`EndTxnResponse.json` @ 4.0.0). Two
 * fields follow the error code, both `"ignorable": true` with the default **-1**:
 *
 * <pre>
 *   EndTxn Response (Version: 5) => throttle_time_ms error_code producer_id producer_epoch TAG_BUFFER
 *     producer_id    => INT64
 *     producer_epoch => INT16
 * </pre>
 *
 * They are the producer id and the epoch the **next** transaction of the producer runs under: the coordinator of
 * the transaction protocol v2 bumps the epoch with every end of a transaction, and hands out a new producer id
 * with the epoch 0 when the epoch is exhausted. The Java `TransactionManager.EndTxnHandler` @ 4.0.0 takes them over
 * whenever the producer id of the answer is not -1 and starts every sequence at 0 again.
 * {@see EndTxnResponseV4} is the answer of Kafka 3.8, without the two fields.
 *
 * @see docs/protocol/4.3.md, section "EndTxn API (key 26, v0 to v5)"
 */
class EndTxnResponse extends AbstractResponse
{
    /**
     * The default of `producer_id` in `EndTxnResponse.json` @ 4.0.0: no producer id handed out
     */
    public const int NO_PRODUCER_ID = -1;

    /**
     * The default of `producer_epoch` in `EndTxnResponse.json` @ 4.0.0
     */
    public const int NO_PRODUCER_EPOCH = -1;

    /**
     * @inheritdoc
     */
    public const int VERSION = 5;

    /**
     * The version 3 of Kafka 2.8 is the first flexible one of this api (KIP-482)
     */
    public const int FLEXIBLE_VERSION = 3;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation
     */
    public int $throttleTimeMs = 0;

    /**
     * Error code of the answer, 0 when the coordinator accepted the outcome of the transaction
     */
    public int $errorCode = 0;

    /**
     * Producer id the next transaction runs under, -1 below the version 5 and in an answer that carries none
     *
     * @since Version 5 of protocol
     */
    public int $producerId = self::NO_PRODUCER_ID;

    /**
     * Epoch the next transaction runs under, bumped by the coordinator with the end of this one; -1 without one
     *
     * @since Version 5 of protocol
     */
    public int $producerEpoch = self::NO_PRODUCER_EPOCH;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = [
            'throttleTimeMs' => BinarySchema::TYPE_INT32,
            'errorCode'      => BinarySchema::TYPE_INT16,
        ];
        if (static::VERSION >= 5) {
            $body['producerId']    = BinarySchema::TYPE_INT64;
            $body['producerEpoch'] = BinarySchema::TYPE_INT16;
        }

        return $header + $body;
    }

    /**
     * Tells whether the answer hands the producer a new producer id and epoch (version 5, KIP-890 part 2)
     *
     * `EndTxnHandler.handleResponse()` @ 4.0.0 reads it as `producerId() != -1`: an answer below the version 5,
     * and an answer of a version 5 that carries the defaults, leave the producer where it is.
     */
    public function hasProducerIdAndEpoch(): bool
    {
        return $this->producerId !== self::NO_PRODUCER_ID;
    }
}
