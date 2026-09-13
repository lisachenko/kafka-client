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

use Protocol\Kafka\Common\Record\ControlRecordType;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;

/**
 * EndTxn, version 1: commits or aborts the open transaction (ApiKey 26, Kafka 0.11, KIP-98)
 *
 * <pre>
 *   EndTxn Request (Version: 0 and 1) => transactional_id producer_id producer_epoch transaction_result
 *     transactional_id   => STRING
 *     producer_id        => INT64
 *     producer_epoch     => INT16
 *     transaction_result => BOOLEAN
 * </pre>
 *
 * The last request of a transaction, sent to the **transaction coordinator** of the transactional id.
 * `transaction_result` is the single byte that decides everything: **1 commits**, **0 aborts**
 * ({@see self::COMMIT} and {@see self::ABORT}, `TransactionResult` of the Java client).
 *
 * The coordinator answers as soon as it has written the `PrepareCommit`/`PrepareAbort` entry into
 * `__transaction_state` - **not** when the markers are in the partitions. What follows the answer is broker
 * business: the coordinator sends a {@see WriteTxnMarkersRequest} to the leader of every partition that
 * {@see AddPartitionsToTxnRequest} enrolled, each of them appends a **control batch** with the
 * {@see ControlRecordType::COMMIT} or {@see ControlRecordType::ABORT} marker, and only then does the coordinator
 * write the `CompleteCommit`/`CompleteAbort` entry. A `read_committed` consumer therefore sees the records of a
 * committed transaction a moment *after* this request was answered - the marker is what moves the last stable
 * offset of a partition past them.
 *
 * An `EndTxn` for an id that has no open transaction is answered with **48** (`InvalidTxnState`); one with an
 * epoch below the one the coordinator holds with **47** (`InvalidProducerEpoch`), which is the fencing of KIP-98.
 * A commit that arrives while the previous transaction of the id is still being completed is **51**
 * (`ConcurrentTransactions`) and may be retried after a back-off.
 *
 * **Kafka 2.0 added version 1** and changed nothing about the bytes: `END_TXN_REQUEST_V1 =
 * END_TXN_REQUEST_V0` in `Protocol.java` @ 2.0.1. The higher version is the client's promise of KIP-219 -
 * that it honours `throttle_time_ms` itself - and a 2.8.2 broker acts on it by answering a throttled request
 * FIRST and muting the channel afterwards, instead of holding the answer back
 * (`RequestHandlerHelper.sendResponseMaybeThrottle` @ 2.8.2).
 * {@see EndTxnRequestV0} is the same frame with the version field of Kafka 0.11.
 *
 * @see docs/protocol/2.8.md, section "EndTxn API (key 26, v0 to v3)"
 */
class EndTxnRequest extends AbstractRequest
{
    /**
     * `transaction_result` of a commit, `TransactionResult.COMMIT` of the Java client
     */
    public const bool COMMIT = true;

    /**
     * `transaction_result` of an abort, `TransactionResult.ABORT` of the Java client
     */
    public const bool ABORT = false;

    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::END_TXN;

    /**
     * @inheritdoc
     */
    public const int VERSION = 3;

    /**
     * The version 3 of Kafka 2.8 is the first flexible one of this api (KIP-482)
     */
    public const int FLEXIBLE_VERSION = 3;

    /**
     * @param string $transactionalId   `transactional.id` of the producer that owns the transaction
     * @param int    $producerId        Producer id the coordinator handed out for that transactional id
     * @param int    $producerEpoch     Epoch of that producer id
     * @param bool   $transactionResult {@see self::COMMIT} or {@see self::ABORT}
     * @param string $clientId          A user specified identifier for the client making the request
     * @param int    $correlationId     A user-supplied value that the broker passes back unmodified
     */
    public function __construct(
        /**
         * The transactional id whose transaction is being ended
         */
        protected readonly string $transactionalId,
        /**
         * Current producer id in use by the transactional id
         */
        protected readonly int $producerId,
        /**
         * Current epoch associated with the producer id
         */
        protected readonly int $producerEpoch,
        /**
         * The result of the transaction: false aborts it, true commits it
         */
        protected readonly bool $transactionResult = self::COMMIT,
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
            'transactionalId'   => BinarySchema::TYPE_STRING,
            'producerId'        => BinarySchema::TYPE_INT64,
            'producerEpoch'     => BinarySchema::TYPE_INT16,
            'transactionResult' => BinarySchema::TYPE_BOOLEAN,
        ];
    }
}
