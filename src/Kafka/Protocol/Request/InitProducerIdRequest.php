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
 * InitProducerId, version 2: asks for a producer id and its epoch (ApiKey 22, Kafka 0.11, KIP-98)
 *
 * <pre>
 *   InitProducerId Request (Version: 0 to 2) => transactional_id transaction_timeout_ms
 *     transactional_id       => NULLABLE_STRING   (COMPACT_NULLABLE_STRING from v2)
 *     transaction_timeout_ms => INT32
 * </pre>
 *
 * This is the first frame of both producers that KIP-98 added, and the `transactional_id` decides which of the two
 * is being started:
 *
 * * **`null` - the idempotent producer.** `TransactionCoordinator.handleInitProducerId` @ 0.11.0.3 "blindly
 *   accepts" the request: it hands out the next producer id of the block that the broker reserved in ZooKeeper,
 *   with the epoch **0**, and it does so on **any** broker of the cluster, because nothing about that id is
 *   coordinated. Every call answers a *new* id, so an idempotent producer asks exactly once and keeps what it got
 *   for its whole session; there is no way to ask for the same id again after a restart, which is why idempotence
 *   is a guarantee within one producer session only.
 * * **A non-empty id - the transactional producer.** The request has to reach the **transaction coordinator** of
 *   that id, {@see \Protocol\Kafka\Client::getTransactionCoordinator()}; it returns the producer id that
 *   `__transaction_state` holds for the id and **bumps its epoch by one**, which fences every producer that is
 *   still running with the previous epoch.
 *
 * `transaction_timeout_ms` is how long the coordinator waits for a status update of an open transaction before it
 * aborts it. It is only looked at for a non-null transactional id, and it is checked against the broker option
 * `transaction.max.timeout.ms` (900000 by default): a larger value is answered with the error code **50**
 * (InvalidTransactionTimeout). With a `null` transactional id the field is ignored entirely - a 0.11.0.3 broker
 * answers `transaction_timeout_ms = 999999999` with a normal producer id, because the null branch returns before
 * the check.
 *
 * The empty string is not a transactional id: the broker answers it with the error code **42** (InvalidRequest),
 * deliberately, to keep its behaviour the same as the Java client, which refuses the empty id in its configuration.
 *
 * **Kafka 2.0 added version 1** and changed nothing about the bytes: `INIT_PRODUCER_ID_REQUEST_V1 =
 * INIT_PRODUCER_ID_REQUEST_V0` in `Protocol.java` @ 2.0.1. The higher version is the client's promise of KIP-219 -
 * that it honours `throttle_time_ms` itself - and a 2.8.2 broker acts on it by answering a throttled request
 * FIRST and muting the channel afterwards, instead of holding the answer back
 * (`RequestHandlerHelper.sendResponseMaybeThrottle` @ 2.8.2).
 *
 * **Kafka 2.4 added version 2**, which changes nothing either: it is the first **flexible** version of the api
 * (`"flexibleVersions": "2+"` in `InitProducerIdRequest.json` @ 2.8.2), so the frame carries the request header v2,
 * the transactional id is a compact string and both the header and the body end in a tagged-field section - the
 * same two fields, three bytes shorter for a null id. This is the version the client sends;
 * {@see InitProducerIdRequestV1} and {@see InitProducerIdRequestV0} are the same frame in the plain encoding.
 *
 * The versions 3 (Kafka 2.5, KIP-360: the producer id and epoch of the caller) and 4 (Kafka 2.7, KIP-588) belong to
 * a later ticket of this line.
 *
 * **Kafka 2.5 added the version 3** (KIP-360): the request gained a `producer_id` and a `producer_epoch`, both -1
 * by default. A **transactional** producer that already holds an id sends the pair to have its epoch **bumped**
 * instead of asking for a new id: `TransactionCoordinator.handleInitProducerId` @ 2.8.2 answers the same id with
 * `epoch + 1` and fences everything that still writes under the old one, which is how a producer recovers from an
 * error that used to make it unusable. The -1/-1 of {@see self::NO_PRODUCER_ID} is the old behaviour, "give me an
 * id"; {@see InitProducerIdRequestV2} is the frame without the two fields.
 *
 * @see docs/protocol/2.8.md, section "InitProducerId API (key 22, v0 to v4)"
 */
class InitProducerIdRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::INIT_PRODUCER_ID;

    /**
     * @inheritdoc
     */
    public const int VERSION = 4;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 2;

    /**
     * Default of the `transaction.timeout.ms` option of the Java producer, one minute
     */
    public const int DEFAULT_TRANSACTION_TIMEOUT_MS = 60000;

    /**
     * `producer_id` of a request that asks for a NEW producer id instead of bumping the epoch of one it has
     *
     * The `"default": "-1"` of the field in `InitProducerIdRequest.json` @ 2.5.1, and
     * `RecordBatch.NO_PRODUCER_ID` of the Java client.
     *
     * @since Version 3 of protocol
     */
    public const int NO_PRODUCER_ID = -1;

    /**
     * `producer_epoch` that goes with {@see self::NO_PRODUCER_ID}, `RecordBatch.NO_PRODUCER_EPOCH`
     *
     * @since Version 3 of protocol
     */
    public const int NO_PRODUCER_EPOCH = -1;

    /**
     * @param string|null $transactionalId       Transactional id whose producer id is asked for, `null` for the
     *        producer id of a plain idempotent producer
     * @param int         $transactionTimeoutMs  How long the coordinator waits for a status update of an open
     *        transaction before it aborts it; ignored for a `null` transactional id
     * @param int         $producerId            Producer id the caller already has, whose epoch it wants bumped
     *        (KIP-360), or {@see self::NO_PRODUCER_ID} for a new one
     * @param int         $producerEpoch         Epoch that belongs to that id, or {@see self::NO_PRODUCER_EPOCH}
     * @param string      $clientId              A user specified identifier for the client making the request
     * @param int         $correlationId         A user-supplied value that the broker passes back unmodified
     */
    public function __construct(
        /**
         * The transactional id whose producer id we want to retrieve or generate
         */
        protected readonly ?string $transactionalId = null,
        /**
         * The time in ms to wait for before aborting idle transactions sent by this producer
         */
        protected readonly int $transactionTimeoutMs = self::DEFAULT_TRANSACTION_TIMEOUT_MS,
        /**
         * Producer id the caller holds, or -1 to ask for a new one
         *
         * @since Version 3 of protocol
         */
        protected readonly int $producerId = self::NO_PRODUCER_ID,
        /**
         * Epoch of that producer id, or -1
         *
         * @since Version 3 of protocol
         */
        protected readonly int $producerEpoch = self::NO_PRODUCER_EPOCH,
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

        $body = [
            'transactionalId'      => BinarySchema::TYPE_NULLABLE_STRING,
            'transactionTimeoutMs' => BinarySchema::TYPE_INT32,
        ];
        if (static::VERSION >= 3) {
            $body['producerId']    = BinarySchema::TYPE_INT64;
            $body['producerEpoch'] = BinarySchema::TYPE_INT16;
        }

        return $header + $body;
    }

    /**
     * Returns the transactional id this request asks for, `null` for a plain idempotent producer
     */
    public function getTransactionalId(): ?string
    {
        return $this->transactionalId;
    }

    /**
     * Returns the transaction timeout in milliseconds that the request states
     */
    public function getTransactionTimeoutMs(): int
    {
        return $this->transactionTimeoutMs;
    }

    /**
     * Returns the producer id whose epoch this request wants bumped, {@see self::NO_PRODUCER_ID} for a new one
     */
    public function getProducerId(): int
    {
        return $this->producerId;
    }

    /**
     * Returns the epoch that belongs to it, {@see self::NO_PRODUCER_EPOCH} when there is none
     */
    public function getProducerEpoch(): int
    {
        return $this->producerEpoch;
    }
}
