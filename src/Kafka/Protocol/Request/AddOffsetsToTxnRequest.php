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
 * AddOffsetsToTxn, version 1: enrols the offsets of a consumer group into the transaction (key 25, Kafka 0.11)
 *
 * <pre>
 *   AddOffsetsToTxn Request (Version: 0 and 1) => transactional_id producer_id producer_epoch consumer_group_id
 *     transactional_id  => STRING
 *     producer_id       => INT64
 *     producer_epoch    => INT16
 *     consumer_group_id => STRING
 * </pre>
 *
 * The first half of `sendOffsetsToTransaction()`, and the request that makes the *consume-transform-produce* loop
 * atomic. It goes to the **transaction coordinator** of the transactional id, and all it does is to enrol the one
 * partition of `__consumer_offsets` that the consumer group hashes to into the transaction, exactly as
 * {@see AddPartitionsToTxnRequest} enrols a data partition: `TransactionCoordinator.handleAddPartitionsToTransaction`
 * @ 0.11.0.3 is literally the method that serves it, called with
 * `new TopicPartition(GROUP_METADATA_TOPIC_NAME, partitionFor(consumerGroupId))`.
 *
 * The offsets themselves are not in this request - they travel in the {@see TxnOffsetCommitRequest} that follows it
 * and that goes to the **group** coordinator. The split is what makes the commit atomic with the produced records:
 * the group coordinator writes the offsets into `__consumer_offsets` as *transactional* records, and it does not
 * make them visible to an OffsetFetch until the transaction coordinator has written the COMMIT marker into that
 * very partition - which it only does because this request put it on the list.
 *
 * **Kafka 2.0 added version 1** and changed nothing about the bytes: `ADD_OFFSETS_TO_TXN_REQUEST_V1 =
 * ADD_OFFSETS_TO_TXN_REQUEST_V0` in `Protocol.java` @ 2.0.1. The higher version is the client's promise of KIP-219 -
 * that it honours `throttle_time_ms` itself - and a 2.8.2 broker acts on it by answering a throttled request
 * FIRST and muting the channel afterwards, instead of holding the answer back
 * (`RequestHandlerHelper.sendResponseMaybeThrottle` @ 2.8.2).
 * {@see AddOffsetsToTxnRequestV0} is the same frame with the version field of Kafka 0.11.
 *
 * @see docs/protocol/2.8.md, section "AddOffsetsToTxn API (key 25, v0 to v2)"
 */
class AddOffsetsToTxnRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::ADD_OFFSETS_TO_TXN;

    /**
     * @inheritdoc
     */
    public const int VERSION = 2;

    /**
     * @param string $transactionalId `transactional.id` of the producer that owns the transaction
     * @param int    $producerId      Producer id the coordinator handed out for that transactional id
     * @param int    $producerEpoch   Epoch of that producer id
     * @param string $groupId         Consumer group whose offsets become part of the transaction
     * @param string $clientId        A user specified identifier for the client making the request
     * @param int    $correlationId   A user-supplied value that the broker passes back unmodified
     */
    public function __construct(
        /**
         * The transactional id whose transaction the offsets belong to
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
         * Consumer group id whose offsets should be included in the transaction (`consumer_group_id` on the wire)
         */
        protected readonly string $groupId,
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
            'transactionalId' => BinarySchema::TYPE_STRING,
            'producerId'      => BinarySchema::TYPE_INT64,
            'producerEpoch'   => BinarySchema::TYPE_INT16,
            'groupId'         => BinarySchema::TYPE_STRING,
        ];
    }
}
