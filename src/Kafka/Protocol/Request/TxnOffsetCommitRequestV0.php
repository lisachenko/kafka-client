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

/**
 * TxnOffsetCommit request of version 0 (Kafka 0.11), the frame of version 1 with a lower version field
 *
 * <pre>
 *   TxnOffsetCommit Request (Version: 0) => transactional_id consumer_group_id producer_id producer_epoch [topics]
 * </pre>
 *
 * `TXN_OFFSET_COMMIT_REQUEST_V1 = TXN_OFFSET_COMMIT_REQUEST_V0` in `Protocol.java` @ 2.0.1.
 * Kafka 2.0 raised the api by one version without touching a single byte of the frame, so this class only lowers
 * the version constant that {@see TxnOffsetCommitRequest::getScheme()} follows.
 *
 * What the higher version buys is the promise of KIP-219: a client that sends it honours `throttle_time_ms`
 * itself, so a 2.8.2 broker answers a throttled request of it FIRST and mutes the channel afterwards
 * (`RequestHandlerHelper.sendResponseMaybeThrottle` @ 2.8.2: "Regardless of throttling, send the response
 * immediately") instead of holding the answer back for the throttle time.
 *
 * @see docs/protocol/2.8.md, section "TxnOffsetCommit API (key 28, v0 to v3)"
 */
final class TxnOffsetCommitRequestV0 extends TxnOffsetCommitRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @param string $transactionalId `transactional.id` of the producer that owns the transaction
     * @param string $groupId         Consumer group whose offsets are committed
     * @param int    $producerId      Producer id the transaction coordinator handed out for that transactional id
     * @param int    $producerEpoch   Epoch of that producer id
     * @param array<string, mixed> $topicPartitionOffsets Offsets to commit, as topic => partition => offset
     * @param string $clientId        A user specified identifier for the client making the request
     * @param int    $correlationId   A user-supplied value that the broker passes back unmodified
     */
    public function __construct(
        string $transactionalId,
        string $groupId,
        int $producerId,
        int $producerEpoch,
        array $topicPartitionOffsets = [],
        string $clientId = '',
        int $correlationId = 0
    ) {
        // The membership of KIP-447 is a field of the version 3 and has no place in this frame, so a commit of
        // this version is always the "not a member" form the coordinator accepted before Kafka 2.5
        parent::__construct(
            $transactionalId,
            $groupId,
            $producerId,
            $producerEpoch,
            $topicPartitionOffsets,
            null,
            $clientId,
            $correlationId
        );
    }

}
