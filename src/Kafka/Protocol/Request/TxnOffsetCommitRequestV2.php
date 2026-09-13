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
 * TxnOffsetCommit request of version 2 (Kafka 2.1), the body of version 3 without the membership of KIP-447
 *
 * <pre>
 *   TxnOffsetCommit Request (Version: 2) => transactional_id group_id producer_id producer_epoch [topics]
 * </pre>
 *
 * The version 2 is the one that carries the `committed_leader_epoch` of KIP-320 per partition and nothing about
 * the consumer itself; Kafka 2.5 put the generation, the member id and the group instance id in front of the
 * topics with the version 3, which is also the first flexible one. A `ConsumerGroupMetadata` handed to this class
 * is therefore not written - the coordinator reads such a commit as the "not a member" form it always was.
 *
 * @see docs/protocol/2.8.md, section "TxnOffsetCommit API (key 28, v0 to v3)"
 */
final class TxnOffsetCommitRequestV2 extends TxnOffsetCommitRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;

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
