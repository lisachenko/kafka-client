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
 * TxnOffsetCommit request of version 1 (Kafka 2.0), the frame of version 0 with a higher version field
 *
 * <pre>
 *   TxnOffsetCommit Request (Version: 0 and 1) => transactional_id consumer_group_id producer_id producer_epoch
 *                                                 [topics]
 *     topics => topic [partitions]
 *       partitions => partition offset metadata
 * </pre>
 *
 * The version Kafka 2.0 added for the throttling promise of KIP-219, and the last one whose partitions have no
 * `committed_leader_epoch`: Kafka 2.1 put that field between the offset and the metadata of version 2 (KIP-320),
 * which is what {@see TxnOffsetCommitRequest} sends. This class lowers the version constant, and with it
 * {@see TxnOffsetCommitRequest::topicClass()} picks the entries without the epoch.
 *
 * @see docs/protocol/2.8.md, section "TxnOffsetCommit API (key 28, v0 to v2)"
 */
final class TxnOffsetCommitRequestV1 extends TxnOffsetCommitRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
