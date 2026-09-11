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

namespace Protocol\Kafka\Protocol\Data;

/**
 * One topic of a TxnOffsetCommit request of the versions 0 and 1 (Kafka 0.11)
 *
 * <pre>
 *   TxnOffsetCommitRequestTopic (Version: 0 and 1) => topic [partitions]
 * </pre>
 *
 * The topic entry itself never changed; what Kafka 2.1 added is the `committed_leader_epoch` of its **partitions**
 * (KIP-320), so this class only lowers the version constant that
 * {@see TxnOffsetCommitRequestTopic::partitionClass()} follows, which picks
 * {@see TxnOffsetCommitRequestPartitionV0}.
 *
 * @see docs/protocol/2.8.md, section "TxnOffsetCommit API (key 28, v0 to v2)"
 */
final class TxnOffsetCommitRequestTopicV0 extends TxnOffsetCommitRequestTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
