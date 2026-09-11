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
 * One partition of a TxnOffsetCommit request of the versions 0 and 1 (Kafka 0.11), without the leader epoch
 *
 * <pre>
 *   TxnOffsetCommitRequestPartition (Version: 0 and 1) => partition offset metadata
 *     partition => INT32
 *     offset    => INT64
 *     metadata  => NULLABLE_STRING
 * </pre>
 *
 * `TXN_OFFSET_COMMIT_PARTITION_OFFSET_METADATA_REQUEST_V0` in `Protocol.java` @ 2.0.1, which version 1 uses
 * unchanged. Kafka 2.1 put the `committed_leader_epoch` of KIP-320 between the offset and the metadata of version
 * 2, so this class only lowers the version constant that
 * {@see TxnOffsetCommitRequestPartition::getScheme()} follows.
 *
 * @see docs/protocol/2.8.md, section "TxnOffsetCommit API (key 28, v0 to v3)"
 */
final class TxnOffsetCommitRequestPartitionV0 extends TxnOffsetCommitRequestPartition
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
