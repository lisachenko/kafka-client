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
 * OffsetCommitRequestPartition DTO, the versions 2 to 5 of the OffsetCommit API
 *
 * <pre>
 *   OffsetCommitRequestPartition => partition offset metadata
 *     partition => INT32
 *     offset    => INT64
 *     metadata  => NULLABLE_STRING
 * </pre>
 *
 * The layout of version 2 is the layout of version 0 again - the `timestamp` of version 1 was replaced by the
 * request-wide `retention_time` - and it stays that way through the versions 3, 4 and 5, because version 5 only
 * **removes** `retention_time` from the request (KIP-211) without touching the partition. The next change of this
 * entry is the `committed_leader_epoch` of version 6 ({@see OffsetCommitRequestPartition}, KIP-320).
 *
 * @see docs/protocol/2.8.md, section "OffsetCommit API (key 8, v0 to v7)"
 */
final class OffsetCommitRequestPartitionV2 extends OffsetCommitRequestPartition
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
