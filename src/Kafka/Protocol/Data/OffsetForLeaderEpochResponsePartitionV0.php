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
 * One partition of an OffsetForLeaderEpoch answer, version 0 (key 23, Kafka 0.11)
 *
 * <pre>
 *   OffsetForLeaderEpochResponsePartition => error_code partition_id end_offset
 * </pre>
 *
 * The entry of version 0 carries no `leader_epoch`: a follower is told the end offset of the epoch it asked for
 * and has to assume that the answer really belongs to that epoch, which is the gap KIP-279 closed in version 1,
 * see {@see OffsetForLeaderEpochResponsePartition::$leaderEpoch}. The property exists on this class as well and
 * keeps its default {@see OffsetForLeaderEpochResponsePartition::UNDEFINED_EPOCH}, because the scheme of version 0
 * does not read it.
 *
 * @see docs/protocol/2.8.md, section "OffsetForLeaderEpoch API (key 23, v0 to v4)"
 */
final class OffsetForLeaderEpochResponsePartitionV0 extends OffsetForLeaderEpochResponsePartition
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
