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
 * One partition of an OffsetForLeaderEpoch request of the versions 0 and 1 (key 23)
 *
 * <pre>
 *   OffsetForLeaderEpochRequestPartition => partition_id leader_epoch
 * </pre>
 *
 * `OffsetForLeaderEpochRequest.json` @ 2.8.2 says "Version 1 is the same as version 0", so these two versions ask
 * with the partition index and the epoch to look up and nothing else. Version 2 (Kafka 2.1, KIP-320) inserted the
 * `current_leader_epoch` that fences the request between them, see
 * {@see OffsetForLeaderEpochRequestPartition::$currentLeaderEpoch}.
 *
 * @see docs/protocol/2.8.md, section "OffsetForLeaderEpoch API (key 23, v0 to v4)"
 */
final class OffsetForLeaderEpochRequestPartitionV0 extends OffsetForLeaderEpochRequestPartition
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
