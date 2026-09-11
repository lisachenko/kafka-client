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
 * One topic of an OffsetForLeaderEpoch answer, version 0 (key 23, Kafka 0.11)
 *
 * <pre>
 *   OffsetForLeaderEpochResponseTopic => topic [partitions]
 * </pre>
 *
 * The topic entry never changed, only the partition entries it holds did, so this class only lowers the version
 * constant that {@see OffsetForLeaderEpochResponseTopic::partitionClass()} follows: a version 0 answer carries no
 * `leader_epoch` per partition.
 *
 * @see docs/protocol/2.8.md, section "OffsetForLeaderEpoch API (key 23, v0 to v3)"
 */
final class OffsetForLeaderEpochResponseTopicV0 extends OffsetForLeaderEpochResponseTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
