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
 * One topic of an OffsetForLeaderEpoch request of the versions 0 and 1 (key 23)
 *
 * The topic entry never changed, only the partition entries it holds did, so this class only lowers the version
 * constant that {@see OffsetForLeaderEpochRequestTopic::partitionClass()} follows: a request of version 0 or 1
 * carries no `current_leader_epoch` per partition.
 *
 * @see docs/protocol/2.8.md, section "OffsetForLeaderEpoch API (key 23, v0 to v2)"
 */
final class OffsetForLeaderEpochRequestTopicV0 extends OffsetForLeaderEpochRequestTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
