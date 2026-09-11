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
 * OffsetCommitRequestTopic DTO, the versions 2 to 5 of the OffsetCommit API
 *
 * The topic entry itself is the same in every version, only its partition entries change, so this class exists
 * solely to lower the version constant that selects the partition class -
 * {@see OffsetCommitRequestPartitionV2}, the entry without the leader epoch of version 6.
 *
 * @see docs/protocol/2.8.md, section "OffsetCommit API (key 8, v0 to v7)"
 */
final class OffsetCommitRequestTopicV2 extends OffsetCommitRequestTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
