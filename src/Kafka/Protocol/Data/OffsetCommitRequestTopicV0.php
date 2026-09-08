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
 * OffsetCommitRequestTopic DTO, version 0 of the OffsetCommit API
 *
 * The topic entry itself is the same in every version, only its partition entries differ, so this class exists
 * solely to lower the version constant that selects the partition class.
 *
 * @see docs/protocol/0.9.0.md, section "OffsetCommit API (key 8, v0, v1 and v2)"
 */
final class OffsetCommitRequestTopicV0 extends OffsetCommitRequestTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
