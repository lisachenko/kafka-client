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
 * OffsetCommitRequestTopic DTO, version 1 of the OffsetCommit API
 *
 * The topic entry itself is the same in every version, only its partition entries carry the extra `timestamp` field
 * of version 1, so this class exists solely to lower the version constant that selects the partition class.
 *
 * @see docs/protocol/1.1.md, section "OffsetCommit API (key 8, v0 to v3)"
 */
final class OffsetCommitRequestTopicV1 extends OffsetCommitRequestTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
