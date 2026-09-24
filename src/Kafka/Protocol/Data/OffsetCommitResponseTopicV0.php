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
 * OffsetCommitResponseTopic DTO, the versions 0 to 9 of the OffsetCommit API: the entry that names its topic
 *
 * <pre>
 *   OffsetCommitResponseTopic => topic [partition_responses]
 *     topic               => STRING
 *     partition_responses => OffsetCommitResponsePartition
 * </pre>
 *
 * The entry did not change from version 0 to version 9; version 10 (Kafka 4.2, KIP-848) replaced its name with the
 * `topic_id` of {@see OffsetCommitResponseTopic}.
 *
 * @see docs/protocol/4.3.md, section "OffsetCommit API (key 8, v0 to v10)"
 */
final class OffsetCommitResponseTopicV0 extends OffsetCommitResponseTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
