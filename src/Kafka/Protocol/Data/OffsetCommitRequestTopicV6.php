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
 * OffsetCommitRequestTopic DTO, the versions 6 to 9 of the OffsetCommit API: the entry that names its topic
 *
 * <pre>
 *   OffsetCommitRequestTopic => topic [partitions]
 *     topic      => STRING
 *     partitions => OffsetCommitRequestPartition   -- with the committed_leader_epoch of version 6
 * </pre>
 *
 * `OffsetCommitRequest.json` @ 4.2.0 declares the `Name` of a topic entry as `"versions": "0-9"`, and version 10
 * (Kafka 4.2, KIP-848) replaced it with the `TopicId` of {@see OffsetCommitRequestTopic}. The partition entries are
 * those of version 6 (KIP-320), which version 10 did not touch.
 *
 * @see docs/protocol/4.3.md, section "OffsetCommit API (key 8, v0 to v10)"
 * @see docs/protocol/4.3.md, section "The topic ids of OffsetCommit (v10, KIP-848)"
 */
final class OffsetCommitRequestTopicV6 extends OffsetCommitRequestTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 6;
}
