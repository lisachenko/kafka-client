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
 * One topic of a TxnOffsetCommit request of the versions 2 to 5: the entry that names its topic
 *
 * <pre>
 *   TxnOffsetCommitRequestTopic (Version: 2 to 5) => topic [partitions]
 *     topic      => STRING
 *     partitions => TxnOffsetCommitRequestPartition   -- with the committed_leader_epoch of version 2
 * </pre>
 *
 * `TxnOffsetCommitRequest.json` @ 4.4.0 declares the `Name` of a topic entry as `"versions": "0-5"`, and version 6
 * (Kafka 4.4, KIP-1319) replaced it with the `TopicId` of {@see TxnOffsetCommitRequestTopic}. The partition entries
 * are those of version 2 (KIP-320), which version 6 did not touch.
 *
 * @see docs/protocol/overview.md, section "TxnOffsetCommit API (key 28, v0 to v6)"
 */
final class TxnOffsetCommitRequestTopicV2 extends TxnOffsetCommitRequestTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
