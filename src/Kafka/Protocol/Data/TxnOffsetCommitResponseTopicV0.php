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
 * The result of one topic of a TxnOffsetCommit answer of the versions 0 to 5: the entry that names its topic
 *
 * <pre>
 *   TxnOffsetCommitResponseTopic (Version: 0 to 5) => topic [partitions]
 *     topic      => STRING
 *     partitions => TxnOffsetCommitResponsePartition
 * </pre>
 *
 * The entry did not change from version 0 to version 5; version 6 (Kafka 4.4, KIP-1319) replaced its name with the
 * `topic_id` of {@see TxnOffsetCommitResponseTopic}.
 *
 * @see docs/protocol/4.4.md, section "TxnOffsetCommit API (key 28, v0 to v6)"
 */
final class TxnOffsetCommitResponseTopicV0 extends TxnOffsetCommitResponseTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
