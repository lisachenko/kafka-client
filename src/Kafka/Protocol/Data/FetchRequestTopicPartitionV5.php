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
 * Fetch request Topic-Partition DTO of the versions 5 to 8
 *
 * <pre>
 *   Partition FetchOffset LogStartOffset MaxBytes
 * </pre>
 *
 * The entry of version 5 (Kafka 0.11, KIP-107) is the one the versions 6, 7 and 8 send as well: KIP-227 added the
 * fetch session to the *request*, not to its partition entries, and version 8 (KIP-219) added nothing at all. What
 * version 9 (Kafka 2.1, KIP-320) inserted is the `current_leader_epoch` of
 * {@see FetchRequestTopicPartition::$currentLeaderEpoch}, which this entry has no place for, so a client that asks
 * with one of these versions is never fenced on its metadata.
 *
 * @see docs/protocol/2.8.md, sections "Fetch API (key 1, v0 to v11)" and "The leader epoch (KIP-320)"
 */
class FetchRequestTopicPartitionV5 extends FetchRequestTopicPartition
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 5;
}
