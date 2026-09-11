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
 * Partition entry of a Fetch request of the versions 9, 10 and 11
 *
 * The entry of version 9 (Kafka 2.1, KIP-320): the partition index, the `current_leader_epoch`, the fetch offset,
 * the log start offset of KIP-107 and the per-partition `max_bytes`. Version 12 (Kafka 2.7, KIP-595) inserted the
 * `last_fetched_epoch` between the fetch offset and the log start offset, see
 * {@see FetchRequestTopicPartition::$lastFetchedEpoch}; this class is the entry without it.
 *
 * @see docs/protocol/2.8.md, section "Fetch API (key 1, v0 to v12)"
 */
final class FetchRequestTopicPartitionV9 extends FetchRequestTopicPartition
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 9;
}
