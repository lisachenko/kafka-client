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
 * Partition entry of a Fetch request of the versions 12 to 16
 *
 * The entry of version 12 (Kafka 2.7, KIP-595): the partition index, the `current_leader_epoch`, the fetch offset,
 * the `last_fetched_epoch`, the log start offset and the per-partition `max_bytes`, followed by the tagged-field
 * section of a flexible structure - an **empty** one, because no version up to 16 declares a tagged field of this
 * entry.
 *
 * Version 17 (Kafka 3.9, KIP-853) declares the first one, the `replica_directory_id` of
 * {@see FetchRequestTopicPartition::$replicaDirectoryId}; this class is the entry that has no place for it. The
 * bytes of the two are the same as long as the sender is a consumer, because the zero uuid of a consumer is the
 * default of the field and a tagged field whose value is its default is not written at all.
 *
 * @see docs/protocol/3.9.md, section "Fetch API (key 1, v0 to v17)"
 */
final class FetchRequestTopicPartitionV12 extends FetchRequestTopicPartition
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 12;
}
