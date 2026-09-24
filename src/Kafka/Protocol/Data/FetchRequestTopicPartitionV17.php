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
 * One partition of a Fetch request of version 17
 *
 * The entry of the `replica_directory_id` of KIP-853 (Kafka 3.9), tag 0 of its tagged-field section, and the last
 * one without the `high_watermark` of a follower that version 18 (Kafka 4.1, KIP-1166) declares as the tag 1, see
 * {@see FetchRequestTopicPartition::$highWatermark}. A consumer writes the same bytes into both.
 *
 * @see docs/protocol/4.3.md, sections "Fetch API (key 1, v0 to v18)" and "The replica directory id of KIP-853 (v17)"
 */
final class FetchRequestTopicPartitionV17 extends FetchRequestTopicPartition
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 17;
}
