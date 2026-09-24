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
 * One topic of a Fetch request of version 17
 *
 * The entry named by its `topic_id` (KIP-516) whose partitions carry the `replica_directory_id` of KIP-853 and not
 * yet the `high_watermark` of KIP-1166 (version 18, Kafka 4.1), see {@see FetchRequestTopicPartitionV17}.
 *
 * @see docs/protocol/4.3.md, sections "Fetch API (key 1, v0 to v18)" and "The replica directory id of KIP-853 (v17)"
 */
final class FetchRequestTopicV17 extends FetchRequestTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 17;
}
