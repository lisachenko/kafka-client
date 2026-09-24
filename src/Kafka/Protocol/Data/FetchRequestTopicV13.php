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
 * Topic entry of a Fetch request of the versions 13 to 16
 *
 * The first entry that names the topic by its **id** (KIP-516) and the last one whose partition entries have no
 * tagged field of their own: this class exists to pick {@see FetchRequestTopicPartitionV12} as its partition
 * entry, i.e. the entry without the `replica_directory_id` that version 17 (Kafka 3.9, KIP-853) added, see
 * {@see FetchRequestTopic}.
 *
 * @see docs/protocol/4.3.md, section "Fetch API (key 1, v0 to v18)"
 */
final class FetchRequestTopicV13 extends FetchRequestTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 13;
}
