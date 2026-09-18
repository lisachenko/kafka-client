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
 * Topic entry of a Fetch request of version 12
 *
 * The last entry that names the topic by its **name**: `FetchRequest.json` @ 3.1.2 declares the `Topic` string as
 * `versions 0-12`, and version 13 (Kafka 3.1, KIP-516) replaced it with the `topic_id` of
 * {@see FetchRequestTopic}. The partition entry is the one of version 12, i.e. the one with the
 * `last_fetched_epoch` of KIP-595 ({@see FetchRequestTopicPartition}), which is what {@see FetchRequestTopicV9}
 * does not carry.
 *
 * @see docs/protocol/3.9.md, section "Fetch API (key 1, v0 to v13)"
 */
final class FetchRequestTopicV12 extends FetchRequestTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 12;
}
