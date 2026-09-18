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
 * Topic entry of a Fetch answer of version 12
 *
 * The last entry that names the topic by its **name**: `FetchResponse.json` @ 3.1.2 declares the `Topic` string as
 * `versions 0-12`, and version 13 (Kafka 3.1, KIP-516) replaced it with the `topic_id` of
 * {@see FetchResponseTopic}. The partition entry is the flexible one of version 12, with the three tagged fields
 * of KIP-595 and KIP-630 ({@see FetchResponsePartition}).
 *
 * @see docs/protocol/3.9.md, section "Fetch API (key 1, v0 to v16)"
 */
final class FetchResponseTopicV12 extends FetchResponseTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 12;
}
