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
 * Produce request Topic DTO of the versions 0 to 12
 *
 * <pre>
 *   TopicName [Partition RecordSetSize RecordSet]
 *     TopicName => string
 * </pre>
 *
 * The entry that names its topic by **name**: `ProduceRequest.json` @ 4.1.0 declares `Name` as `"versions": "0-12"`,
 * and version 13 (Kafka 4.1, KIP-516) replaced it with the `topic_id` of {@see ProduceRequestTopic}.
 *
 * @see docs/protocol/4.3.md, sections "Produce API (key 0, v0 to v13)" and "The topic ids of the produce path (v13,
 *      KIP-516)"
 */
final class ProduceRequestTopicV12 extends ProduceRequestTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 12;
}
