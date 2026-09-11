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
 * The result of creating one topic in the versions 5 and 6 of the CreateTopics API
 *
 * <pre>
 *   CreateTopicsResponseTopic (Version: 5 and 6) => Topic ErrorCode ErrorMessage
 *                                                   NumPartitions ReplicationFactor [Configs]
 *                                                   <TAG_BUFFER: 0 => TopicConfigErrorCode>
 * </pre>
 *
 * Kafka 2.8 inserted the **`topic_id`** of KIP-516 between the name and the error code with the version 7; every
 * version below it has no such field, which is what this class only lowers the version constant for.
 *
 * @see docs/protocol/2.8.md, section "CreateTopics API (key 19, v0 to v7)"
 */
final class CreateTopicsResponseTopicV5 extends CreateTopicsResponseTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 5;
}
