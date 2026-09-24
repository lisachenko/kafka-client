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
 * Produce response Topic DTO of the versions 10 to 12
 *
 * The entry of the leader discovery of KIP-951 (Kafka 3.7): the topic **name** and the partition entries with the
 * tagged `current_leader`. Version 13 (Kafka 4.1, KIP-516) replaced the name with the `topic_id` of
 * {@see ProduceResponseTopic}; the partition entry did not change.
 *
 * @see docs/protocol/4.3.md, sections "Produce API (key 0, v0 to v13)" and "The leader discovery of KIP-951 (v10)"
 */
final class ProduceResponseTopicV10 extends ProduceResponseTopic
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 10;
}
