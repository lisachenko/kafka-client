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

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * The partitions of one topic in a ConsumerGroupDescribe answer (key 69, Kafka 3.7, KIP-848)
 *
 * <pre>
 *   TopicPartitions => topic_id topic_name [partitions]
 *     topic_id   => UUID
 *     topic_name => COMPACT_STRING
 *     partitions => partition
 *       partition => INT32
 * </pre>
 *
 * The `commonStructs` entry `TopicPartitions` of `ConsumerGroupDescribeResponse.json` @ 3.9.2. It is **not** the
 * structure of the same name in the ConsumerGroupHeartbeat frames
 * ({@see ConsumerGroupHeartbeatTopicPartitions}): the describe api puts the **topic name** next to the id,
 * because the answer is read by a human being or by an administrative tool that has no metadata of its own, while
 * the heartbeat is read by a consumer that has.
 *
 * @see \Protocol\Kafka\Protocol\Request\ConsumerGroupDescribeResponse
 * @see docs/protocol/4.3.md, section "ConsumerGroupDescribe API (key 69, v0 and v1)"
 */
class ConsumerGroupDescribeTopicPartitions implements BinarySchemaInterface
{
    /**
     * @param string    $topicId    The 16 raw bytes of the topic id (KIP-516)
     * @param string    $topicName  Name of the same topic, which this api reports next to the id
     * @param list<int> $partitions Partitions of that topic
     */
    public function __construct(
        /**
         * The 16 raw bytes of the id of the topic these partitions belong to
         */
        public string $topicId = '',
        /**
         * Name of that topic
         */
        public string $topicName = '',
        /**
         * Partitions of that topic
         *
         * @var list<int>
         */
        public array $partitions = []
    ) {}

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topicId'    => BinarySchema::TYPE_UUID,
            'topicName'  => BinarySchema::TYPE_STRING,
            'partitions' => [BinarySchema::TYPE_INT32],
        ];
    }
}
