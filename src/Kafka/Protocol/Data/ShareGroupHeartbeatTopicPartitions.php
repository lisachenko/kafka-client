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
 * One topic of the assignment of a ShareGroupHeartbeat answer (key 76, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   TopicPartitions => topic_id [partitions]
 *     topic_id   => UUID
 *     partitions => INT32
 * </pre>
 *
 * The `TopicPartitions` common struct of `ShareGroupHeartbeatResponse.json` @ 4.1.0: the topic is named by the 16 raw
 * bytes of its id alone, as in the answer of the consumer protocol ({@see ConsumerGroupHeartbeatTopicPartitions}).
 * The request of a share member carries no partitions at all - a share member acknowledges records, not partitions.
 *
 * @see docs/protocol/4.3.md, section "ShareGroupHeartbeat API (key 76, v1)"
 */
final class ShareGroupHeartbeatTopicPartitions implements BinarySchemaInterface
{
    /**
     * @param string    $topicId    The 16 raw bytes of the topic id
     * @param list<int> $partitions Partitions of the topic
     */
    public function __construct(
        public string $topicId = '',
        public array $partitions = []
    ) {}

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topicId'    => BinarySchema::TYPE_UUID,
            'partitions' => [BinarySchema::TYPE_INT32],
        ];
    }
}
