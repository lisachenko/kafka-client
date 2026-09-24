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
 * One topic of the assignment of a member in a ShareGroupDescribe answer (key 77, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   TopicPartitions => topic_id topic_name [partitions]
 * </pre>
 *
 * The `TopicPartitions` common struct of `ShareGroupDescribeResponse.json` @ 4.1.0, which names the topic by its id
 * AND by its name, as the describe api of the consumer protocol does.
 *
 * @see docs/protocol/4.3.md, section "ShareGroupDescribe API (key 77, v1)"
 */
final class ShareGroupDescribeTopicPartitions implements BinarySchemaInterface
{
    /**
     * @param string    $topicId    The 16 raw bytes of the topic id
     * @param string    $topicName  Name of the topic
     * @param list<int> $partitions Partitions of the topic
     */
    public function __construct(
        public string $topicId = '',
        public string $topicName = '',
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
