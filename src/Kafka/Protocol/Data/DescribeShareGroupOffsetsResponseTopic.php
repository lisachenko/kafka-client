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

use Protocol\Kafka\Common\Uuid;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One topic of a group in a DescribeShareGroupOffsets answer (ApiKey 90, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   DescribeShareGroupOffsetsResponseTopic => TopicName TopicId [Partitions] TAG_BUFFER
 *     TopicName  => COMPACT_STRING
 *     TopicId    => UUID
 *     Partitions => COMPACT_ARRAY of {@see DescribeShareGroupOffsetsResponsePartition}
 * </pre>
 *
 * @see docs/protocol/4.3.md, section "DescribeShareGroupOffsets API (key 90, v0)"
 */
final class DescribeShareGroupOffsetsResponseTopic implements BinarySchemaInterface
{
    /**
     * Name of the topic
     */
    public string $topicName;

    /**
     * Id of the topic, the 16 raw bytes of the `uuid` of KIP-516, {@see Uuid::ZERO} for a topic the node does not have
     */
    public string $topicId = Uuid::ZERO;

    /**
     * Start offset of every partition, indexed by the partition index
     *
     * @var array<int, DescribeShareGroupOffsetsResponsePartition>
     */
    public array $partitions = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topicName'  => BinarySchema::TYPE_STRING,
            'topicId'    => BinarySchema::TYPE_UUID,
            'partitions' => ['partitionIndex' => DescribeShareGroupOffsetsResponsePartition::class],
        ];
    }
}
