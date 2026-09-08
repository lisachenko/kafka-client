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
/**
 * @author Alexander.Lisachenko
 * @date 14.07.2016
 */

namespace Protocol\Kafka\Common;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * Topic metadata DTO
 *
 * <pre>
 *   TopicMetadata => TopicErrorCode TopicName [PartitionMetadata]
 *     TopicErrorCode => int16
 *     TopicName      => string
 * </pre>
 *
 * The `IsInternal` flag only exists from version 1 of the Metadata API (Kafka 0.10.0) onwards.
 *
 * @see docs/protocol/0.9.0.md, section "Metadata API (key 3, v0)"
 */
class TopicMetadata implements BinarySchemaInterface
{
    use RestorableTrait;

    /**
     * The error code for the given topic.
     *
     * A topic that was just auto-created is announced with error code 5 (LeaderNotAvailable) and an empty partition
     * list until the controller has elected the partition leaders.
     */
    public int $topicErrorCode = 0;

    /**
     * The name of the topic
     */
    public string $topic = '';

    /**
     * Metadata for each partition of the topic, indexed by the partition id.
     *
     * @var array<int, PartitionMetadata>
     */
    public array $partitions = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topicErrorCode' => BinarySchema::TYPE_INT16,
            'topic'          => BinarySchema::TYPE_STRING,
            // A broker does not promise any ordering for the partitions, so they are indexed by their id: the
            // cluster looks a partition up by number, see Cluster::partition() and Cluster::leaderFor()
            'partitions'     => ['partitionId' => PartitionMetadata::class],
        ];
    }
}
