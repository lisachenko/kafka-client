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
 *   TopicMetadata => TopicErrorCode TopicName IsInternal [PartitionMetadata]
 *     TopicErrorCode => int16
 *     TopicName      => string
 *     IsInternal     => boolean
 * </pre>
 *
 * `TOPIC_METADATA_V1` in `Protocol.java` @ 0.10.2.2: version 1 of the Metadata API (Kafka 0.10.0) inserted the
 * `IsInternal` flag between the topic name and its partitions, which {@see TopicMetadataV0} still lacks. A topic is
 * internal when Kafka itself keeps it - `Topic.isInternal` @ 0.10.2.2 knows exactly one, `__consumer_offsets`, the
 * log the group coordinator stores the committed offsets in.
 *
 * @see docs/protocol/0.11.0.md, section "Metadata API (key 3, v0 to v4)"
 */
class TopicMetadata implements BinarySchemaInterface
{
    use RestorableTrait;

    /**
     * Version of the Metadata API that this entry is unpacked from
     */
    public const int VERSION = 1;

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
     * Whether the topic is considered a Kafka internal topic, null when the answer was a version 0 one.
     *
     * @since Version 1 of protocol
     */
    public ?bool $isInternal = null;

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
        $scheme = [
            'topicErrorCode' => BinarySchema::TYPE_INT16,
            'topic'          => BinarySchema::TYPE_STRING,
        ];
        if (static::VERSION >= 1) {
            $scheme['isInternal'] = BinarySchema::TYPE_BOOLEAN;
        }
        // A broker does not promise any ordering for the partitions, so they are indexed by their id: the
        // cluster looks a partition up by number, see Cluster::partition() and Cluster::leaderFor()
        $scheme['partitions'] = ['partitionId' => PartitionMetadata::class];

        return $scheme;
    }
}
