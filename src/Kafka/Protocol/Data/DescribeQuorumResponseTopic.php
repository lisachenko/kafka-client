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
 * One topic of a DescribeQuorum answer (key 55, Kafka 2.8, KIP-595)
 *
 * <pre>
 *   TopicData => TopicName [Partitions]
 *     TopicName  => COMPACT_STRING
 *     Partitions => COMPACT_ARRAY of {@see DescribeQuorumResponsePartition}
 * </pre>
 *
 * The version of the api selects the shape of the partition entries and nothing else here
 * ({@see self::partitionClass()}), see {@see DescribeQuorumResponseTopicV1} and
 * {@see DescribeQuorumResponseTopicV0}: neither KIP-836 nor KIP-853 gave this entry a field of its own.
 *
 * @see docs/protocol/4.3.md, section "DescribeQuorum API (key 55, v0 to v2)"
 */
class DescribeQuorumResponseTopic implements BinarySchemaInterface
{
    /**
     * Version of the DescribeQuorum API that this DTO is unpacked from
     */
    public const int VERSION = 2;

    /**
     * Name of the topic this entry belongs to
     */
    public string $topicName;

    /**
     * Quorum of every requested partition, indexed by the partition index
     *
     * @var array<int, DescribeQuorumResponsePartition>
     */
    public array $partitions = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topicName'  => BinarySchema::TYPE_STRING,
            'partitions' => ['partitionIndex' => static::partitionClass()],
        ];
    }

    /**
     * Returns the class of a partition entry for the version of the API that this DTO belongs to
     *
     * @return class-string<DescribeQuorumResponsePartition>
     */
    protected static function partitionClass(): string
    {
        return match (true) {
            static::VERSION >= 2 => DescribeQuorumResponsePartition::class,
            static::VERSION >= 1 => DescribeQuorumResponsePartitionV1::class,
            default              => DescribeQuorumResponsePartitionV0::class,
        };
    }
}
