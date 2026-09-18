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
 * ({@see self::partitionClass()}), see {@see DescribeQuorumResponseTopicV0}.
 *
 * @see docs/protocol/3.9.md, section "DescribeQuorum API (key 55, v0 and v1)"
 */
class DescribeQuorumResponseTopic implements BinarySchemaInterface
{
    /**
     * Version of the DescribeQuorum API that this DTO is unpacked from
     */
    public const int VERSION = 1;

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
        return static::VERSION >= 1
            ? DescribeQuorumResponsePartition::class
            : DescribeQuorumResponsePartitionV0::class;
    }
}
