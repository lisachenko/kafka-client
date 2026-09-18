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
 * One topic a DescribeQuorum request asks about (key 55, Kafka 2.8, KIP-595)
 *
 * <pre>
 *   TopicData => TopicName [Partitions]
 *     TopicName  => COMPACT_STRING
 *     Partitions => COMPACT_ARRAY of {@see DescribeQuorumRequestPartition}
 * </pre>
 *
 * @see docs/protocol/3.9.md, section "DescribeQuorum API (key 55, v0 to v2)"
 */
class DescribeQuorumRequestTopic implements BinarySchemaInterface
{
    /**
     * Name of the topic, `__cluster_metadata` for the metadata log of a KRaft cluster
     */
    public string $topicName;

    /**
     * Partitions of this topic the request asks about, indexed by the partition index
     *
     * @var array<int, DescribeQuorumRequestPartition>
     */
    public array $partitions = [];

    /**
     * @param string    $topicName  Name of the topic to ask about
     * @param list<int> $partitions Indexes of the partitions to ask about
     */
    public function __construct(string $topicName = '', array $partitions = [])
    {
        $this->topicName = $topicName;
        foreach ($partitions as $partitionIndex) {
            $this->partitions[$partitionIndex] = new DescribeQuorumRequestPartition($partitionIndex);
        }
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topicName'  => BinarySchema::TYPE_STRING,
            'partitions' => ['partitionIndex' => DescribeQuorumRequestPartition::class],
        ];
    }
}
