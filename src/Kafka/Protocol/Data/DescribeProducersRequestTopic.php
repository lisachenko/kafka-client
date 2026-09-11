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
 * One topic of a DescribeProducers request (key 61, Kafka 2.8, KIP-664)
 *
 * <pre>
 *   TopicRequest => Name PartitionIndexes
 *     Name             => COMPACT_STRING
 *     PartitionIndexes => COMPACT_ARRAY of INT32
 * </pre>
 *
 * @see docs/protocol/2.8.md, section "DescribeProducers API (key 61, v0)"
 */
class DescribeProducersRequestTopic implements BinarySchemaInterface
{
    /**
     * Name of the topic whose partitions are asked about
     */
    public string $name;

    /**
     * Indexes of the partitions to describe
     *
     * @var list<int>
     */
    public array $partitionIndexes;

    /**
     * @param string    $name             Name of the topic
     * @param list<int> $partitionIndexes Partitions to describe
     */
    public function __construct(string $name = '', array $partitionIndexes = [])
    {
        $this->name             = $name;
        $this->partitionIndexes = array_values($partitionIndexes);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'name'             => BinarySchema::TYPE_STRING,
            'partitionIndexes' => [BinarySchema::TYPE_INT32],
        ];
    }
}
