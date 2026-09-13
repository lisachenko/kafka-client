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
 * The result of one topic of a DescribeProducers answer (key 61, Kafka 2.8, KIP-664)
 *
 * <pre>
 *   TopicResponse => Name Partitions
 *     Name       => COMPACT_STRING
 *     Partitions => COMPACT_ARRAY of {@see DescribeProducersResponsePartition}
 * </pre>
 *
 * @see docs/protocol/2.8.md, section "DescribeProducers API (key 61, v0)"
 */
class DescribeProducersResponseTopic implements BinarySchemaInterface
{
    /**
     * Name of the topic this result belongs to
     */
    public string $name;

    /**
     * Result of every requested partition, indexed by the partition index
     *
     * @var array<int, DescribeProducersResponsePartition>
     */
    public array $partitions = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'name'       => BinarySchema::TYPE_STRING,
            'partitions' => ['partitionIndex' => DescribeProducersResponsePartition::class],
        ];
    }
}
