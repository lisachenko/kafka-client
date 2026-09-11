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
 * The partitions of one topic that are being reassigned (key 46, Kafka 2.4, KIP-455)
 *
 * <pre>
 *   OngoingTopicReassignment => Name Partitions TAG_BUFFER
 *     Name       => COMPACT_STRING
 *     Partitions => COMPACT_ARRAY of {@see OngoingPartitionReassignment}
 * </pre>
 *
 * A topic without a reassignment in progress is **not** in the answer at all, whether the request named it or not:
 * the api answers what is going on, not what was asked for.
 *
 * @see docs/protocol/2.8.md, section "ListPartitionReassignments API (key 46, v0)"
 */
class OngoingTopicReassignment implements BinarySchemaInterface
{
    /**
     * Name of the topic
     */
    public string $name;

    /**
     * Partitions of this topic that are being reassigned, indexed by the partition index
     *
     * @var array<int, OngoingPartitionReassignment>
     */
    public array $partitions;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'name'       => BinarySchema::TYPE_STRING,
            'partitions' => ['partitionIndex' => OngoingPartitionReassignment::class],
        ];
    }
}
