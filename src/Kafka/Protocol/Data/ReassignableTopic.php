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
 * One topic of an AlterPartitionReassignments request (key 45, Kafka 2.4, KIP-455)
 *
 * <pre>
 *   ReassignableTopic => Name Partitions TAG_BUFFER
 *     Name       => COMPACT_STRING
 *     Partitions => COMPACT_ARRAY of {@see ReassignablePartition}
 * </pre>
 *
 * The api is **flexible from its version 0 on** - it was added by Kafka 2.4, the release that introduced the
 * encoding - so the name is a compact string, the partition array a compact array, and both this structure and
 * every partition of it end in a tagged-field section.
 *
 * @see docs/protocol/2.8.md, section "AlterPartitionReassignments API (key 45, v0)"
 */
class ReassignableTopic implements BinarySchemaInterface
{
    /**
     * Name of the topic whose partitions are reassigned
     */
    public string $name;

    /**
     * Partitions of this topic to reassign, indexed by their partition index
     *
     * @var array<int, ReassignablePartition>
     */
    public array $partitions;

    /**
     * @param string                            $name       Name of the topic
     * @param array<int, ReassignablePartition> $partitions Partitions to reassign
     */
    public function __construct(string $name, array $partitions)
    {
        $this->name       = $name;
        $this->partitions = $partitions;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'name'       => BinarySchema::TYPE_STRING,
            'partitions' => ['partitionIndex' => ReassignablePartition::class],
        ];
    }
}
