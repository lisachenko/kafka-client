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
 * One topic of an OffsetDelete request (key 47, Kafka 2.4, KIP-496)
 *
 * <pre>
 *   OffsetDeleteRequestTopic => Name Partitions
 *     Name       => STRING
 *     Partitions => ARRAY of {@see OffsetDeleteRequestPartition}
 * </pre>
 *
 * `Name` is the `mapKey` of the array in `OffsetDeleteRequestTopic.json` @ 2.8.2, so a topic may not appear twice
 * in one request; this package indexes the array by the same field.
 *
 * @see docs/protocol/2.8.md, section "OffsetDelete API (key 47, v0)"
 */
class OffsetDeleteRequestTopic implements BinarySchemaInterface
{
    /**
     * Name of the topic whose committed offsets are deleted
     */
    public string $name;

    /**
     * Partitions of this topic, indexed by their partition index
     *
     * @var array<int, OffsetDeleteRequestPartition>
     */
    public array $partitions;

    /**
     * @param string             $name       Name of the topic
     * @param iterable<int, int> $partitions Indexes of the partitions to delete the committed offset of
     */
    public function __construct(string $name, iterable $partitions)
    {
        $packed = [];
        foreach ($partitions as $partitionIndex) {
            $packed[$partitionIndex] = new OffsetDeleteRequestPartition($partitionIndex);
        }

        $this->name       = $name;
        $this->partitions = $packed;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'name'       => BinarySchema::TYPE_STRING,
            'partitions' => ['partitionIndex' => OffsetDeleteRequestPartition::class],
        ];
    }
}
