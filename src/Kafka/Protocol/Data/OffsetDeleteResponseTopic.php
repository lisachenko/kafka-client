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
 * The result of one topic of an OffsetDelete answer (key 47, Kafka 2.4, KIP-496)
 *
 * <pre>
 *   OffsetDeleteResponseTopic => Name Partitions
 *     Name       => STRING
 *     Partitions => ARRAY of {@see OffsetDeleteResponsePartition}
 * </pre>
 *
 * The coordinator groups the partitions it answered by their topic, so the order of this array is the order of
 * that map and not the order of the request - the answer is read by name here, never by position.
 *
 * @see docs/protocol/2.8.md, section "OffsetDelete API (key 47, v0)"
 */
class OffsetDeleteResponseTopic implements BinarySchemaInterface
{
    /**
     * Name of the topic this result belongs to
     */
    public string $name;

    /**
     * Result of every partition of this topic, indexed by the partition index
     *
     * @var array<int, OffsetDeleteResponsePartition>
     */
    public array $partitions;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'name'       => BinarySchema::TYPE_STRING,
            'partitions' => ['partitionIndex' => OffsetDeleteResponsePartition::class],
        ];
    }
}
