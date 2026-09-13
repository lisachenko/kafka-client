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
 * The result of one topic of an AlterPartitionReassignments answer (key 45, Kafka 2.4, KIP-455)
 *
 * <pre>
 *   ReassignableTopicResponse => Name Partitions TAG_BUFFER
 *     Name       => COMPACT_STRING
 *     Partitions => COMPACT_ARRAY of {@see ReassignablePartitionResponse}
 * </pre>
 *
 * The controller answers one entry per topic of the request, in the order it received them, and every partition of
 * the request carries its own error - see {@see ReassignablePartitionResponse}.
 *
 * @see docs/protocol/2.8.md, section "AlterPartitionReassignments API (key 45, v0)"
 */
class ReassignableTopicResponse implements BinarySchemaInterface
{
    /**
     * Name of the topic this result belongs to
     */
    public string $name;

    /**
     * Result of every requested partition of this topic, indexed by the partition index
     *
     * @var array<int, ReassignablePartitionResponse>
     */
    public array $partitions;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'name'       => BinarySchema::TYPE_STRING,
            'partitions' => ['partitionIndex' => ReassignablePartitionResponse::class],
        ];
    }
}
