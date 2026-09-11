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
 * One topic of a ListPartitionReassignments request (key 46, Kafka 2.4, KIP-455)
 *
 * <pre>
 *   ListPartitionReassignmentsTopics => Name PartitionIndexes TAG_BUFFER
 *     Name             => COMPACT_STRING
 *     PartitionIndexes => COMPACT_ARRAY of INT32
 * </pre>
 *
 * The plural name is the one of the JSON message specification @ 2.8.2, where the structure of *one* topic is
 * called `ListPartitionReassignmentsTopics`.
 *
 * The topic array of the request is **nullable**, and the two values are not the same question: `null` asks for
 * every reassignment the cluster has in progress, an empty array asks for none of them, and an entry with an empty
 * `PartitionIndexes` asks for no partition of that topic. A client that wants "everything" has to send the null
 * array on purpose.
 *
 * @see docs/protocol/2.8.md, section "ListPartitionReassignments API (key 46, v0)"
 */
class ListPartitionReassignmentsTopics implements BinarySchemaInterface
{
    /**
     * Name of the topic whose reassignments are asked for
     */
    public string $name;

    /**
     * Partitions of that topic to ask for
     *
     * @var list<int>
     */
    public array $partitionIndexes;

    /**
     * @param string    $name             Name of the topic
     * @param list<int> $partitionIndexes Partitions to ask for
     */
    public function __construct(string $name, array $partitionIndexes)
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
