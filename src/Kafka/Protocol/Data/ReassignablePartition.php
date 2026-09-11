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
 * One partition of an AlterPartitionReassignments request (key 45, Kafka 2.4, KIP-455)
 *
 * <pre>
 *   ReassignablePartition => PartitionIndex Replicas TAG_BUFFER
 *     PartitionIndex => INT32
 *     Replicas       => COMPACT_NULLABLE_ARRAY of INT32
 * </pre>
 *
 * `Replicas` is **the whole replica set the partition should end up with**, in order - the first broker id is the
 * preferred leader - and not the set to add: the controller computes what to add and what to remove from the
 * difference to the current assignment. `null` is the second meaning of the field and the reason it is nullable:
 * it **cancels** a reassignment that is still in progress and restores the original replica set. A partition that
 * has no reassignment in progress answers a cancellation with the error code **85**
 * (`NoReassignmentInProgress`), which is measured on the container.
 *
 * An empty array is neither of the two and is refused with **39** (`InvalidReplicaAssignment`, *"Empty replica list
 * specified in partition reassignment."*), and so is a set that names a broker that is not alive (*"Replica
 * assignment has brokers that are not alive. Replica list: ArrayBuffer(7), live broker list: Set(0)"*).
 *
 * @see docs/protocol/2.8.md, section "AlterPartitionReassignments API (key 45, v0)"
 */
class ReassignablePartition implements BinarySchemaInterface
{
    /**
     * Index of the partition to reassign
     */
    public int $partitionIndex;

    /**
     * Broker ids the partition should live on afterwards, or null to cancel a reassignment in progress
     *
     * @var list<int>|null
     */
    public ?array $replicas;

    /**
     * @param int             $partitionIndex Index of the partition
     * @param list<int>|null  $replicas       Target replica set, or null to cancel a pending reassignment
     */
    public function __construct(int $partitionIndex, ?array $replicas)
    {
        $this->partitionIndex = $partitionIndex;
        $this->replicas       = $replicas === null ? null : array_values($replicas);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partitionIndex' => BinarySchema::TYPE_INT32,
            'replicas'       => [BinarySchema::TYPE_INT32, BinarySchema::FLAG_NULLABLE => true],
        ];
    }
}
