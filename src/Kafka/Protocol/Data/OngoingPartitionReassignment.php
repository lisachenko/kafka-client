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
 * The reassignment one partition is going through (key 46, Kafka 2.4, KIP-455)
 *
 * <pre>
 *   OngoingPartitionReassignment => PartitionIndex Replicas AddingReplicas RemovingReplicas TAG_BUFFER
 *     PartitionIndex   => INT32
 *     Replicas         => COMPACT_ARRAY of INT32
 *     AddingReplicas   => COMPACT_ARRAY of INT32
 *     RemovingReplicas => COMPACT_ARRAY of INT32
 * </pre>
 *
 * `Replicas` is the replica set of the partition **right now**, which during a reassignment is the union of the
 * old and the new one: the controller adds every target replica to the set first, waits for it to catch up, and
 * only then removes the ones that are leaving. `AddingReplicas` and `RemovingReplicas` are those two halves, so
 * the target set of the reassignment is `Replicas` minus `RemovingReplicas` and the original one is `Replicas`
 * minus `AddingReplicas` (`ReplicaAssignment` in `kafka.controller.ControllerContext` @ 2.8.2).
 *
 * **A partition appears here only while its reassignment is in progress**, which a one-broker cluster cannot
 * produce: every replica of every partition is already on the only broker, so a reassignment is complete before
 * the controller answers and the list of the container is always empty. The shape above is therefore documented
 * from the sources and from the Java client, and the wire vectors of this api are the empty answer.
 *
 * @see docs/protocol/2.8.md, section "ListPartitionReassignments API (key 46, v0)"
 */
class OngoingPartitionReassignment implements BinarySchemaInterface
{
    /**
     * Index of the partition that is being reassigned
     */
    public int $partitionIndex;

    /**
     * Broker ids the partition lives on at this moment, the union of the old and the new replica set
     *
     * @var list<int>
     */
    public array $replicas;

    /**
     * Broker ids that are being added by this reassignment
     *
     * @var list<int>
     */
    public array $addingReplicas;

    /**
     * Broker ids that are being removed by this reassignment
     *
     * @var list<int>
     */
    public array $removingReplicas;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partitionIndex'   => BinarySchema::TYPE_INT32,
            'replicas'         => [BinarySchema::TYPE_INT32],
            'addingReplicas'   => [BinarySchema::TYPE_INT32],
            'removingReplicas' => [BinarySchema::TYPE_INT32],
        ];
    }
}
