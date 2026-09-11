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
 * The replicas of one partition of a topic that is being created
 *
 * <pre>
 *   CreateTopicsRequestReplicaAssignment => PartitionId [Replicas]
 *     PartitionId => int32
 *     Replicas    => int32
 * </pre>
 *
 * `PARTITION_REPLICA_ASSIGNMENT_ENTRY` in `Protocol.java` @ 0.10.2.2. The first broker id of the list is the
 * preferred leader of the partition. An explicit assignment replaces the placement that
 * `AdminUtils.assignReplicasToBrokers` would have computed, so `num_partitions` and `replication_factor` have to be
 * left unset (-1) next to it - `AdminManager.createTopics` answers a request that sets both with the error code 42
 * (InvalidRequest).
 *
 * The broker does NOT check that the ids name brokers that exist ("we don't check that replicaAssignment contains
 * unknown brokers - unlike in add-partitions case, this follows the existing logic in TopicCommand"), it only
 * checks that every partition of the topic is assigned and that all of them get the same number of replicas.
 *
 * @see docs/protocol/2.8.md, section "CreateTopics API (key 19, v0 to v4)"
 */
class CreateTopicsRequestReplicaAssignment implements BinarySchemaInterface
{
    /**
     * Id of the partition this assignment is for
     */
    public int $partitionId;

    /**
     * Broker ids that should host the partition, the preferred leader first
     *
     * @var list<int>
     */
    public array $replicas;

    /**
     * @param list<int> $replicas Broker ids that should host the partition
     */
    public function __construct(int $partitionId, array $replicas)
    {
        $this->partitionId = $partitionId;
        $this->replicas    = $replicas;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partitionId' => BinarySchema::TYPE_INT32,
            'replicas'    => [BinarySchema::TYPE_INT32],
        ];
    }
}
