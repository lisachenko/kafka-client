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
 * One partition of a DescribeTopicPartitions answer (key 75, Kafka 3.8, KIP-966)
 *
 * <pre>
 *   DescribeTopicPartitionsResponsePartition => ErrorCode PartitionIndex LeaderId LeaderEpoch [ReplicaNodes]
 *                                               [IsrNodes] [EligibleLeaderReplicas] [LastKnownElr] [OfflineReplicas]
 *     ErrorCode              => INT16
 *     PartitionIndex         => INT32
 *     LeaderId               => INT32   (-1 while the partition has no leader)
 *     LeaderEpoch            => INT32
 *     ReplicaNodes           => COMPACT_ARRAY of INT32
 *     IsrNodes               => COMPACT_ARRAY of INT32
 *     EligibleLeaderReplicas => COMPACT_ARRAY of INT32, nullable
 *     LastKnownElr           => COMPACT_ARRAY of INT32, nullable
 *     OfflineReplicas        => COMPACT_ARRAY of INT32
 * </pre>
 *
 * `DescribeTopicPartitionsResponsePartition` of `DescribeTopicPartitionsResponse.json` @ 3.8.1. Everything up to
 * `isr_nodes` is the partition entry of a **Metadata** answer with the fields in another order; what KIP-966 adds
 * are the two arrays in the middle - the **eligible leader replicas** and the **last known ELR** - which is the
 * reason this api exists at all: `MetadataResponse` could not gain them without growing a version that every
 * client would have to understand, so KIP-966 gave `--describe` an api of its own.
 *
 * **The ELR is the set of replicas that were in the ISR when the partition lost its last leader** and may
 * therefore be elected without losing acknowledged writes; `last_known_elr` keeps the set of the previous such
 * election. Both are `null` in the specification's default, and both are an **empty array** on a cluster whose
 * `eligible.leader.replicas.version` feature is not enabled - `Replicas.toList(partition.elr)` @ 3.9.2 turns the
 * empty replica array of the metadata image into an empty list, never into a null.
 *
 * @see docs/protocol/4.3.md, section "DescribeTopicPartitions API (key 75, v0)"
 */
class DescribeTopicPartitionsResponsePartition implements BinarySchemaInterface
{
    /**
     * Leader id of a partition that has no leader
     */
    public const int NO_LEADER = -1;

    /**
     * Error of this partition, 0 when it could be described
     */
    public int $errorCode = 0;

    /**
     * Index of this partition inside its topic
     */
    public int $partitionIndex = 0;

    /**
     * Broker id of the leader, {@see self::NO_LEADER} while the partition has none
     */
    public int $leaderId = self::NO_LEADER;

    /**
     * Leader epoch of the partition, the counter KIP-320 fences stale readers with
     */
    public int $leaderEpoch = -1;

    /**
     * Every broker that hosts this partition
     *
     * @var list<int>
     */
    public array $replicaNodes = [];

    /**
     * The brokers that are in sync with the leader
     *
     * @var list<int>
     */
    public array $isrNodes = [];

    /**
     * Replicas that may be elected leader without losing an acknowledged write (KIP-966), `null` when the answer
     * carries no set at all
     *
     * @var list<int>|null
     */
    public ?array $eligibleLeaderReplicas = [];

    /**
     * The eligible leader replicas of the previous election (KIP-966), `null` when the answer carries none
     *
     * @var list<int>|null
     */
    public ?array $lastKnownElr = [];

    /**
     * Replicas whose broker is down or whose log directory has failed
     *
     * @var list<int>
     */
    public array $offlineReplicas = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'errorCode'              => BinarySchema::TYPE_INT16,
            'partitionIndex'         => BinarySchema::TYPE_INT32,
            'leaderId'               => BinarySchema::TYPE_INT32,
            'leaderEpoch'            => BinarySchema::TYPE_INT32,
            'replicaNodes'           => [BinarySchema::TYPE_INT32],
            'isrNodes'               => [BinarySchema::TYPE_INT32],
            'eligibleLeaderReplicas' => [BinarySchema::TYPE_INT32, BinarySchema::FLAG_NULLABLE => true],
            'lastKnownElr'           => [BinarySchema::TYPE_INT32, BinarySchema::FLAG_NULLABLE => true],
            'offlineReplicas'        => [BinarySchema::TYPE_INT32],
        ];
    }
}
