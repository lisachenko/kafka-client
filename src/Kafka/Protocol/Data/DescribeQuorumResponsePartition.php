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
 * The quorum of one partition in a DescribeQuorum answer (key 55, Kafka 2.8, KIP-595)
 *
 * <pre>
 *   PartitionData => PartitionIndex ErrorCode LeaderId LeaderEpoch HighWatermark [CurrentVoters] [Observers]
 *     PartitionIndex => INT32
 *     ErrorCode      => INT16
 *     LeaderId       => INT32   (-1 when the leader is unknown)
 *     LeaderEpoch    => INT32
 *     HighWatermark  => INT64
 *     CurrentVoters  => COMPACT_ARRAY of {@see DescribeQuorumResponseReplicaState}
 *     Observers      => COMPACT_ARRAY of {@see DescribeQuorumResponseReplicaState}
 * </pre>
 *
 * The **voters** are the nodes that elect the leader of this raft partition - the
 * `controller.quorum.voters` of the cluster - and the **observers** are the nodes that replicate it without a
 * vote, i.e. the brokers of a cluster whose controllers are separate processes. A combined node
 * (`process.roles=broker,controller`) is a voter and nothing else, however many roles it plays.
 *
 * The version of the api selects the shape of those entries and nothing else here
 * ({@see self::replicaStateClass()}): version 1 (KIP-836, Kafka 3.3) added the two timestamps of a replica state,
 * see {@see DescribeQuorumResponsePartitionV0}.
 *
 * @see docs/protocol/3.9.md, section "DescribeQuorum API (key 55, v0 and v1)"
 */
class DescribeQuorumResponsePartition implements BinarySchemaInterface
{
    /**
     * Version of the DescribeQuorum API that this DTO is unpacked from
     */
    public const int VERSION = 1;

    /**
     * Index of the partition this quorum belongs to
     */
    public int $partitionIndex;

    /**
     * Error of this partition, 0 when the quorum could be described
     */
    public int $errorCode;

    /**
     * Id of the node that leads the quorum, -1 when the leader is unknown
     */
    public int $leaderId;

    /**
     * Latest known epoch of the leader of this raft partition
     */
    public int $leaderEpoch;

    /**
     * Offset up to which the quorum has replicated the partition
     */
    public int $highWatermark;

    /**
     * State of every voter of the quorum, indexed by the replica id
     *
     * @var array<int, DescribeQuorumResponseReplicaState>
     */
    public array $currentVoters = [];

    /**
     * State of every observer of the quorum, indexed by the replica id
     *
     * @var array<int, DescribeQuorumResponseReplicaState>
     */
    public array $observers = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $replicaState = static::replicaStateClass();

        return [
            'partitionIndex' => BinarySchema::TYPE_INT32,
            'errorCode'      => BinarySchema::TYPE_INT16,
            'leaderId'       => BinarySchema::TYPE_INT32,
            'leaderEpoch'    => BinarySchema::TYPE_INT32,
            'highWatermark'  => BinarySchema::TYPE_INT64,
            'currentVoters'  => ['replicaId' => $replicaState],
            'observers'      => ['replicaId' => $replicaState],
        ];
    }

    /**
     * Returns the class of a replica state for the version of the API that this DTO belongs to
     *
     * @return class-string<DescribeQuorumResponseReplicaState>
     */
    protected static function replicaStateClass(): string
    {
        return static::VERSION >= 1
            ? DescribeQuorumResponseReplicaState::class
            : DescribeQuorumResponseReplicaStateV0::class;
    }
}
