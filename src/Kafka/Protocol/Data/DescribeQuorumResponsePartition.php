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
 *   PartitionData => PartitionIndex ErrorCode ErrorMessage LeaderId LeaderEpoch HighWatermark
 *                    [CurrentVoters] [Observers]
 *     PartitionIndex => INT32
 *     ErrorCode      => INT16
 *     ErrorMessage   => NULLABLE_STRING   -- since version 2
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
 * The version of the api selects the shape of those entries ({@see self::replicaStateClass()}): version 1
 * (KIP-836, Kafka 3.3) added the two timestamps of a replica state and version 2 (KIP-853, Kafka 3.9) its
 * directory id, see {@see DescribeQuorumResponsePartitionV1} and {@see DescribeQuorumResponsePartitionV0}.
 *
 * **Version 2 added one field of its own here**, the `ErrorMessage` behind the `ErrorCode`: the text the leader
 * has for a partition it could not describe. A version 1 answer carries the code alone, so a client of it has to
 * know what the 3 of a topic that is not `__cluster_metadata` means; the node of this line fills the field with
 * `This server does not host this topic-partition.` for exactly that case, and with the **empty string** - not
 * with a null - when there was no error at all.
 *
 * @see docs/protocol/4.3.md, section "DescribeQuorum API (key 55, v0 to v2)"
 */
class DescribeQuorumResponsePartition implements BinarySchemaInterface
{
    /**
     * Version of the DescribeQuorum API that this DTO is unpacked from
     */
    public const int VERSION = 2;

    /**
     * Index of the partition this quorum belongs to
     */
    public int $partitionIndex;

    /**
     * Error of this partition, 0 when the quorum could be described
     */
    public int $errorCode;

    /**
     * Text of that error, the empty string when there was none and null when the answer left the field out
     *
     * @since Version 2 of protocol (Kafka 3.9, KIP-853)
     */
    public ?string $errorMessage = null;

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

        $scheme = [
            'partitionIndex' => BinarySchema::TYPE_INT32,
            'errorCode'      => BinarySchema::TYPE_INT16,
        ];
        if (static::VERSION >= 2) {
            $scheme['errorMessage'] = BinarySchema::TYPE_NULLABLE_STRING;
        }

        return $scheme + [
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
        return match (true) {
            static::VERSION >= 2 => DescribeQuorumResponseReplicaState::class,
            static::VERSION >= 1 => DescribeQuorumResponseReplicaStateV1::class,
            default              => DescribeQuorumResponseReplicaStateV0::class,
        };
    }
}
