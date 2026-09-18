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

namespace Protocol\Kafka\Admin;

/**
 * The state of the metadata quorum, as {@see AdminClient::describeMetadataQuorum()} reports it
 *
 * `QuorumInfo` of the Java admin client: the leader of the raft quorum that replaces ZooKeeper in a KRaft
 * cluster (KIP-595), the epoch it leads in, how far the quorum has replicated the metadata log and what the
 * leader knows about every replica of it.
 *
 * A **voter** takes part in the election of the leader - the nodes of `controller.quorum.voters` - and an
 * **observer** replicates the log without a vote, which is what a broker that is not a controller does. On a
 * combined node (`process.roles=broker,controller`) there is one voter and no observer at all.
 *
 * @see docs/protocol/3.9.md, section "DescribeQuorum API (key 55, v0 and v1)"
 */
final class QuorumInfo
{
    /**
     * @param int                $leaderId     Id of the node that leads the quorum, -1 when it is unknown
     * @param int                $leaderEpoch  Epoch that leader leads in
     * @param int                $highWatermark Offset up to which the quorum has replicated the metadata log
     * @param list<ReplicaState> $voters       State of every voter of the quorum
     * @param list<ReplicaState> $observers    State of every observer of the quorum
     */
    public function __construct(
        public readonly int $leaderId,
        public readonly int $leaderEpoch,
        public readonly int $highWatermark,
        public readonly array $voters,
        public readonly array $observers
    ) {}

    /**
     * Returns the state of one node of the quorum, whether it votes or observes, null when it is not in it
     */
    public function replicaState(int $replicaId): ?ReplicaState
    {
        foreach ([...$this->voters, ...$this->observers] as $replica) {
            if ($replica->replicaId === $replicaId) {
                return $replica;
            }
        }

        return null;
    }
}
