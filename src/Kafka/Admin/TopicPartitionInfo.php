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

use Protocol\Kafka\Protocol\Data\DescribeTopicPartitionsResponsePartition;

/**
 * One partition of a topic, as {@see AdminClient::describeTopicPartitions()} reports it
 *
 * `TopicPartitionInfo` of the Java admin client @ 3.9.2, which carries the partition index, the leader, the
 * replicas, the in-sync replicas and - since KIP-966 - the **eligible leader replicas** and the **last known
 * ELR**. Two fields of the wire entry have no place in the Java class and are kept here because this package has
 * them in {@see \Protocol\Kafka\Common\PartitionMetadata} as well: the `leaderEpoch` of KIP-320 and the
 * `offlineReplicas` of KIP-113.
 *
 * The broker ids are plain integers, not {@see \Protocol\Kafka\Common\Node} objects: a DescribeTopicPartitions
 * answer carries no endpoint at all, so resolving an id to a host needs a Metadata or DescribeCluster answer
 * next to this one.
 *
 * @see docs/protocol/4.3.md, section "DescribeTopicPartitions API (key 75, v0)"
 */
final class TopicPartitionInfo
{
    /**
     * Leader id of a partition that has no leader
     */
    public const int NO_LEADER = DescribeTopicPartitionsResponsePartition::NO_LEADER;

    /**
     * @param int            $partition       Index of the partition inside its topic
     * @param int            $leader          Broker id of the leader, {@see self::NO_LEADER} without one
     * @param int            $leaderEpoch     Leader epoch of the partition (KIP-320)
     * @param list<int>      $replicas        Every broker that hosts the partition
     * @param list<int>      $isr             The replicas that are in sync with the leader
     * @param list<int>|null $elr             Replicas that may be elected without losing a write (KIP-966)
     * @param list<int>|null $lastKnownElr    The eligible leader replicas of the previous election (KIP-966)
     * @param list<int>      $offlineReplicas Replicas whose broker or log directory is down
     */
    public function __construct(
        public readonly int $partition,
        public readonly int $leader,
        public readonly int $leaderEpoch,
        public readonly array $replicas,
        public readonly array $isr,
        public readonly ?array $elr,
        public readonly ?array $lastKnownElr,
        public readonly array $offlineReplicas
    ) {}

    /**
     * Builds the value object from the partition entry of a DescribeTopicPartitions answer
     */
    public static function fromResponsePartition(DescribeTopicPartitionsResponsePartition $partition): self
    {
        return new self(
            $partition->partitionIndex,
            $partition->leaderId,
            $partition->leaderEpoch,
            $partition->replicaNodes,
            $partition->isrNodes,
            $partition->eligibleLeaderReplicas,
            $partition->lastKnownElr,
            $partition->offlineReplicas
        );
    }

    /**
     * Whether a leader of this partition is known at the moment
     */
    public function hasLeader(): bool
    {
        return $this->leader !== self::NO_LEADER;
    }
}
