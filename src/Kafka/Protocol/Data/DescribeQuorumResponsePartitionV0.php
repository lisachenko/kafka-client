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

/**
 * The quorum of one partition in a DescribeQuorum **v0** answer
 *
 * The fields of the partition itself are the ones of {@see DescribeQuorumResponsePartition} - KIP-836 changed
 * nothing here - and what this version selects is the shape of its voters and observers:
 * {@see DescribeQuorumResponseReplicaStateV0}, the replica state without the two timestamps.
 *
 * @see docs/protocol/3.9.md, sections "DescribeQuorum API (key 55, v0 and v1)" and "The two timestamps of a
 *      replica state (v1, KIP-836)"
 */
final class DescribeQuorumResponsePartitionV0 extends DescribeQuorumResponsePartition
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
