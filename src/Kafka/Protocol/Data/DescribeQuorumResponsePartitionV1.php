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
 * The quorum of one partition in a DescribeQuorum **v1** answer
 *
 * The partition of {@see DescribeQuorumResponsePartition} without the `ErrorMessage` that KIP-853 put behind its
 * error code: a version 1 answer carries the code alone, so {@see DescribeQuorumResponsePartition::$errorMessage}
 * stays null here. What this version selects on top of that is
 * {@see DescribeQuorumResponseReplicaStateV1}, the replica state with the two timestamps of KIP-836 and without a
 * directory id.
 *
 * @see docs/protocol/3.9.md, sections "DescribeQuorum API (key 55, v0 to v2)" and "The nodes, the directory ids and the error messages of KIP-853 (v2)"
 */
final class DescribeQuorumResponsePartitionV1 extends DescribeQuorumResponsePartition
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
