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

namespace Protocol\Kafka\Protocol\Request;

/**
 * DescribeQuorum answer of version 1 (Kafka 3.3, KIP-836): the quorum without the additions of KIP-853
 *
 * The frame of {@see DescribeQuorumResponse} without the four things Kafka 3.9 added to it - the top-level
 * `ErrorMessage`, the `ErrorMessage` of a partition, the `ReplicaDirectoryId` of a replica state and the
 * top-level `Nodes` array - so {@see DescribeQuorumResponse::$errorMessage} stays null here and
 * {@see DescribeQuorumResponse::$nodes} stays empty. Its partitions are
 * {@see \Protocol\Kafka\Protocol\Data\DescribeQuorumResponsePartitionV1} and its replica states carry the two
 * timestamps of KIP-836 alone.
 *
 * @see docs/protocol/4.3.md, sections "DescribeQuorum API (key 55, v0 to v2)" and "The nodes, the directory ids and the error messages of KIP-853 (v2)"
 */
final class DescribeQuorumResponseV1 extends DescribeQuorumResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
