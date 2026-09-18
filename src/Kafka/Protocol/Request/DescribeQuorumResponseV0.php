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
 * DescribeQuorum answer of version 0 (Kafka 2.8, KIP-595): the quorum without the timestamps of KIP-836
 *
 * The frame of {@see DescribeQuorumResponse} with the shorter replica states
 * ({@see \Protocol\Kafka\Protocol\Data\DescribeQuorumResponseReplicaStateV0}): a voter or an observer is its id
 * and its log end offset, and when the leader last heard from it is not in the answer.
 *
 * @see docs/protocol/3.9.md, sections "DescribeQuorum API (key 55, v0 and v1)" and "The two timestamps of a
 *      replica state (v1, KIP-836)"
 */
final class DescribeQuorumResponseV0 extends DescribeQuorumResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
