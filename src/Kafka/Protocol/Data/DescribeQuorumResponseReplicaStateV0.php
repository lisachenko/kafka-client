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
 * The replica state of a DescribeQuorum **v0** answer: the id and the offset, without the timestamps of KIP-836
 *
 * <pre>
 *   ReplicaState => ReplicaId LogEndOffset
 *     ReplicaId    => INT32
 *     LogEndOffset => INT64
 * </pre>
 *
 * Version 1 (KIP-836, Kafka 3.3) appended `LastFetchTimestamp` and `LastCaughtUpTimestamp`, see
 * {@see DescribeQuorumResponseReplicaState}; an entry of this class leaves both at
 * {@see DescribeQuorumResponseReplicaState::UNKNOWN_TIMESTAMP}, because a version 0 answer never says when a
 * replica was last heard from.
 *
 * @see docs/protocol/4.3.md, sections "DescribeQuorum API (key 55, v0 to v2)" and "The two timestamps of a
 *      replica state (v1, KIP-836)"
 */
final class DescribeQuorumResponseReplicaStateV0 extends DescribeQuorumResponseReplicaState
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
