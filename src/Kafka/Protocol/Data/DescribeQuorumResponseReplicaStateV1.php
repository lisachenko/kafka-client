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
 * The replica state of a DescribeQuorum **v1** answer: the two timestamps of KIP-836, no directory id
 *
 * <pre>
 *   ReplicaState => ReplicaId LogEndOffset LastFetchTimestamp LastCaughtUpTimestamp
 *     ReplicaId             => INT32
 *     LogEndOffset          => INT64
 *     LastFetchTimestamp    => INT64
 *     LastCaughtUpTimestamp => INT64
 * </pre>
 *
 * Version 2 (KIP-853, Kafka 3.9) inserted a `ReplicaDirectoryId` between the id and the offset, see
 * {@see DescribeQuorumResponseReplicaState}; an entry of this class leaves
 * {@see DescribeQuorumResponseReplicaState::$replicaDirectoryId} at the zero uuid, because a version 1 answer does
 * not say which directory a replica keeps the metadata log in.
 *
 * @see docs/protocol/4.3.md, sections "DescribeQuorum API (key 55, v0 to v2)" and "The nodes, the directory ids and the error messages of KIP-853 (v2)"
 */
final class DescribeQuorumResponseReplicaStateV1 extends DescribeQuorumResponseReplicaState
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
