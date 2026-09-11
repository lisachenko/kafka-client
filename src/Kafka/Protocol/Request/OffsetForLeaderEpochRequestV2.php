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
 * OffsetForLeaderEpoch request of version 2 (key 23)
 *
 * The frame of version 2 (Kafka 2.1, KIP-320): the topics array, whose partition entries carry the
 * `current_leader_epoch` that fences a stale belief about the leadership, and **no `replica_id`** - version 3
 * (Kafka 2.3, KIP-392) put that at the head of the frame, see {@see OffsetForLeaderEpochRequest::$replicaId}. A
 * broker serves this version as it serves a follower.
 *
 * @see docs/protocol/2.8.md, section "OffsetForLeaderEpoch API (key 23, v0 to v4)"
 */
final class OffsetForLeaderEpochRequestV2 extends OffsetForLeaderEpochRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
