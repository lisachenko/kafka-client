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
 * OffsetForLeaderEpoch Response of version 1 (key 23, Kafka 2.0)
 *
 * Version 1 (KIP-279) gave the ANSWER the `leader_epoch` the end offset belongs to and left the request alone.
 * Version 2 (Kafka 2.1, KIP-320) changed both sides: the request partition gained a `current_leader_epoch` that
 * fences it, and the answer gained a leading `throttle_time_ms`, see {@see OffsetForLeaderEpochResponse}.
 *
 * @see docs/protocol/2.8.md, section "OffsetForLeaderEpoch API (key 23, v0 to v4)"
 */
final class OffsetForLeaderEpochResponseV1 extends OffsetForLeaderEpochResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
