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
 * OffsetCommit request of version 5 (Kafka 2.1, KIP-211): the frame of version 4 **without** `retention_time`
 *
 * <pre>
 *   OffsetCommit Request (Version: 5) => group_id generation_id member_id [topics]
 *     group_id      => STRING
 *     generation_id => INT32
 *     member_id     => STRING
 *     topics        => topic [partitions]
 * </pre>
 *
 * KIP-211 changed when the committed offsets of a group expire: not a fixed time after each commit any more, but
 * `offsets.retention.minutes` after the **group** became empty. The per-request retention had no place in that
 * model and the field was dropped from the frame - `RetentionTimeMs` has the versions `2-4` in
 * `OffsetCommitRequest.json` @ 2.8.2, it is not sent as -1. The `retentionTime` a caller passes to the
 * constructor is therefore simply not written by this version, and the broker stores the offset with the
 * `__consumer_offsets` value schema v3, which has no expiry field at all.
 *
 * @see docs/protocol/2.8.md, section "OffsetCommit API (key 8, v0 to v8)"
 */
final class OffsetCommitRequestV5 extends OffsetCommitRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 5;
}
