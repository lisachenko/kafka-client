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
 * OffsetForLeaderEpoch response, version 0 (key 23, Kafka 0.11)
 *
 * <pre>
 *   OffsetForLeaderEpoch Response (Version: 0) => [topics]
 *     topics => topic [partitions]
 *       topic      => STRING
 *       partitions => error_code partition_id end_offset
 * </pre>
 *
 * The answer of version 0 names the end offset of the requested epoch and nothing else; version 1 (Kafka 2.0,
 * KIP-279) inserted the `leader_epoch` that the offset really belongs to between the partition id and the offset,
 * see {@see OffsetForLeaderEpochResponse}. This class decodes the frame the `0.11.x` and `1.x` lines captured, and
 * a 2.8.2 broker still answers it.
 *
 * @see docs/protocol/2.8.md, section "OffsetForLeaderEpoch API (key 23, v0 and v1)"
 */
final class OffsetForLeaderEpochResponseV0 extends OffsetForLeaderEpochResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
