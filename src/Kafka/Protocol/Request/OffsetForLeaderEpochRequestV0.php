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
 * OffsetForLeaderEpoch request, version 0 (key 23, Kafka 0.11)
 *
 * <pre>
 *   OffsetForLeaderEpoch Request (Version: 0) => [topics]
 *     topics => topic [partitions]
 *       topic      => STRING
 *       partitions => partition_id leader_epoch
 * </pre>
 *
 * The body of version 0 is the body of version 1, byte for byte: `OffsetForLeaderEpochRequest.json` @ 2.8.2 says
 * "Version 1 is the same as version 0", because KIP-279 only changed the answer. This class exists so that the
 * **answer** of version 0, which carries no `leader_epoch` per partition, can be asked for and decoded, see
 * {@see OffsetForLeaderEpochResponseV0}.
 *
 * @see docs/protocol/2.8.md, section "OffsetForLeaderEpoch API (key 23, v0 and v1)"
 */
final class OffsetForLeaderEpochRequestV0 extends OffsetForLeaderEpochRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
