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
 * Offsets (ListOffset) response, version 1: one offset per partition, without a throttle time (key 2)
 *
 * <pre>
 *   ListOffsets Response (Version: 1) => [responses]
 *     responses => topic [partition_responses]
 *       partition_responses => partition error_code timestamp offset
 * </pre>
 *
 * Version 2 (KIP-124, Kafka 0.11) put a `throttle_time_ms` in front of the topics array and left the partitions
 * alone, so this class only lowers the version constant that {@see OffsetsResponse::getScheme()} follows; the
 * partition entries are the `timestamp offset` pair of version 1, not the offset array of
 * {@see OffsetsResponseV0}.
 *
 * @see docs/protocol/0.11.0.md, section "Offsets API (key 2, v0, v1 and v2), a.k.a. ListOffset"
 */
final class OffsetsResponseV1 extends OffsetsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
