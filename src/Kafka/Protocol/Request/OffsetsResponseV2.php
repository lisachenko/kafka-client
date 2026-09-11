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
 * Offsets (ListOffset) response object (key 2, v2)
 *
 * <pre>
 *   ListOffsets Response (Version: 2) => throttle_time_ms [responses]
 * </pre>
 *
 * The frame of version 2 (KIP-124, Kafka 0.11) is the frame of version 3, byte for byte - the throttle time in
 * front of the topics, one offset and one timestamp per partition. The two versions differ only in what the
 * client promises about the throttle time of KIP-219, see {@see OffsetsResponse}.
 *
 * @see docs/protocol/2.8.md, sections "Offsets API (key 2, v0 to v5), a.k.a. ListOffset" and
 *      "Quotas and throttle time"
 */
final class OffsetsResponseV2 extends OffsetsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
