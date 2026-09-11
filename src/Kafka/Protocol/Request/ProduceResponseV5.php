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
 * Produce response object, version 5
 *
 * <pre>
 *   ProduceResponse (Version: 5) => [TopicName [Partition ErrorCode Offset LogAppendTime LogStartOffset]]
 *                                   ThrottleTime
 * </pre>
 *
 * The frame of version 5 (Kafka 1.0) is the frame of version 6, byte for byte: `ProduceResponse.json` @ 2.8.2 has
 * no field of version 6 at all, so the two classes decode the same bytes and differ only in the version of the
 * request they belong to. What version 6 adds is the client's promise to honour the `ThrottleTime` itself, see
 * {@see ProduceResponse}.
 *
 * @see docs/protocol/2.8.md, section "Produce API (key 0, v0 to v8)"
 */
final class ProduceResponseV5 extends ProduceResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 5;
}
