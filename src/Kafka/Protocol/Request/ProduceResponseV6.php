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
 * Produce response object, version 6
 *
 * <pre>
 *   ProduceResponse (Version: 6) => [TopicName [Partition ErrorCode Offset LogAppendTime LogStartOffset]]
 *                                   ThrottleTime
 * </pre>
 *
 * The frame of version 6 (Kafka 2.0) is the frame of version 7, byte for byte: `ProduceResponse.json` @ 2.8.2 has
 * no field of version 7 at all - version 7 (Kafka 2.1, KIP-110) is a promise of the client about the *request* -
 * so the two classes decode the same bytes and differ only in the version of the request they belong to, see
 * {@see ProduceResponse}.
 *
 * @see docs/protocol/2.8.md, section "Produce API (key 0, v0 to v9)"
 */
final class ProduceResponseV6 extends ProduceResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 6;
}
