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
 * Produce response object, version 2
 *
 * <pre>
 *   ProduceResponse (Version: 2) => [TopicName [Partition ErrorCode Offset LogAppendTime]] ThrottleTime
 *     LogAppendTime => int64
 *     ThrottleTime  => int32
 * </pre>
 *
 * The answer of a version 2 request is byte for byte the answer of a version 3 one - `PRODUCE_RESPONSE_V3` is
 * `PRODUCE_RESPONSE_V2` in `Protocol.java` @ 0.11.0.3 - so this class only lowers the version constant to the
 * version of the request it belongs to. It exists for the same reason {@see \Protocol\Kafka\Protocol\Request\FetchResponseV3}
 * does: the class of an answer states which request it was read back for.
 *
 * @see docs/protocol/0.11.0.md, section "Produce API (key 0, v0 to v3)"
 */
final class ProduceResponseV2 extends ProduceResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
