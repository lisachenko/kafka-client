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
 * Produce response object, version 1
 *
 * <pre>
 *   ProduceResponse (Version: 1) => [TopicName [Partition ErrorCode Offset]] ThrottleTime
 *     ThrottleTime => int32
 * </pre>
 *
 * The answer of a version 1 request carries the `ThrottleTime` of version 1, but neither the `LogAppendTime` nor
 * the `LogStartOffset` in its partition entries, so this class only lowers the version constant that
 * {@see ProduceResponse::getScheme()} and {@see \Protocol\Kafka\Protocol\Data\ProduceResponseTopic} follow.
 * Reading a version 1 answer with the version 2 class would take the throttle time for the append time of the
 * partition and run past the end of the frame.
 *
 * @see docs/protocol/1.1.md, section "Produce API (key 0, v0 to v3)"
 */
final class ProduceResponseV1 extends ProduceResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
