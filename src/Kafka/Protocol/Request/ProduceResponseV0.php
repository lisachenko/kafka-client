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
 * Produce response object, version 0
 *
 * <pre>
 *   ProduceResponse (Version: 0) => [TopicName [Partition ErrorCode Offset]]
 * </pre>
 *
 * The answer of a version 0 request carries neither a `ThrottleTime` nor the `LogAppendTime` and the
 * `LogStartOffset` of a partition, so this class only lowers the version constant that
 * {@see ProduceResponse::getScheme()} and {@see \Protocol\Kafka\Protocol\Data\ProduceResponseTopic} follow.
 * Reading a version 0 answer with a higher version class would run past the end of the frame.
 *
 * @see docs/protocol/1.1.md, section "Produce API (key 0, v0 to v3)"
 */
final class ProduceResponseV0 extends ProduceResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
