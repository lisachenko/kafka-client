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
 * Offsets (ListOffset) response object (key 2, v0)
 *
 * <pre>
 *   OffsetResponse => [TopicName [Partition ErrorCode [Offset]]]
 *     TopicName => string
 *     Partition => int32
 *     ErrorCode => int16
 *     Offset    => int64
 * </pre>
 *
 * The answer of a version 0 request carries a list of segment start offsets per partition instead of the
 * `Timestamp`/`Offset` pair of version 1, so this class only lowers the version constant that
 * {@see OffsetsResponse::topicClass()} follows. Reading a version 0 answer with the version 1 class would run past
 * the end of the frame.
 *
 * @see docs/protocol/0.11.0.md, section "Offsets API (key 2, v0, v1 and v2), a.k.a. ListOffset"
 */
final class OffsetsResponseV0 extends OffsetsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
