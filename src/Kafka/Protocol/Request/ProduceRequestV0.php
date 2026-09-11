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
 * The produce API, version 0
 *
 * <pre>
 *   ProduceRequest (Version: 0) => RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]
 *     RequiredAcks => int16
 *     Timeout      => int32
 * </pre>
 *
 * The bytes of a version 0 request are the bytes of a version 2 request with another value in the `ApiVersion`
 * field of the header, so this class only lowers the version constant. What the version does change is the answer:
 * a version 0 request is answered without the `ThrottleTime` field and without the `LogAppendTime` of a
 * partition, see {@see ProduceResponseV0}.
 *
 * @see docs/protocol/2.8.md, section "Produce API (key 0, v0 to v7)"
 */
final class ProduceRequestV0 extends ProduceRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
