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
 * The produce API, version 1
 *
 * <pre>
 *   ProduceRequest (Version: 1) => RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]
 * </pre>
 *
 * The bytes of a version 1 request are the bytes of a version 2 request with another value in the `ApiVersion`
 * field of the header, so this class only lowers the version constant. What the version does change is the answer:
 * a version 1 request is answered without the `LogAppendTime` of every partition, see {@see ProduceResponseV1}.
 *
 * @see docs/protocol/1.1.md, section "Produce API (key 0, v0 to v3)"
 */
final class ProduceRequestV1 extends ProduceRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
