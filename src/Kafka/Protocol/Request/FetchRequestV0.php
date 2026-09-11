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
 * Fetch API (key 1), version 0
 *
 * <pre>
 *   FetchRequest (Version: 0) => ReplicaId MaxWaitTime MinBytes [TopicName [Partition FetchOffset MaxBytes]]
 * </pre>
 *
 * The bytes of a version 0 request are the bytes of a version 1 request with another value in the `ApiVersion`
 * field of the header, so this class only lowers the version constant. What the version does change is the answer:
 * a version 0 request is answered without the `ThrottleTimeMs` prefix, see {@see FetchResponseV0}, and - like every
 * version below 2 - with a message set that the broker converted down to message format v0.
 *
 * @see docs/protocol/2.8.md, section "Fetch API (key 1, v0 to v8)"
 */
final class FetchRequestV0 extends FetchRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
