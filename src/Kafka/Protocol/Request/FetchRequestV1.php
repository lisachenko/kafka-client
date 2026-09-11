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
 * Fetch API (key 1), version 1
 *
 * <pre>
 *   FetchRequest (Version: 1) => ReplicaId MaxWaitTime MinBytes [TopicName [Partition FetchOffset MaxBytes]]
 * </pre>
 *
 * The body of a version 1 request is the body of a version 2 one, and the answer is the same frame as well, so this
 * class only lowers the version constant: the request-level `MaxBytes` of version 3 is not written. What the lower
 * version does change is the message format of the answer - a broker converts every message of format v1 down to
 * format v0 for a request below version 2, so the records of such an answer carry no timestamps at all - and the
 * fact that the broker guarantees no progress: a message that is bigger than the `MaxBytes` of its partition comes
 * back as an incomplete set instead of being returned in full, see {@see FetchRequest::$maxBytes}.
 *
 * @see docs/protocol/2.8.md, sections "Fetch API (key 1, v0 to v7)" and "MessageSet and Message"
 */
final class FetchRequestV1 extends FetchRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
