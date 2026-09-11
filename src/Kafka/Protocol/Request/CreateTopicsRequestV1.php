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
 * CreateTopics request of version 1 (Kafka 0.10.1), the frame of version 2 with a lower version field
 *
 * <pre>
 *   CreateTopics Request (Version: 1) => [create_topic_requests] timeout validate_only
 * </pre>
 *
 * `CREATE_TOPICS_REQUEST_V2 = CREATE_TOPICS_REQUEST_V1` in `Protocol.java` @ 0.11.0.3: the `validate_only` flag of
 * version 1 is the last field of both versions, and only the answer of version 2 is different
 * ({@see CreateTopicsResponseV1}).
 *
 * @see docs/protocol/2.8.md, section "CreateTopics API (key 19, v0 to v6)"
 */
final class CreateTopicsRequestV1 extends CreateTopicsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
