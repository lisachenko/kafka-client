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
 * DeleteTopics request of version 0 (Kafka 0.10.1), the frame of version 1 with a lower version field
 *
 * <pre>
 *   DeleteTopics Request (Version: 0) => [topics] timeout
 * </pre>
 *
 * `DELETE_TOPICS_REQUEST_V1 = DELETE_TOPICS_REQUEST_V0` in `Protocol.java` @ 0.11.0.3: only the answer of version 1
 * is different ({@see DeleteTopicsResponseV0}).
 *
 * @see docs/protocol/2.8.md, section "DeleteTopics API (key 20, v0 to v3)"
 */
final class DeleteTopicsRequestV0 extends DeleteTopicsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
