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
 * DeleteTopics request of version 3 (Kafka 2.1), the body of version 4 in the encoding before KIP-482
 *
 * <pre>
 *   DeleteTopics Request (Version: 0 to 3) => [topics] timeout_ms
 * </pre>
 *
 * Kafka 2.4 made the version 4 the first **flexible** one of this api, so the topic array and every topic name of
 * it travel compactly and the body ends in a tagged-field section. Not one field was added or removed with it,
 * which is why this class only lowers the version constant: the version 3 of Kafka 2.1 is what a broker below
 * Kafka 2.4 speaks, and it is the version whose number promises that the client understands the error code 73.
 *
 * @see docs/protocol/2.8.md, section "DeleteTopics API (key 20, v0 to v6)"
 */
final class DeleteTopicsRequestV3 extends DeleteTopicsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
