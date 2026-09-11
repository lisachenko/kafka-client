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
 * CreateTopics response, version 1: the topic errors with their message, without a throttle time (key 19)
 *
 * <pre>
 *   CreateTopics Response (Version: 1) => [topic_errors]
 *     topic_errors => topic error_code error_message
 * </pre>
 *
 * Version 2 (KIP-124, Kafka 0.11) put a `throttle_time_ms` in front of the array and left the entries alone, so this
 * class only lowers the version constant that {@see CreateTopicsResponse::getScheme()} follows; the entries are the
 * `TOPIC_ERROR` of version 1, with the `error_message` that {@see CreateTopicsResponseV0} does not have.
 *
 * @see docs/protocol/2.8.md, section "CreateTopics API (key 19, v0 to v6)"
 */
final class CreateTopicsResponseV1 extends CreateTopicsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
