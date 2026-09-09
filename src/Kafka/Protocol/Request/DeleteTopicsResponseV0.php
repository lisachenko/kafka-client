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
 * DeleteTopics response, version 0: the topic error codes alone, without a throttle time (key 20)
 *
 * <pre>
 *   DeleteTopics Response (Version: 0) => [topic_error_codes]
 * </pre>
 *
 * Version 1 (KIP-124, Kafka 0.11) put a `throttle_time_ms` in front of the array; this class only lowers the version
 * constant that {@see DeleteTopicsResponse::getScheme()} follows.
 *
 * @see docs/protocol/0.11.0.md, section "DeleteTopics API (key 20, v0 and v1)"
 */
final class DeleteTopicsResponseV0 extends DeleteTopicsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
