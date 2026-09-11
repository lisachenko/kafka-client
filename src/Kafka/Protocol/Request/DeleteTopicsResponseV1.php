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
 * DeleteTopics response of version 1 (Kafka 0.11), the frame of version 2 with a lower version field
 *
 * <pre>
 *   DeleteTopics Response (Version: 1) => throttle_time_ms [topic_error_codes]
 * </pre>
 *
 * `DELETE_TOPICS_RESPONSE_V2 = DELETE_TOPICS_RESPONSE_V1` in `Protocol.java` @ 2.0.1.
 * Kafka 2.0 raised the api by one version without touching a single byte of the frame, so this class only lowers
 * the version constant that {@see DeleteTopicsResponse::getScheme()} follows.
 *
 * The answer of version 2 is this one, sent for a request that carried the higher version field; what
 * KIP-219 changed is the MOMENT it arrives - a throttled client of version 2 is answered first and muted
 * afterwards, and waits `throttle_time_ms` out itself.
 *
 * @see docs/protocol/2.8.md, section "DeleteTopics API (key 20, v0 to v4)"
 */
final class DeleteTopicsResponseV1 extends DeleteTopicsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
