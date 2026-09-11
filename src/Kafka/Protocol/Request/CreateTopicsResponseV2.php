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
 * CreateTopics response of version 2 (Kafka 0.11), the frame of version 3 with a lower version field
 *
 * <pre>
 *   CreateTopics Response (Version: 2) => throttle_time_ms [topic_errors]
 * </pre>
 *
 * `CREATE_TOPICS_RESPONSE_V3 = CREATE_TOPICS_RESPONSE_V2` in `Protocol.java` @ 2.0.1.
 * Kafka 2.0 raised the api by one version without touching a single byte of the frame, so this class only lowers
 * the version constant that {@see CreateTopicsResponse::getScheme()} follows.
 *
 * The answer of version 3 is this one, sent for a request that carried the higher version field; what
 * KIP-219 changed is the MOMENT it arrives - a throttled client of version 3 is answered first and muted
 * afterwards, and waits `throttle_time_ms` out itself.
 *
 * @see docs/protocol/2.8.md, section "CreateTopics API (key 19, v0 to v6)"
 */
final class CreateTopicsResponseV2 extends CreateTopicsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
