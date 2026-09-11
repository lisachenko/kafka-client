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
 * DescribeConfigs response of version 1 (Kafka 1.1), the frame of version 2 with a lower version field
 *
 * <pre>
 *   DescribeConfigs Response (Version: 1) => throttle_time_ms [resources]
 * </pre>
 *
 * `DESCRIBE_CONFIGS_RESPONSE_V2 = DESCRIBE_CONFIGS_RESPONSE_V1` in `Protocol.java` @ 2.0.1.
 * Kafka 2.0 raised the api by one version without touching a single byte of the frame, so this class only lowers
 * the version constant that {@see DescribeConfigsResponse::getScheme()} follows.
 *
 * The answer of version 2 is this one, sent for a request that carried the higher version field; what
 * KIP-219 changed is the MOMENT it arrives - a throttled client of version 2 is answered first and muted
 * afterwards, and waits `throttle_time_ms` out itself.
 *
 * @see docs/protocol/2.8.md, section "DescribeConfigs API (key 32, v0, v1 and v2)"
 */
final class DescribeConfigsResponseV1 extends DescribeConfigsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
