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
 * DescribeConfigs request of version 1 (Kafka 1.1), the frame of version 2 with a lower version field
 *
 * <pre>
 *   DescribeConfigs Request (Version: 1) => [resources] include_synonyms
 * </pre>
 *
 * `DESCRIBE_CONFIGS_REQUEST_V2 = DESCRIBE_CONFIGS_REQUEST_V1` in `Protocol.java` @ 2.0.1.
 * Kafka 2.0 raised the api by one version without touching a single byte of the frame, so this class only lowers
 * the version constant that {@see DescribeConfigsRequest::getScheme()} follows.
 *
 * What the higher version buys is the promise of KIP-219: a client that sends it honours `throttle_time_ms`
 * itself, so a 2.8.2 broker answers a throttled request of it FIRST and mutes the channel afterwards
 * (`RequestHandlerHelper.sendResponseMaybeThrottle` @ 2.8.2: "Regardless of throttling, send the response
 * immediately") instead of holding the answer back for the throttle time.
 *
 * @see docs/protocol/2.8.md, section "DescribeConfigs API (key 32, v0, v1 and v2)"
 */
final class DescribeConfigsRequestV1 extends DescribeConfigsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
