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
 * The api keys and versions one broker serves, version 1 (key 18)
 *
 * <pre>
 *   ApiVersions Response (Version: 1) => error_code [api_versions] throttle_time_ms
 * </pre>
 *
 * Version 1 is the answer Kafka 0.11 added with the trailing `throttle_time_ms` of KIP-124, and version 2 - the
 * KIP-219 bump of Kafka 2.0 that {@see ApiVersionsResponse} reads - is the very same frame: this class only lowers
 * the version constant that the version-aware `getScheme()` follows, so that the version 1 wire vectors are
 * replayed through the class of their own version.
 *
 * What the two versions really differ in is the behaviour of a **throttled** broker, which no frame shows: a broker
 * that throttles a version 2 request answers first and mutes the channel afterwards, where version 1 was answered
 * only at the end of the throttle window.
 *
 * @see docs/protocol/2.8.md, section "ApiVersions API (key 18, v0 to v2)"
 */
final class ApiVersionsResponseV1 extends ApiVersionsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
