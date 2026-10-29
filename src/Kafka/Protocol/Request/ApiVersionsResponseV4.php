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
 * The api keys and versions one broker serves, version 4 (key 18, Kafka 3.9, KAFKA-17011)
 *
 * <pre>
 *   ApiVersions Response (Version: 4) => error_code compact_[api_versions] throttle_time_ms TAG_BUFFER
 * </pre>
 *
 * The answer of the version 5 that {@see ApiVersionsResponse} reads, byte for byte: KIP-1242 added two fields to the
 * *request* and none to the answer ("Version 5 introduces ClusterId and NodeId checking and REBOOTSTRAP_REQUIRED
 * error", `ApiVersionsResponse.json` @ 4.4.0). What the version 5 adds is one more error code a version 4 is never
 * answered, the 129 of a misrouted connection. This class only lowers the version constant, so that the version 4
 * wire vectors are replayed through the class of their own version.
 *
 * @see docs/protocol/4.4.md, section "ApiVersions API (key 18, v0 to v5)"
 */
final class ApiVersionsResponseV4 extends ApiVersionsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
