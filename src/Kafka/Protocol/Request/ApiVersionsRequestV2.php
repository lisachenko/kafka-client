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
 * ApiVersions, version 2: the KIP-219 bump of Kafka 2.0 (key 18)
 *
 * <pre>
 *   ApiVersions Request (Version: 2) =>
 * </pre>
 *
 * The last version of this api without a body, and the last one with the plain request header: version 3 is the
 * first flexible one ({@see ApiVersionsRequest}). What version 2 says is that the client honours a
 * `throttle_time_ms` itself, because a broker answers a throttled request of a bumped version before it mutes the
 * channel; the frame is the one of the versions 0 and 1, down to the byte.
 *
 * This class is what a client sends to a broker of Kafka 2.0 to 2.3, and it is the class the version 2 wire vectors
 * are replayed through.
 *
 * @see docs/protocol/2.8.md, section "ApiVersions API (key 18, v0 to v3)"
 */
final class ApiVersionsRequestV2 extends ApiVersionsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
