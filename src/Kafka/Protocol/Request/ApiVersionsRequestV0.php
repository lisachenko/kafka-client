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
 * ApiVersions, version 0: the request of Kafka 0.10 (key 18)
 *
 * <pre>
 *   ApiVersions Request (Version: 0) =>
 * </pre>
 *
 * The frame of version 1 and the frame of version 0 differ in exactly one field, the `ApiVersion` of the request
 * header, because `API_VERSIONS_REQUEST_V1 = API_VERSIONS_REQUEST_V0` in `Protocol.java` @ 0.11.0.3 - both requests
 * are the header and nothing else. The version still matters: a version 0 request is answered with the version 0
 * layout, which has no `throttle_time_ms`, see {@see ApiVersionsResponseV0}.
 *
 * This class is what a client sends to a broker of the `0.10.x` line, which reports the api as v0 only, and it is
 * the class the version 0 wire vectors are replayed through.
 *
 * @see docs/protocol/2.8.md, section "ApiVersions API (key 18, v0 and v1)"
 */
final class ApiVersionsRequestV0 extends ApiVersionsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
