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
 * The frames of the versions 0, 1 and 2 differ in exactly one field, the `ApiVersion` of the request header -
 * "Versions 0 through 2 of ApiVersionsRequest are the same" (`ApiVersionsRequest.json` @ 2.8.2), all three being
 * the header and nothing else. The version still matters: a version 0 request is answered with the version 0
 * layout, which has no `throttle_time_ms`, see {@see ApiVersionsResponseV0}.
 *
 * This class is what a client sends to a broker of the `0.10.x` line, which reports the api as v0 only, and it is
 * the class the version 0 wire vectors are replayed through.
 *
 * @see docs/protocol/2.8.md, section "ApiVersions API (key 18, v0 to v3)"
 */
final class ApiVersionsRequestV0 extends ApiVersionsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
