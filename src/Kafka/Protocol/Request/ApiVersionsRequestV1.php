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
 * ApiVersions, version 1: the request of Kafka 0.11 (key 18)
 *
 * <pre>
 *   ApiVersions Request (Version: 1) =>
 * </pre>
 *
 * The frame of version 2 and the frame of version 1 differ in exactly one field, the `ApiVersion` of the request
 * header - "Versions 0 through 2 of ApiVersionsRequest are the same" (`ApiVersionsRequest.json` @ 2.8.2), all three
 * being the header and nothing else. What the version selects is the *answer* and the *behaviour* of a throttled
 * broker: version 1 gained the trailing `throttle_time_ms` of KIP-124 ({@see ApiVersionsResponseV1}), and version 2
 * is the KIP-219 bump of Kafka 2.0 that this client sends ({@see ApiVersionsRequest}).
 *
 * This class is what a client sends to a broker of the `0.11.x` or `1.x` line, which reports the api as v0 to v1
 * only, and it is the class the version 1 wire vectors are replayed through.
 *
 * @see docs/protocol/2.8.md, section "ApiVersions API (key 18, v0 to v2)"
 */
final class ApiVersionsRequestV1 extends ApiVersionsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
