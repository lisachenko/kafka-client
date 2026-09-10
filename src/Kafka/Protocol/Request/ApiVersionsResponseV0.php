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
 * The api keys and versions one broker serves, version 0 (key 18)
 *
 * <pre>
 *   ApiVersions Response (Version: 0) => error_code [api_versions]
 *     error_code   => INT16
 *     api_versions => api_key min_version max_version
 * </pre>
 *
 * Version 0 is the answer without the trailing `throttle_time_ms` that Kafka 0.11 appended, so this class only
 * lowers the version constant that {@see ApiVersionsResponse::getScheme()} follows. Two frames are read with it:
 *
 * * the answer to an {@see ApiVersionsRequestV0}, which every broker from Kafka 0.10.0 on serves;
 * * the answer to a request of a version the broker does **not** serve, whichever version that was. A 0.11.0.3
 *   broker writes it with `ApiVersionsResponse.unsupportedVersionSend()`, which hard-codes the version `0` - the
 *   error code 35 and an empty api array - so that a client that guessed too high can still read it.
 *
 * @see docs/protocol/1.1.md, section "ApiVersions API (key 18, v0 and v1)"
 */
final class ApiVersionsResponseV0 extends ApiVersionsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
