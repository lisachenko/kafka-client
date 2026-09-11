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
 * The api keys and versions one broker serves, version 2 (key 18)
 *
 * <pre>
 *   ApiVersions Response (Version: 2) => error_code [api_versions] throttle_time_ms
 * </pre>
 *
 * The last answer of this api in the plain encoding: version 3 counts the api array compactly and appends the
 * tagged fields of KIP-584 ({@see ApiVersionsResponse}). Version 2 itself is the frame of version 1 - the KIP-219
 * bump of Kafka 2.0 changed when a throttled broker answers, not what it answers - so this class only lowers the
 * version constant that the version-aware `getScheme()` follows.
 *
 * @see docs/protocol/2.8.md, section "ApiVersions API (key 18, v0 to v3)"
 */
final class ApiVersionsResponseV2 extends ApiVersionsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
