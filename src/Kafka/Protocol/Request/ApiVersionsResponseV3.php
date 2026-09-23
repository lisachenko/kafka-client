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
 * The api keys and versions one broker serves, version 3 (key 18, Kafka 2.4, KIP-511)
 *
 * <pre>
 *   ApiVersions Response (Version: 3) => error_code compact_[api_versions] throttle_time_ms TAG_BUFFER
 * </pre>
 *
 * The first flexible answer of this api and, apart from one value, the answer of the version 4 that
 * {@see ApiVersionsResponse} reads: Kafka 3.9 declares no field for its bump. What it changes is a **filter** on
 * the tagged field 0 - a broker drops every supported feature whose `min_version` is 0 from an answer below the
 * version 4 (KAFKA-17011) - so this class reads the same three tagged fields of KIP-584 and one supported feature
 * fewer on the KRaft node of this line, where `kraft.version` has the minimum 0.
 *
 * It is what a broker of Kafka 2.4 to 3.8 answers, and the class the version 3 wire vectors are replayed through.
 *
 * @see docs/protocol/3.9.md, section "ApiVersions API (key 18, v0 to v4)"
 */
final class ApiVersionsResponseV3 extends ApiVersionsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
