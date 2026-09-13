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
 * OffsetFetch response, version 2: the topics and the group error code, without a throttle time (key 9)
 *
 * <pre>
 *   OffsetFetch Response (Version: 2) => [responses] error_code
 * </pre>
 *
 * Version 3 (KIP-124, Kafka 0.11) put a `throttle_time_ms` in front of the topics array and changed nothing else, so
 * this class only lowers the version constant that {@see OffsetFetchResponse::getScheme()} follows. The group-level
 * `error_code` that version 2 appended is still read here, which is what separates this class from
 * {@see OffsetFetchResponseV1}.
 *
 * @see docs/protocol/2.8.md, section "OffsetFetch API (key 9, v0 to v7)"
 */
final class OffsetFetchResponseV2 extends OffsetFetchResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
