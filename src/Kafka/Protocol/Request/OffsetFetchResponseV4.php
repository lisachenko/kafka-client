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
 * OffsetFetch response, version 4: the answer of version 3, without the leader epoch of version 5
 *
 * <pre>
 *   OffsetFetch Response (Version: 3 and 4) => throttle_time_ms [responses] error_code
 * </pre>
 *
 * @see docs/protocol/2.8.md, section "OffsetFetch API (key 9, v0 to v5)"
 */
final class OffsetFetchResponseV4 extends OffsetFetchResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
