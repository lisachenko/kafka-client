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
 * OffsetFetch response, version 3: the throttle time, the topics and the group error, the answer of version 4 too
 *
 * <pre>
 *   OffsetFetch Response (Version: 3 and 4) => throttle_time_ms [responses] error_code
 * </pre>
 *
 * @see docs/protocol/2.8.md, sections "OffsetFetch API (key 9, v0 to v5)" and "Quotas and throttle time"
 */
final class OffsetFetchResponseV3 extends OffsetFetchResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
