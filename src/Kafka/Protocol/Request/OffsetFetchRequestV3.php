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
 * OffsetFetch request of version 3 (Kafka 0.11), the frame of version 4 with a lower version field
 *
 * <pre>
 *   OffsetFetch Request (Version: 2, 3 and 4) => group_id [topics]
 * </pre>
 *
 * Version 4 (KIP-219, Kafka 2.0) changed neither the request nor the answer: it only promises that this client
 * honours the `throttle_time_ms` of an answer itself, because a throttled broker of 2.0 and above answers first
 * and mutes the channel afterwards. The nullable topic array is the one of version 2.
 *
 * @see docs/protocol/2.8.md, sections "OffsetFetch API (key 9, v0 to v6)" and "Quotas and throttle time"
 */
final class OffsetFetchRequestV3 extends OffsetFetchRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
