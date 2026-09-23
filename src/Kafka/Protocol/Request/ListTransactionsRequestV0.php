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
 * ListTransactions request of version 0 (Kafka 3.0): the two filters, without the duration of KIP-994
 *
 * The frame of {@see ListTransactionsRequest} **minus eight bytes**: version 1 (Kafka 3.8) appended the
 * `duration_filter` int64 behind the producer ids, and this is the version below it, which can bound a listing by
 * state and by producer id but never by age. A coordinator answers such a frame as if the filter were the
 * `"default": -1` of the field - `ListTransactionsRequestData.durationFilter()` @ 3.9.2 gives that default to
 * every version 0 request - so "no duration filter" and "version 0" are the same listing.
 *
 * @see docs/protocol/3.9.md, section "ListTransactions API (key 66, v0 and v1)"
 */
final class ListTransactionsRequestV0 extends ListTransactionsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
