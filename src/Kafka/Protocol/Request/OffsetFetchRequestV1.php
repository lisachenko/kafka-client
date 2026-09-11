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
 * OffsetFetch, version 1: the offsets are read from the `__consumer_offsets` topic of the cluster.
 *
 * <pre>
 *   OffsetFetch Request (Version: 1) => group_id [topics]
 *     group_id => STRING
 *     topics   => topic [partitions]     -- NOT nullable before version 2
 * </pre>
 *
 * The request is the one of version 0 with another version field in the header - `OFFSET_FETCH_REQUEST_V1 =
 * OFFSET_FETCH_REQUEST_V0` in `Protocol.java` @ 0.10.2.2 - and it differs from version 2 only in the topic array,
 * which is **not** nullable here: a `-1` size makes the broker close the connection instead of answering with the
 * offsets of every topic of the group. This class therefore only lowers the version constant that
 * {@see OffsetFetchRequest::getScheme()} follows.
 *
 * @see docs/protocol/2.8.md, section "OffsetFetch API (key 9, v0 to v7)"
 */
final class OffsetFetchRequestV1 extends OffsetFetchRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
