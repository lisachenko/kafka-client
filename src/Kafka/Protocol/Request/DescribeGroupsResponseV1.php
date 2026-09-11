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
 * DescribeGroups response, version 1: the throttle time and the group array, the answer of version 2 as well
 *
 * <pre>
 *   DescribeGroups Response (Version: 1 and 2) => throttle_time_ms [groups]
 * </pre>
 *
 * @see docs/protocol/2.8.md, sections "DescribeGroups API (key 15, v0 to v2)" and "Quotas and throttle time"
 */
final class DescribeGroupsResponseV1 extends DescribeGroupsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
