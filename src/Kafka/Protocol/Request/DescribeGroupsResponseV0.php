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
 * DescribeGroups response, version 0: the group array alone, without a throttle time (key 15)
 *
 * <pre>
 *   DescribeGroups Response (Version: 0) => [groups]
 * </pre>
 *
 * Version 1 (KIP-124, Kafka 0.11) put a `throttle_time_ms` in front of the array; this class only lowers the version
 * constant that {@see DescribeGroupsResponse::getScheme()} follows.
 *
 * @see docs/protocol/2.8.md, section "DescribeGroups API (key 15, v0 to v3)"
 */
final class DescribeGroupsResponseV0 extends DescribeGroupsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
