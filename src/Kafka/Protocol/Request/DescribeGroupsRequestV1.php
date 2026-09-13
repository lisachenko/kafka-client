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
 * DescribeGroups request of version 1 (Kafka 0.11), the frame of version 2 with a lower version field
 *
 * <pre>
 *   DescribeGroups Request (Version: 0, 1 and 2) => [group_ids]
 * </pre>
 *
 * Version 2 (KIP-219, Kafka 2.0) added nothing to the group array; the first field the api gains afterwards is the
 * `include_authorized_operations` of version 3 (KIP-430, Kafka 2.3). The answer is unchanged as well and is read
 * with {@see DescribeGroupsResponseV1}.
 *
 * @see docs/protocol/2.8.md, sections "DescribeGroups API (key 15, v0 to v5)" and "Quotas and throttle time"
 */
final class DescribeGroupsRequestV1 extends DescribeGroupsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
