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
 * DescribeGroups request of version 0 (Kafka 0.9), the frame of version 1 with a lower version field
 *
 * <pre>
 *   DescribeGroups Request (Version: 0) => [group_ids]
 * </pre>
 *
 * `DESCRIBE_GROUPS_REQUEST_V1 = DESCRIBE_GROUPS_REQUEST_V0` in `Protocol.java` @ 0.11.0.3: only the answer of
 * version 1 is different ({@see DescribeGroupsResponseV0}).
 *
 * @see docs/protocol/2.8.md, section "DescribeGroups API (key 15, v0 and v1)"
 */
final class DescribeGroupsRequestV0 extends DescribeGroupsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
