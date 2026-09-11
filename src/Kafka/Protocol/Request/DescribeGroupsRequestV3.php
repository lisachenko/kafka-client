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
 * DescribeGroups request of version 3 (Kafka 2.3, KIP-430): the group array and the authorized-operations flag
 *
 * Version 4 (Kafka 2.4, KIP-345) changed the **answer** alone - every member entry of it gained a
 * `group_instance_id` - so this frame and the one of {@see DescribeGroupsRequestV4} are the same bytes with
 * another version number, and version 5 (KIP-482) is that frame in the flexible encoding, which
 * {@see DescribeGroupsRequest} sends.
 *
 * @see docs/protocol/2.8.md, section "DescribeGroups API (key 15, v0 to v5)"
 */
final class DescribeGroupsRequestV3 extends DescribeGroupsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
