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
 * DescribeGroups request of version 2 (Kafka 2.0, KIP-219): the group ids and nothing else
 *
 * <pre>
 *   DescribeGroups Request (Version: 0 to 2) => [group_ids]
 * </pre>
 *
 * Version 3 (KIP-430, Kafka 2.3) appended the boolean `include_authorized_operations`, with which a caller asks
 * the broker which operations the client may perform on each group; {@see DescribeGroupsRequest} sends that
 * frame, and this class is the request of every version below it.
 *
 * @see docs/protocol/2.8.md, section "The authorized operations of a group (v3, KIP-430)"
 */
final class DescribeGroupsRequestV2 extends DescribeGroupsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
