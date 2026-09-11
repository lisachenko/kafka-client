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
 * DescribeGroups request of version 4 (Kafka 2.4, KIP-345): the last version with the plain encoding
 *
 * The request of version 4 is byte for byte the one of version 3 - what KIP-345 added at this version is the
 * `group_instance_id` of every **member** of the answer ({@see \Protocol\Kafka\Protocol\Data\DescribeGroupResponseMember}).
 * Version 5 (KIP-482) is the same frame written with the compact types and a tagged-field section, which
 * {@see DescribeGroupsRequest} sends.
 *
 * @see docs/protocol/2.8.md, section "The flexible versions of the group apis (Kafka 2.4)"
 * @see docs/protocol/2.8.md, section "DescribeGroups API (key 15, v0 to v5)"
 */
final class DescribeGroupsRequestV4 extends DescribeGroupsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
