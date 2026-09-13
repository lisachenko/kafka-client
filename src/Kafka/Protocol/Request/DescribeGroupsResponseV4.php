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
 * DescribeGroups response, version 4 (Kafka 2.4, KIP-345): the members carry their instance id, plainly encoded
 *
 * This is the version that completes static membership on the administrative side: `kafka-consumer-groups.sh
 * --describe` shows which member is which *instance* from here on. Version 5 (KIP-482) is the same answer in the
 * flexible encoding, which {@see DescribeGroupsResponse} decodes.
 *
 * @see docs/protocol/2.8.md, section "The flexible versions of the group apis (Kafka 2.4)"
 * @see docs/protocol/2.8.md, section "DescribeGroups API (key 15, v0 to v5)"
 */
final class DescribeGroupsResponseV4 extends DescribeGroupsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
