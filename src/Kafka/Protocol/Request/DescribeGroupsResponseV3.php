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
 * DescribeGroups response, version 3: the group entries with their authorized operations and members without
 * an instance id
 *
 * Version 4 (Kafka 2.4, KIP-345) gave every member entry a nullable `group_instance_id` behind its member id,
 * see {@see DescribeGroupsResponseV4}; this is the answer of version 3 alone.
 *
 * @see docs/protocol/2.8.md, section "DescribeGroups API (key 15, v0 to v5)"
 */
final class DescribeGroupsResponseV3 extends DescribeGroupsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
