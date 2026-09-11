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
 * DescribeGroups response, version 2: the throttle time and a group entry without `authorized_operations`
 *
 * <pre>
 *   DescribeGroups Response (Version: 1 and 2) => throttle_time_ms [groups]
 *     groups => error_code group_id state protocol_type protocol [members]
 * </pre>
 *
 * Version 3 (KIP-430, Kafka 2.3) appended a 32-bit `authorized_operations` bit set to every group entry, which
 * {@see DescribeGroupsResponse} decodes; the answer of the versions 1 and 2 is this one.
 *
 * @see docs/protocol/2.8.md, section "The authorized operations of a group (v3, KIP-430)"
 */
final class DescribeGroupsResponseV2 extends DescribeGroupsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
