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

namespace Protocol\Kafka\Protocol\Data;

/**
 * One member of a group as the DescribeGroups answer of the versions 0 to 3 reports it
 *
 * <pre>
 *   DescribeGroupResponseMember => MemberId ClientId ClientHost MemberMetadata MemberAssignment
 * </pre>
 *
 * Version 4 (Kafka 2.4, KIP-345) inserted the nullable `group_instance_id` of a static member behind the member
 * id, see {@see DescribeGroupResponseMember}; this is the entry without it.
 *
 * @see docs/protocol/2.8.md, section "DescribeGroups API (key 15, v0 to v5)"
 */
final class DescribeGroupResponseMemberV0 extends DescribeGroupResponseMember
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
