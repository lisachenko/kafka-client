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
 * One member of a group as the JoinGroup answer of the versions 0 to 4 reports it
 *
 * <pre>
 *   JoinGroupResponseMember => MemberId MemberMetadata
 *     MemberId       => string
 *     MemberMetadata => bytes
 * </pre>
 *
 * Version 5 (KIP-345, Kafka 2.3) inserted the nullable `group_instance_id` of a static member between the member
 * id and the metadata, see {@see JoinGroupResponseMember}; this is the entry without it.
 *
 * @see docs/protocol/2.8.md, section "Static membership (KIP-345)"
 */
final class JoinGroupResponseMemberV0 extends JoinGroupResponseMember
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
