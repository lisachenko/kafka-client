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
 * One member of the batch of a LeaveGroup v3 or v4 request: the identity without the `reason` of KIP-800
 *
 * <pre>
 *   MemberIdentity => MemberId GroupInstanceId
 *     MemberId        => string
 *     GroupInstanceId => nullable_string
 * </pre>
 *
 * Version 5 (KIP-800, Kafka 3.2) appended the nullable `reason` behind the instance id, see
 * {@see LeaveGroupRequestMember}; this is the entry of the two versions below it, whose
 * {@see LeaveGroupRequestMember::$reason} never reaches the wire.
 *
 * @see docs/protocol/3.9.md, section "The batch leave of KIP-345 (v3)"
 */
final class LeaveGroupRequestMemberV3 extends LeaveGroupRequestMember
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
