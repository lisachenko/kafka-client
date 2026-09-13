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

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One member of the batch that a LeaveGroup v3 request removes from its group (KIP-345, Kafka 2.4)
 *
 * <pre>
 *   MemberIdentity => MemberId GroupInstanceId
 *     MemberId        => string
 *     GroupInstanceId => nullable_string
 * </pre>
 *
 * A member can be named in two ways, and `GroupCoordinator.handleLeaveGroup` @ 2.8.2 resolves them in this order:
 *
 * * by its **`group.instance.id`**, with {@see self::UNKNOWN_MEMBER_ID} as the member id - the way an
 *   administrator removes a *static* member it never saw the member id of
 *   ({@see \Protocol\Kafka\Admin\AdminClient::removeMembersFromConsumerGroup()});
 * * by its **member id**, with a null instance id - the way a member removes itself, which is what
 *   {@see \Protocol\Kafka\Client::leaveGroup()} sends.
 *
 * Naming both is legal and is how a static member leaves on its own: the coordinator then checks that the member
 * id really belongs to that instance and answers **82** (`FencedInstanceId`) when another consumer has taken the
 * instance over.
 *
 * @see docs/protocol/2.8.md, section "The batch leave of KIP-345 (v3)"
 */
final class LeaveGroupRequestMember implements BinarySchemaInterface
{
    /**
     * Member id of an entry that names its member by the `group.instance.id` alone
     *
     * `JoinGroupRequest.UNKNOWN_MEMBER_ID` @ 2.8.2, the empty string.
     */
    public const string UNKNOWN_MEMBER_ID = '';

    /**
     * Member id to remove from the group, {@see self::UNKNOWN_MEMBER_ID} when the instance id names the member
     */
    public string $memberId;

    /**
     * `group.instance.id` of the member to remove, null for a dynamic member
     */
    public ?string $groupInstanceId;

    public function __construct(string $memberId = self::UNKNOWN_MEMBER_ID, ?string $groupInstanceId = null)
    {
        $this->memberId        = $memberId;
        $this->groupInstanceId = $groupInstanceId;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'memberId'        => BinarySchema::TYPE_STRING,
            'groupInstanceId' => BinarySchema::TYPE_NULLABLE_STRING,
        ];
    }
}
