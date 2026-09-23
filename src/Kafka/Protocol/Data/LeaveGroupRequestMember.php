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
use Protocol\Kafka\Protocol\Request\JoinGroupRequest;

/**
 * One member of the batch that a LeaveGroup request removes from its group (KIP-345, Kafka 2.4)
 *
 * <pre>
 *   MemberIdentity => MemberId GroupInstanceId Reason
 *     MemberId        => string
 *     GroupInstanceId => nullable_string
 *     Reason          => nullable_string   -- since version 5
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
 * **Version 5 of the api (KIP-800, Kafka 3.2) appended the nullable `reason`** to every entry, "the reason why
 * the member left the group" - `LeaveGroupRequest.json` @ 3.2.3 - which the coordinator writes into the log line
 * of the member it removes and nowhere else. It is cut off at
 * {@see \Protocol\Kafka\Protocol\Request\JoinGroupRequest::MAX_REASON_LENGTH} characters, the same bound the
 * join reason of the same KIP has. {@see LeaveGroupRequestMemberV3} is the entry of the versions 3 and 4, which
 * have no such field.
 *
 * @see docs/protocol/3.9.md, section "The leave reason of KIP-800 (v5)"
 * @see docs/protocol/3.9.md, section "The batch leave of KIP-345 (v3)"
 */
class LeaveGroupRequestMember implements BinarySchemaInterface
{
    /**
     * Version of the LeaveGroup API that this DTO encodes an entry of
     */
    public const int VERSION = 5;

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

    /**
     * Why this member left the group, null when the caller names no reason
     *
     * @since Version 5 of protocol
     */
    public ?string $reason = null;

    public function __construct(
        string $memberId = self::UNKNOWN_MEMBER_ID,
        ?string $groupInstanceId = null,
        ?string $reason = null
    ) {
        $this->memberId        = $memberId;
        $this->groupInstanceId = $groupInstanceId;
        $this->reason          = $reason === null ? null : JoinGroupRequest::truncateReason($reason);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $scheme = [
            'memberId'        => BinarySchema::TYPE_STRING,
            'groupInstanceId' => BinarySchema::TYPE_NULLABLE_STRING,
        ];
        if (static::VERSION >= 5) {
            $scheme['reason'] = BinarySchema::TYPE_NULLABLE_STRING;
        }

        return $scheme;
    }
}
