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
 * What became of one member of a LeaveGroup v3 batch (KIP-345, Kafka 2.4)
 *
 * <pre>
 *   MemberResponse => MemberId GroupInstanceId ErrorCode
 *     MemberId        => string
 *     GroupInstanceId => nullable_string
 *     ErrorCode       => int16
 * </pre>
 *
 * The entries come back in the order of the request and **echo the identity that was sent**, not the one the
 * coordinator resolved: `memberLeaveError(leavingMember, error)` @ 2.8.2 builds every entry from the request
 * entry. The error code is the one of that member alone - 0 when it was removed, 25 (`UnknownMemberId`) when the
 * group does not have it, 82 (`FencedInstanceId`) when another consumer holds its instance id - and the top-level
 * error code of the answer stays 0 for all of them.
 *
 * @see docs/protocol/2.8.md, section "The batch leave of KIP-345 (v3)"
 */
final class LeaveGroupResponseMember implements BinarySchemaInterface
{
    /**
     * Member id of the entry, as the request sent it
     */
    public string $memberId;

    /**
     * `group.instance.id` of the entry, as the request sent it, null when it named no instance
     */
    public ?string $groupInstanceId;

    /**
     * Error code of this member alone
     */
    public int $errorCode;

    public function __construct(string $memberId, ?string $groupInstanceId, int $errorCode)
    {
        $this->memberId        = $memberId;
        $this->groupInstanceId = $groupInstanceId;
        $this->errorCode       = $errorCode;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'memberId'        => BinarySchema::TYPE_STRING,
            'groupInstanceId' => BinarySchema::TYPE_NULLABLE_STRING,
            'errorCode'       => BinarySchema::TYPE_INT16,
        ];
    }
}
