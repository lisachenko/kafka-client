<?php

/*
 * This file is part of the lisachenko/kafka-client package.
 *
 * (c) Alexander Lisachenko <lisachenko.it@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare (strict_types=1);

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Consumer\MemberAssignment;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\SyncGroupRequestMember;

/**
 * SyncGroup Request
 *
 * The sync group request is used by the group leader to assign state (e.g. partition assignments) to all members of
 * the current generation. All members send SyncGroup immediately after joining the group, but only the leader provides
 * the group's assignment.
 *
 * SyncGroupRequest => GroupId GenerationId MemberId GroupAssignment
 *   GroupId => string
 *   GenerationId => int32
 *   MemberId => string
 *   GroupAssignment => [MemberId MemberAssignment]
 *     MemberId => string
 *     MemberAssignment => bytes
 */
class SyncGroupRequest extends AbstractRequest
{
    /**
     * @inheritDoc
     */
    protected const VERSION = 1;

    /**
     * List of group member assignments
     *
     * @var SyncGroupRequestMember[]
     */
    private readonly array $groupAssignments;

    /**
     * SyncGroupRequest constructor.
     *
     * @param string             $consumerGroup    The consumer group id
     * @param int                $generationId     The generation of the group
     * @param string|null        $memberId         The member id assigned by the group coordinator
     * @param MemberAssignment[] $groupAssignments List of group member assignments
     * @param string             $clientId         Client identifier
     * @param int                $correlationId    Correlated request ID
     */
    public function __construct(
        private readonly string $consumerGroup,
        private readonly int $generationId,
        private readonly ?string $memberId = null,
        array $groupAssignments = [],
        string $clientId = '',
        int $correlationId = 0
    ) {
        $packedGroupAssignments = [];
        foreach ($groupAssignments as $groupMemberId => $memberAssignment) {
            $packedGroupAssignments[$groupMemberId] = new SyncGroupRequestMember($groupMemberId, $memberAssignment);
        }
        $this->groupAssignments = $packedGroupAssignments;

        parent::__construct(ApiKeys::SYNC_GROUP, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = null;

        return $header + [
            'consumerGroup'    => BinarySchema::TYPE_STRING,
            'generationId'     => BinarySchema::TYPE_INT32,
            'memberId'         => BinarySchema::TYPE_NULLABLE_STRING,
            'groupAssignments' => ['memberId' => SyncGroupRequestMember::class],
        ];
    }
}
