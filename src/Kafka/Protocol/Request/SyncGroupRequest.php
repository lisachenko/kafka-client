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

use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\SyncGroupRequestMember;

/**
 * SyncGroup, version 0: the request with which the leader of a group publishes the state of the new generation.
 *
 * All members send SyncGroup immediately after they joined the group, but only the leader provides the assignment
 * of the group; every other member sends an empty assignment array and receives its own share in the answer. The
 * coordinator holds the answers of the followers until the leader has sent its assignment.
 *
 * <pre>
 *   SyncGroup Request (Version: 0) => group_id generation_id member_id [group_assignment]
 *     group_id         => STRING
 *     generation_id    => INT32
 *     member_id        => STRING
 *     group_assignment => member_id member_assignment
 *       member_id         => STRING
 *       member_assignment => BYTES
 * </pre>
 *
 * Version 0 is the only version a Kafka 0.10.2.2 broker serves; the `throttle_time_ms` that the answer of version 1
 * carries arrived with Kafka 0.10.1.
 *
 * @see docs/protocol/0.11.0.md, section "SyncGroup API (key 14, v0)"
 */
class SyncGroupRequest extends AbstractRequest
{
    /**
     * Assignment of each member of the group, indexed by the member id
     *
     * @var array<string, SyncGroupRequestMember>
     */
    protected readonly array $groupAssignments;

    /**
     * A value of the `$groupAssignments` map is either the raw assignment of that member or an already built
     * {@see SyncGroupRequestMember}; the assignment itself is opaque to this api.
     *
     * @param string                                            $consumerGroup    The consumer group id
     * @param int                                               $generationId     The generation of the group
     * @param string                                            $memberId         The member id of the sender
     * @param array<string, string|SyncGroupRequestMember>      $groupAssignments Assignment of every member, sent by
     *        the leader of the group and left empty by every other member
     * @param string                                            $clientId         Unique client identifier
     * @param int                                               $correlationId    Correlated request id
     */
    public function __construct(
        /**
         * The consumer group id.
         */
        protected readonly string $consumerGroup,
        /**
         * The generation of the group, as the JoinGroup response reported it.
         */
        protected readonly int $generationId,
        /**
         * The member id assigned by the group coordinator.
         */
        protected readonly string $memberId,
        array $groupAssignments = [],
        string $clientId = '',
        int $correlationId = 0
    ) {
        $packedGroupAssignments = [];
        foreach ($groupAssignments as $groupMemberId => $memberAssignment) {
            $packedGroupAssignments[$groupMemberId] = $memberAssignment instanceof SyncGroupRequestMember
                ? $memberAssignment
                : new SyncGroupRequestMember((string) $groupMemberId, $memberAssignment);
        }
        $this->groupAssignments = $packedGroupAssignments;

        parent::__construct(ApiKeys::SYNC_GROUP, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'consumerGroup'    => BinarySchema::TYPE_STRING,
            'generationId'     => BinarySchema::TYPE_INT32,
            'memberId'         => BinarySchema::TYPE_STRING,
            'groupAssignments' => ['memberId' => SyncGroupRequestMember::class],
        ];
    }
}
