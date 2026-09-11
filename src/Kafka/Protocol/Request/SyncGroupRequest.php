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
 * SyncGroup, version 3: the request with which the leader of a group publishes the state of the new generation.
 *
 * All members send SyncGroup immediately after they joined the group, but only the leader provides the assignment
 * of the group; every other member sends an empty assignment array and receives its own share in the answer. The
 * coordinator holds the answers of the followers until the leader has sent its assignment.
 *
 * <pre>
 *   SyncGroup Request (Version: 0 to 3) => group_id generation_id member_id group_instance_id
 *                                            [group_assignment]
 *     group_id          => STRING
 *     generation_id     => INT32
 *     member_id         => STRING
 *     group_instance_id => NULLABLE_STRING   -- since version 3
 *     group_assignment  => member_id member_assignment
 *       member_id         => STRING
 *       member_assignment => BYTES
 * </pre>
 *
 * `SYNC_GROUP_REQUEST_V1 = SYNC_GROUP_REQUEST_V0` in `Protocol.java` @ 0.11.0.3: version 1 (KIP-124, Kafka 0.11)
 * added the `throttle_time_ms` to the ANSWER alone ({@see SyncGroupResponse}), so {@see SyncGroupRequestV0} puts
 * the same bytes on the wire and differs in the version field of the header only. Version 2 (KIP-219, Kafka 2.0)
 * did the same for the throttling contract and left the frame untouched, which is what
 * {@see SyncGroupRequestV1} sends. **Version 3 (KIP-345, Kafka 2.3) is the first one that changed the frame**:
 * it inserted the nullable `group_instance_id` behind the member id, with which a static member names itself, so
 * that the coordinator can tell a restarted instance from a second one that took its place - the second one
 * fences the first, whose requests are answered 82 (`FencedInstanceId`) afterwards. A dynamic member sends
 * `null`, which is the frame {@see SyncGroupRequestV2} sends with one field less.
 *
 * @see docs/protocol/2.8.md, section "SyncGroup API (key 14, v0 to v3)"
 */
class SyncGroupRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::SYNC_GROUP;

    /**
     * @inheritdoc
     */
    public const int VERSION = 3;

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
     * @param string|null                                       $groupInstanceId  `group.instance.id` of a static
     *        member (KIP-345), null for a dynamic one
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
        int $correlationId = 0,
        /**
         * Unique identifier of this consumer instance, `group.instance.id`, null for a dynamic member.
         *
         * @since Version 3 of protocol
         */
        protected readonly ?string $groupInstanceId = null
    ) {
        $packedGroupAssignments = [];
        foreach ($groupAssignments as $groupMemberId => $memberAssignment) {
            $packedGroupAssignments[$groupMemberId] = $memberAssignment instanceof SyncGroupRequestMember
                ? $memberAssignment
                : new SyncGroupRequestMember((string) $groupMemberId, $memberAssignment);
        }
        $this->groupAssignments = $packedGroupAssignments;

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        $body = [
            'consumerGroup' => BinarySchema::TYPE_STRING,
            'generationId'  => BinarySchema::TYPE_INT32,
            'memberId'      => BinarySchema::TYPE_STRING,
        ];
        if (static::VERSION >= 3) {
            $body['groupInstanceId'] = BinarySchema::TYPE_NULLABLE_STRING;
        }
        $body['groupAssignments'] = ['memberId' => SyncGroupRequestMember::class];

        return $header + $body;
    }
}
