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
use Protocol\Kafka\Protocol\Data\LeaveGroupRequestMember;
use Protocol\Kafka\Protocol\Data\LeaveGroupRequestMemberV3;

/**
 * LeaveGroup, version 5: removes one or more members from a group without waiting for their session timeout.
 *
 * This is preferred over letting the session timeout expire, since it lets the group rebalance right away - for a
 * consumer that means that less time elapses before its partitions can be reassigned to an active member.
 *
 * <pre>
 *   LeaveGroup Request (Version: 0, 1 and 2) => group_id member_id
 *     group_id  => STRING
 *     member_id => STRING
 *
 *   LeaveGroup Request (Version: 3 to 5) => group_id [members]
 *     group_id => STRING
 *     members  => member_id group_instance_id reason    -- the single member_id is GONE
 *       member_id         => STRING
 *       group_instance_id => NULLABLE_STRING
 *       reason            => NULLABLE_STRING   -- since version 5
 * </pre>
 *
 * `LEAVE_GROUP_REQUEST_V1 = LEAVE_GROUP_REQUEST_V0` in `Protocol.java` @ 0.11.0.3: version 1 (KIP-124, Kafka 0.11)
 * changed the answer alone ({@see LeaveGroupResponse}), so {@see LeaveGroupRequestV0} sends the same bytes, and
 * version 2 (KIP-219, Kafka 2.0) changed nothing at all but the throttling contract, so {@see LeaveGroupRequestV2}
 * and {@see LeaveGroupRequestV1} send them too.
 *
 * **Version 3 (Kafka 2.4, KIP-345) replaced the single `member_id` with a batch of member identities**, so that an
 * administrator can remove several members of a group with one request - the wire half of
 * {@see \Protocol\Kafka\Admin\AdminClient::removeMembersFromConsumerGroup()}, the `kafka-consumer-groups.sh
 * --delete-offsets`-style tooling of KIP-345 that exists because a *static* member does not leave on its own. Each
 * entry names its member by its member id, by its `group.instance.id` or by both, see
 * {@see LeaveGroupRequestMember}, and every entry is answered with an error code of its own. A member that removes
 * itself - which is what {@see \Protocol\Kafka\Client::leaveGroup()} does - sends a batch of exactly one entry.
 *
 * The frame this class writes is therefore a different one below and above version 3, and the constructor takes
 * either shape: a plain member id, which becomes a one-element batch, or the batch itself.
 *
 * **Version 5 (Kafka 3.2, KIP-800) gave every entry of the batch a nullable `reason`** - "Version 5 adds the
 * Reason field (KIP-800)" in `LeaveGroupRequest.json` @ 3.2.3 - "the reason why the member left the group", which
 * the coordinator logs and nothing more; the answer did not change with it ({@see LeaveGroupResponse}). The
 * entries of the versions 3 and 4 have no such field, so the constructor rebuilds every entry it is given into
 * the {@see \Protocol\Kafka\Protocol\Data\LeaveGroupRequestMember} class of **this** api version: a batch of
 * a {@see LeaveGroupRequestV4} therefore never writes a reason onto the wire, whatever the caller named.
 *
 * @see docs/protocol/3.9.md, section "The leave reason of KIP-800 (v5)"
 * @see docs/protocol/3.9.md, section "The batch leave of KIP-345 (v3)"
 */
class LeaveGroupRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::LEAVE_GROUP;

    /**
     * @inheritdoc
     */
    public const int VERSION = 5;

    /**
     * The first flexible version of the api (KIP-482, Kafka 2.4): every string, byte array and array of it
     * is compact and every structure of it ends in a tagged-field section.
     */
    public const int FLEXIBLE_VERSION = 4;

    /**
     * Members to remove from the group, one entry for the versions below 3
     *
     * @var list<LeaveGroupRequestMember>
     *
     * @since Version 3 of protocol
     */
    protected readonly array $members;

    /**
     * The member id of the first entry, which is the whole frame of the versions 0 to 2
     */
    protected readonly string $memberId;

    /**
     * @param string                                          $consumerGroup The consumer group id
     * @param string|iterable<LeaveGroupRequestMember|string> $members       The member that leaves, as its member
     *        id, or the batch of members to remove: a {@see LeaveGroupRequestMember} each, or a plain string, which
     *        names a member by its member id
     * @param string                                          $clientId      Unique client identifier
     * @param int                                             $correlationId Correlated request id
     * @param string|null                                     $reason        Why the members leave (KIP-800,
     *        version 5), applied to every entry that does not carry one of its own; null names none
     */
    public function __construct(
        /**
         * The consumer group id.
         */
        protected readonly string $consumerGroup,
        string|iterable $members,
        string $clientId = '',
        int $correlationId = 0,
        ?string $reason = null
    ) {
        $memberClass = static::memberClass();
        $batch       = [];
        foreach (is_string($members) ? [$members] : $members as $member) {
            $batch[] = $member instanceof LeaveGroupRequestMember
                ? new $memberClass($member->memberId, $member->groupInstanceId, $member->reason ?? $reason)
                : new $memberClass($member, null, $reason);
        }
        $this->members  = $batch;
        $this->memberId = $batch[0]->memberId ?? LeaveGroupRequestMember::UNKNOWN_MEMBER_ID;

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * Returns the members this request removes from the group
     *
     * @return list<LeaveGroupRequestMember>
     */
    public function getMembers(): array
    {
        return $this->members;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = ['consumerGroup' => BinarySchema::TYPE_STRING];
        if (static::VERSION >= 3) {
            $body['members'] = [static::memberClass()];
        } else {
            $body['memberId'] = BinarySchema::TYPE_STRING;
        }

        return $header + $body;
    }

    /**
     * Returns the class of a batch entry for the version of the API that this class sends
     *
     * @return class-string<LeaveGroupRequestMember>
     */
    protected static function memberClass(): string
    {
        return static::VERSION >= 5 ? LeaveGroupRequestMember::class : LeaveGroupRequestMemberV3::class;
    }
}
