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
 * Description of a single group, as reported by the DescribeGroups API
 *
 * <pre>
 *   DescribeGroupResponseMetadata => ErrorCode GroupId State ProtocolType Protocol [Members]
 *     ErrorCode    => int16
 *     GroupId      => string
 *     State        => string
 *     ProtocolType => string
 *     Protocol     => string
 *     Members      => MemberId ClientId ClientHost MemberMetadata MemberAssignment
 * </pre>
 *
 * The state is one of the constants below; `kafka/coordinator/group/GroupMetadata.scala` @ 1.1.1 defines exactly
 * the five states, and the coordinator answers a group it does not know with {@see self::STATE_DEAD} and the error
 * code 0, not with an error.
 *
 * Kafka 0.10.1 split "the group is gone" in two. A group whose last member left is no longer dropped at once, it
 * moves to {@see self::STATE_EMPTY} and lingers there with its committed offsets until `offsets.retention.minutes`
 * expires them; only then does it become {@see self::STATE_DEAD} and disappear from ListGroups. A 0.9.0.1
 * coordinator had no such state and answered `Dead` from the moment the last member had left.
 *
 * Kafka 1.0 **renamed** one of the five: the state between the last JoinGroup and the SyncGroup of the leader is
 * called {@see self::STATE_COMPLETING_REBALANCE} since then, where 0.9 to 0.11 called it
 * {@see self::STATE_AWAITING_SYNC}. Only the name on the wire changed, the state itself did not.
 *
 * @see docs/protocol/2.8.md, section "DescribeGroups API (key 15, v0 and v1)"
 */
class DescribeGroupResponseMetadata implements BinarySchemaInterface
{
    /**
     * The coordinator is collecting the members of the group and waits for their JoinGroup requests
     */
    public const string STATE_PREPARING_REBALANCE = 'PreparingRebalance';

    /**
     * Every member has joined and the coordinator waits for the SyncGroup request of the leader
     *
     * This is the name Kafka 1.0 gave the state (`CompletingRebalance` in
     * `kafka/coordinator/group/GroupMetadata.scala` @ 1.1.1) and the one a broker of this line answers with.
     */
    public const string STATE_COMPLETING_REBALANCE = 'CompletingRebalance';

    /**
     * The Kafka 0.9 to 0.11 name of {@see self::STATE_COMPLETING_REBALANCE}, the very same state
     *
     * `GroupMetadata.scala` called the state `AwaitingSync` until Kafka 1.0 renamed it; a broker of this line
     * never answers this string, and the constant is kept so that code written against the `0.9.x` to `0.11.x`
     * lines - and a client that talks to a broker of one of them - still compiles and still compares correctly.
     */
    public const string STATE_AWAITING_SYNC = 'AwaitingSync';

    /**
     * The members have their assignment and only send heartbeats
     */
    public const string STATE_STABLE = 'Stable';

    /**
     * The group has no members left but still holds committed offsets (Kafka 0.10.1 and later)
     *
     * A group reaches this state when its last member leaves, and a group that only uses Kafka to store offsets and
     * never joins is in it from the start. It answers JoinGroup normally, every other membership api with 25
     * (UnknownMemberId), and it is still listed by ListGroups.
     */
    public const string STATE_EMPTY = 'Empty';

    /**
     * The coordinator has never heard of the group, or has forgotten it because its offsets expired
     */
    public const string STATE_DEAD = 'Dead';

    /**
     * Error code for the group
     */
    public int $errorCode;

    /**
     * Name of the group
     */
    public string $groupId;

    /**
     * The current state of the group, one of the `STATE_*` constants, or an empty string when the broker that
     * answered is not the coordinator of the group and therefore knows nothing about it
     */
    public string $state;

    /**
     * The current group protocol type (empty if there is no active group), `consumer` for a consumer group
     */
    public string $protocolType;

    /**
     * The current group protocol, i.e. the assignor the members agreed on (only provided if the group is stable)
     */
    public string $protocol;

    /**
     * Current group members, indexed by the member id (only provided if the group is not dead)
     *
     * @var array<string, DescribeGroupResponseMember>
     */
    public array $members = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'errorCode'    => BinarySchema::TYPE_INT16,
            'groupId'      => BinarySchema::TYPE_STRING,
            'state'        => BinarySchema::TYPE_STRING,
            'protocolType' => BinarySchema::TYPE_STRING,
            'protocol'     => BinarySchema::TYPE_STRING,
            'members'      => ['memberId' => DescribeGroupResponseMember::class],
        ];
    }
}
