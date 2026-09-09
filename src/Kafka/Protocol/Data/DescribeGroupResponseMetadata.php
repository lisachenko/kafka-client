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
 * The state is one of the constants below; `kafka/coordinator/GroupMetadata.scala` @ 0.9.0.1 defines exactly the
 * four states, and the coordinator answers a group it does not know with {@see self::STATE_DEAD} and the error
 * code 0, not with an error - a group only exists while it has members or committed offsets.
 *
 * @see docs/protocol/0.9.0.md, section "DescribeGroups API (key 15, v0)"
 */
class DescribeGroupResponseMetadata implements BinarySchemaInterface
{
    /**
     * The coordinator is collecting the members of the group and waits for their JoinGroup requests
     */
    public const string STATE_PREPARING_REBALANCE = 'PreparingRebalance';

    /**
     * Every member has joined and the coordinator waits for the SyncGroup request of the leader
     */
    public const string STATE_AWAITING_SYNC = 'AwaitingSync';

    /**
     * The members have their assignment and only send heartbeats
     */
    public const string STATE_STABLE = 'Stable';

    /**
     * The group has no members left, or the coordinator has never heard of it
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
