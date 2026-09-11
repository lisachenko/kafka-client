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
 * One member of a group, as reported by the DescribeGroups API
 *
 * <pre>
 *   DescribeGroupResponseMember => MemberId GroupInstanceId ClientId ClientHost MemberMetadata MemberAssignment
 *     MemberId         => string
 *     GroupInstanceId  => nullable_string   -- since version 4
 *     ClientId         => string
 *     ClientHost       => string
 *     MemberMetadata   => bytes
 *     MemberAssignment => bytes
 * </pre>
 *
 * Both byte arrays are opaque to this api: their content depends on the protocol type of the group. For the
 * `consumer` protocol type they hold the `Subscription` the member sent with its JoinGroup request and the
 * `MemberAssignment` the leader published with SyncGroup.
 *
 * **Version 4 of the api (KIP-345, Kafka 2.4) added the `group_instance_id`**, which is what completes static
 * membership on the administrative side: an operator sees which member id belongs to which *instance*, and that
 * instance id is what {@see \Protocol\Kafka\Admin\AdminClient::removeMembersFromConsumerGroup()} removes a
 * member by. It is `null` for a dynamic member, and {@see DescribeGroupResponseMemberV0} is the entry of the
 * versions 0 to 3, which have no such field.
 *
 * @see docs/protocol/2.8.md, section "DescribeGroups API (key 15, v0 to v5)"
 */
class DescribeGroupResponseMember implements BinarySchemaInterface
{
    /**
     * Version of the DescribeGroups API that this DTO decodes an entry of
     */
    public const int VERSION = 4;

    /**
     * The memberId assigned by the coordinator
     */
    public string $memberId;

    /**
     * `group.instance.id` of a static member (KIP-345), null for a dynamic one
     *
     * @since Version 4 of protocol
     */
    public ?string $groupInstanceId = null;

    /**
     * The client id used in the member's latest join group request
     */
    public string $clientId;

    /**
     * The client host used in the request session corresponding to the member's join group
     */
    public string $clientHost;

    /**
     * The metadata corresponding to the current group protocol in use (only present if the group is stable)
     */
    public string $memberMetadata;

    /**
     * The current assignment provided by the group leader (only present if the group is stable)
     */
    public string $memberAssignment;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $scheme = ['memberId' => BinarySchema::TYPE_STRING];
        if (static::VERSION >= 4) {
            $scheme['groupInstanceId'] = BinarySchema::TYPE_NULLABLE_STRING;
        }
        $scheme['clientId']         = BinarySchema::TYPE_STRING;
        $scheme['clientHost']       = BinarySchema::TYPE_STRING;
        $scheme['memberMetadata']   = BinarySchema::TYPE_BYTEARRAY;
        $scheme['memberAssignment'] = BinarySchema::TYPE_BYTEARRAY;

        return $scheme;
    }
}
