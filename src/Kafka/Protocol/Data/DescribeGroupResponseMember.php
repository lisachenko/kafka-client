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
 *   DescribeGroupResponseMember => MemberId ClientId ClientHost MemberMetadata MemberAssignment
 *     MemberId         => string
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
 * @see docs/protocol/0.10.2.md, section "DescribeGroups API (key 15, v0)"
 */
class DescribeGroupResponseMember implements BinarySchemaInterface
{
    /**
     * The memberId assigned by the coordinator
     */
    public string $memberId;

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
        return [
            'memberId'         => BinarySchema::TYPE_STRING,
            'clientId'         => BinarySchema::TYPE_STRING,
            'clientHost'       => BinarySchema::TYPE_STRING,
            'memberMetadata'   => BinarySchema::TYPE_BYTEARRAY,
            'memberAssignment' => BinarySchema::TYPE_BYTEARRAY,
        ];
    }
}
