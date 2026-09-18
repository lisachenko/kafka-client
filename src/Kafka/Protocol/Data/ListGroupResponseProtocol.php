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
 * One group of a ListGroups response
 *
 * <pre>
 *   ListGroupResponseProtocol => GroupId ProtocolType GroupState GroupType
 *     GroupId      => string
 *     ProtocolType => string
 *     GroupState   => string    -- since version 4
 *     GroupType    => string    -- since version 5
 * </pre>
 *
 * The class is named after the `main` branch; the Kafka sources call the structure
 * `LIST_GROUPS_RESPONSE_GROUP_V0` (Protocol.java @ 0.10.2.2) and `GroupOverview` (kafka/coordinator/GroupMetadata.scala).
 *
 * **Version 4 of the api (KIP-518, Kafka 2.6) added the `group_state`**, the very state a DescribeGroups answer
 * reports - one of the `STATE_*` constants of {@see DescribeGroupResponseMetadata} - so that an operator sees
 * which of the listed groups are `Empty`, `Stable` or rebalancing without describing each of them one by one. It
 * is the half of the KIP that the *answer* carries; the other half is the `states_filter` of the request.
 * {@see ListGroupResponseProtocolV0} is the entry of the versions 0 to 3, which have no such field.
 *
 * **Version 5 of the api (KIP-848, Kafka 3.8) appended the `group_type`**, one of the `TYPE_*` constants below:
 * the new group coordinator of Kafka 3.x runs the groups of the classic membership protocol and the groups of the
 * KIP-848 consumer protocol side by side, and until this field a listing could not tell them apart - the
 * `protocol_type` of both is `consumer`. It is the half of the KIP that the *answer* carries; the other half is
 * the `types_filter` of the request. {@see ListGroupResponseProtocolV4} is the entry of version 4, which has the
 * state and not the type.
 *
 * @see docs/protocol/3.9.md, section "ListGroups API (key 16, v0 to v5)"
 */
class ListGroupResponseProtocol implements BinarySchemaInterface
{
    /**
     * Version of the ListGroups API that this DTO decodes an entry of
     */
    public const int VERSION = 5;

    /**
     * A group of the **classic** membership protocol, the JoinGroup/SyncGroup/Heartbeat one of Kafka 0.9
     *
     * `Group.GroupType.CLASSIC` @ 3.9.2; the name is lower case on the wire, and the `types_filter` of the
     * request parses it case-insensitively.
     *
     * @since Version 5 of protocol
     */
    public const string TYPE_CLASSIC = 'classic';

    /**
     * A group of the **consumer** protocol of KIP-848, whose members drive the ConsumerGroupHeartbeat api
     *
     * `Group.GroupType.CONSUMER` @ 3.9.2.
     *
     * @since Version 5 of protocol
     */
    public const string TYPE_CONSUMER = 'consumer';

    /**
     * A **share** group of KIP-932, which is early access in Kafka 3.9 and out of scope of this line
     *
     * `Group.GroupType.SHARE` @ 3.9.2.
     *
     * @since Version 5 of protocol
     */
    public const string TYPE_SHARE = 'share';

    /**
     * What `Group.GroupType.parse()` @ 3.9.2 answers for a name it does not know, and no group ever is
     *
     * A `types_filter` that names it - or anything else the enum does not define - is a filter no group matches,
     * so the answer is the empty list and never an error.
     *
     * @since Version 5 of protocol
     */
    public const string TYPE_UNKNOWN = 'unknown';

    /**
     * The unique group identifier
     */
    public string $groupId;

    /**
     * Protocol type the group runs, `consumer` for a consumer group of Kafka 0.9
     */
    public string $protocolType;

    /**
     * State of the group, one of the `STATE_*` constants of {@see DescribeGroupResponseMetadata}
     *
     * The field is null for every version below 4, which does not carry it: a client that reads it has to treat
     * "not reported" and "the group has no state" as the same thing, which is what the broker means by both.
     *
     * @since Version 4 of protocol
     */
    public ?string $groupState = null;

    /**
     * Type of the group, one of the `TYPE_*` constants of this class
     *
     * The field is null for every version below 5, which does not carry it: such an answer says nothing about
     * the type, and not that the group has none.
     *
     * @since Version 5 of protocol
     */
    public ?string $groupType = null;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $scheme = [
            'groupId'      => BinarySchema::TYPE_STRING,
            'protocolType' => BinarySchema::TYPE_STRING,
        ];
        if (static::VERSION >= 4) {
            $scheme['groupState'] = BinarySchema::TYPE_STRING;
        }
        if (static::VERSION >= 5) {
            $scheme['groupType'] = BinarySchema::TYPE_STRING;
        }

        return $scheme;
    }
}
