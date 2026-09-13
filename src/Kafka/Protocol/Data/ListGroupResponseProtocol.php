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
 *   ListGroupResponseProtocol => GroupId ProtocolType GroupState
 *     GroupId      => string
 *     ProtocolType => string
 *     GroupState   => string    -- since version 4
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
 * @see docs/protocol/2.8.md, section "ListGroups API (key 16, v0 to v4)"
 */
class ListGroupResponseProtocol implements BinarySchemaInterface
{
    /**
     * Version of the ListGroups API that this DTO decodes an entry of
     */
    public const int VERSION = 4;

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

        return $scheme;
    }
}
