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
 *   ListGroupResponseProtocol => GroupId ProtocolType
 *     GroupId      => string
 *     ProtocolType => string
 * </pre>
 *
 * The class is named after the `main` branch; the Kafka sources call the structure
 * `LIST_GROUPS_RESPONSE_GROUP_V0` (Protocol.java @ 0.9.0.1) and `GroupOverview` (kafka/coordinator/GroupMetadata.scala).
 *
 * @see docs/protocol/0.10.2.md, section "ListGroups API (key 16, v0)"
 */
class ListGroupResponseProtocol implements BinarySchemaInterface
{
    /**
     * The unique group identifier
     */
    public string $groupId;

    /**
     * Protocol type the group runs, `consumer` for a consumer group of Kafka 0.9
     */
    public string $protocolType;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'groupId'      => BinarySchema::TYPE_STRING,
            'protocolType' => BinarySchema::TYPE_STRING,
        ];
    }
}
