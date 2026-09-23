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

/**
 * One group of a ListGroups answer of version 4
 *
 * <pre>
 *   ListGroupResponseProtocol => GroupId ProtocolType GroupState
 * </pre>
 *
 * Version 5 (Kafka 3.8, KIP-848) appended the `group_type` of the group, see {@see ListGroupResponseProtocol};
 * this is the entry with the `group_state` of KIP-518 and without the type, whose
 * {@see ListGroupResponseProtocol::$groupType} stays null. {@see ListGroupResponseProtocolV0} is the entry of the
 * versions 0 to 3, which have neither.
 *
 * @see docs/protocol/3.9.md, section "ListGroups API (key 16, v0 to v5)"
 */
final class ListGroupResponseProtocolV4 extends ListGroupResponseProtocol
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
