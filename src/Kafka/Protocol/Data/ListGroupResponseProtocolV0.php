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
 * One group of a ListGroups answer of the versions 0 to 3
 *
 * <pre>
 *   ListGroupResponseProtocol => GroupId ProtocolType
 * </pre>
 *
 * Version 4 (Kafka 2.6, KIP-518) appended the `group_state` of the group, see {@see ListGroupResponseProtocol};
 * this is the entry without it, whose {@see ListGroupResponseProtocol::$groupState} stays null.
 *
 * @see docs/protocol/2.8.md, section "ListGroups API (key 16, v0 to v4)"
 */
final class ListGroupResponseProtocolV0 extends ListGroupResponseProtocol
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
