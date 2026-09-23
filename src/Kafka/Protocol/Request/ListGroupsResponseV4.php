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

/**
 * ListGroups answer of version 4 (Kafka 2.6, KIP-518): the group state without the group type
 *
 * Version 5 (Kafka 3.8, KIP-848) appended the `group_type` to every entry of the `groups` array; this is the
 * answer of the version below, whose entries are {@see \Protocol\Kafka\Protocol\Data\ListGroupResponseProtocolV4}
 * and whose {@see \Protocol\Kafka\Protocol\Data\ListGroupResponseProtocol::$groupType} stays null.
 * {@see ListGroupsResponse} decodes the answer with the type.
 *
 * @see docs/protocol/4.3.md, section "ListGroups API (key 16, v0 to v5)"
 */
final class ListGroupsResponseV4 extends ListGroupsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
