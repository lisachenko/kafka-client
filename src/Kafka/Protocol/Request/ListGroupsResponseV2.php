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
 * ListGroups response, version 2: the groups of the broker that answered, plainly encoded
 *
 * Version 3 (Kafka 2.4, KIP-482) is the same answer in the flexible encoding, see {@see ListGroupsResponse}.
 *
 * @see docs/protocol/2.8.md, section "The flexible versions of the group apis (Kafka 2.4)"
 * @see docs/protocol/2.8.md, section "ListGroups API (key 16, v0 to v3)"
 */
final class ListGroupsResponseV2 extends ListGroupsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
