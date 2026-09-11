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
 * ListGroups request of version 2 (Kafka 2.0, KIP-219): the empty body, plainly encoded
 *
 * Version 3 (Kafka 2.4, KIP-482) added no field either, but its body is not empty any more: a flexible
 * structure always ends in its tagged-field section, so the frame of {@see ListGroupsRequest} carries the
 * single byte `00` where this one carries nothing at all.
 *
 * @see docs/protocol/2.8.md, section "The flexible versions of the group apis (Kafka 2.4)"
 * @see docs/protocol/2.8.md, section "ListGroups API (key 16, v0 to v4)"
 */
final class ListGroupsRequestV2 extends ListGroupsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
