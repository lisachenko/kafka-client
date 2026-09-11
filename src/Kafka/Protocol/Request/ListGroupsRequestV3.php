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
 * ListGroups request of version 3 (Kafka 2.4, KIP-482): the first flexible version of the api
 *
 * Version 4 (Kafka 2.6, KIP-518) gave the request its `states_filter` and every group entry of the answer its
 * `group_state`; this is the version without either, whose request body is nothing but one empty tag buffer.
 *
 * @see docs/protocol/2.8.md, section "The group states of KIP-518 (Kafka 2.6)"
 * @see docs/protocol/2.8.md, section "ListGroups API (key 16, v0 to v4)"
 */
final class ListGroupsRequestV3 extends ListGroupsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
