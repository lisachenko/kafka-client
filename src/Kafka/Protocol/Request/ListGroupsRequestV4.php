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
 * ListGroups request of version 4 (Kafka 2.6, KIP-518): the states filter alone
 *
 * Version 5 (Kafka 3.8, KIP-848) put a second array behind it, the `types_filter`, and gave every group entry of
 * the answer its `group_type`; this is the request of the version below, which names the states and nothing else.
 * The {@see ListGroupsRequest::$typesFilter} of such a frame is never on the wire, so a broker that speaks this
 * version lists every type it knows. {@see ListGroupsRequest} sends the frame with both filters.
 *
 * @see docs/protocol/4.3.md, section "ListGroups API (key 16, v0 to v5)"
 */
final class ListGroupsRequestV4 extends ListGroupsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
