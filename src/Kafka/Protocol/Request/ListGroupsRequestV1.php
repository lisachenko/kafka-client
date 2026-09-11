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
 * ListGroups request of version 1 (Kafka 0.11), an empty body like every version below the flexible one
 *
 * <pre>
 *   ListGroups Request (Version: 0, 1 and 2) =>
 * </pre>
 *
 * Version 2 (KIP-219, Kafka 2.0) is the same empty body one api version higher; the api only gains a field again
 * at version 4 (KIP-518, Kafka 2.6), the `states_filter`. The answer of version 1 is the answer of version 2 and
 * is read with {@see ListGroupsResponseV1}.
 *
 * @see docs/protocol/2.8.md, sections "ListGroups API (key 16, v0 to v3)" and "Quotas and throttle time"
 */
final class ListGroupsRequestV1 extends ListGroupsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
