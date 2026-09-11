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
 * ListGroups request of version 0 (Kafka 0.9), an empty body just like version 1
 *
 * <pre>
 *   ListGroups Request (Version: 0) =>
 * </pre>
 *
 * `LIST_GROUPS_REQUEST_V1 = LIST_GROUPS_REQUEST_V0` in `Protocol.java` @ 0.11.0.3 - both versions are the bare
 * request header - so only the answer of version 1 is different ({@see ListGroupsResponseV0}).
 *
 * @see docs/protocol/2.8.md, section "ListGroups API (key 16, v0 to v3)"
 */
final class ListGroupsRequestV0 extends ListGroupsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
