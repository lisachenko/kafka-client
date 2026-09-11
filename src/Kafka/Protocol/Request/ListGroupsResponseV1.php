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
 * ListGroups response, version 1: the throttle time, the error code and the groups, the answer of version 2 too
 *
 * <pre>
 *   ListGroups Response (Version: 1 and 2) => throttle_time_ms error_code [groups]
 * </pre>
 *
 * @see docs/protocol/2.8.md, sections "ListGroups API (key 16, v0 to v4)" and "Quotas and throttle time"
 */
final class ListGroupsResponseV1 extends ListGroupsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
