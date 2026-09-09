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
 * ListGroups response, version 0: the error code and the group array, without a throttle time (key 16)
 *
 * <pre>
 *   ListGroups Response (Version: 0) => error_code [groups]
 * </pre>
 *
 * Version 1 (KIP-124, Kafka 0.11) put a `throttle_time_ms` in front of the error code; this class only lowers the
 * version constant that {@see ListGroupsResponse::getScheme()} follows.
 *
 * @see docs/protocol/0.11.0.md, section "ListGroups API (key 16, v0 and v1)"
 */
final class ListGroupsResponseV0 extends ListGroupsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
