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
 * SyncGroup response, version 2: the throttle time, the error code and the assignment of this member
 *
 * <pre>
 *   SyncGroup Response (Version: 1 to 3) => throttle_time_ms error_code member_assignment
 * </pre>
 *
 * Version 3 (KIP-345, Kafka 2.3) changed the **request** alone - the `group_instance_id` of a static member - so
 * this answer and {@see SyncGroupResponse} decode the very same bytes.
 *
 * @see docs/protocol/2.8.md, section "Static membership (KIP-345)"
 */
final class SyncGroupResponseV2 extends SyncGroupResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
