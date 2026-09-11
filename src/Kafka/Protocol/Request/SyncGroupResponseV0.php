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
 * SyncGroup response, version 0: the error code and the assignment, without a throttle time (key 14)
 *
 * <pre>
 *   SyncGroup Response (Version: 0) => error_code member_assignment
 * </pre>
 *
 * Version 1 (KIP-124, Kafka 0.11) opened the answer with a `throttle_time_ms`, which this layout has not, so this
 * class only lowers the version constant that {@see SyncGroupResponse::getScheme()} follows. Reading a version 0
 * answer with the version 1 class would take the error code and the first two bytes of the assignment size for a
 * throttle time.
 *
 * @see docs/protocol/2.8.md, section "SyncGroup API (key 14, v0 to v2)"
 */
final class SyncGroupResponseV0 extends SyncGroupResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
