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
 * LeaveGroup response, version 1: the throttle time and the error code, the answer of version 2 as well
 *
 * <pre>
 *   LeaveGroup Response (Version: 1 and 2) => throttle_time_ms error_code
 * </pre>
 *
 * @see docs/protocol/2.8.md, sections "LeaveGroup API (key 13, v0 to v4)" and "Quotas and throttle time"
 */
final class LeaveGroupResponseV1 extends LeaveGroupResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
