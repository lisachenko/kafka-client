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
 * LeaveGroup request of version 0 (Kafka 0.9), the frame of version 1 with a lower version field
 *
 * <pre>
 *   LeaveGroup Request (Version: 0) => group_id member_id
 * </pre>
 *
 * `LEAVE_GROUP_REQUEST_V1 = LEAVE_GROUP_REQUEST_V0` in `Protocol.java` @ 0.11.0.3: only the answer of version 1 is
 * different ({@see LeaveGroupResponseV0}).
 *
 * @see docs/protocol/2.8.md, section "LeaveGroup API (key 13, v0 to v3)"
 */
final class LeaveGroupRequestV0 extends LeaveGroupRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
