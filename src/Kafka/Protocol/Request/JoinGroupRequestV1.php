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
 * JoinGroup request of version 1 (Kafka 0.10.1), the frame of version 2 with a lower version field
 *
 * <pre>
 *   JoinGroup Request (Version: 1) => group_id session_timeout rebalance_timeout member_id protocol_type
 *                                     [group_protocols]
 * </pre>
 *
 * `JOIN_GROUP_REQUEST_V2 = JOIN_GROUP_REQUEST_V1` in `Protocol.java` @ 0.11.0.3: the frame of this class differs from
 * the one of {@see JoinGroupRequest} in the version field of the header alone. It exists because the ANSWER differs -
 * a version 1 answer has no `throttle_time_ms` ({@see JoinGroupResponseV1}) - so a client that asks with this class
 * has to read the answer with the matching response class.
 *
 * @see docs/protocol/2.8.md, section "JoinGroup API (key 11, v0 to v6)"
 */
final class JoinGroupRequestV1 extends JoinGroupRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
