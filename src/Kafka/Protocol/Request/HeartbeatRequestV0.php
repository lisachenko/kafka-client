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
 * Heartbeat request of version 0 (Kafka 0.9), the frame of version 1 with a lower version field
 *
 * <pre>
 *   Heartbeat Request (Version: 0) => group_id group_generation_id member_id
 * </pre>
 *
 * `HEARTBEAT_REQUEST_V1 = HEARTBEAT_REQUEST_V0` in `Protocol.java` @ 0.11.0.3, so this class sends the very same body
 * as {@see HeartbeatRequest}; only the answer differs, which is why the version has to be a class of its own
 * ({@see HeartbeatResponseV0}).
 *
 * @see docs/protocol/1.1.md, section "Heartbeat API (key 12, v0 and v1)"
 */
final class HeartbeatRequestV0 extends HeartbeatRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
