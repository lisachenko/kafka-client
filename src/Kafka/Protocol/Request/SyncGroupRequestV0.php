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
 * SyncGroup request of version 0 (Kafka 0.9), the frame of version 1 with a lower version field
 *
 * <pre>
 *   SyncGroup Request (Version: 0) => group_id generation_id member_id [group_assignment]
 * </pre>
 *
 * `SYNC_GROUP_REQUEST_V1 = SYNC_GROUP_REQUEST_V0` in `Protocol.java` @ 0.11.0.3: only the answer of version 1 is
 * different ({@see SyncGroupResponseV0}).
 *
 * @see docs/protocol/2.8.md, section "SyncGroup API (key 14, v0 and v1)"
 */
final class SyncGroupRequestV0 extends SyncGroupRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
