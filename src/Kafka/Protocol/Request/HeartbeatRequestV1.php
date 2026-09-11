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
 * Heartbeat request of version 1 (Kafka 0.11), the frame of version 2 with a lower version field
 *
 * <pre>
 *   Heartbeat Request (Version: 0, 1 and 2) => group_id group_generation_id member_id
 * </pre>
 *
 * Version 2 (KIP-219, Kafka 2.0) added nothing: the next field of the api is the `group_instance_id` of version 3
 * (KIP-345, Kafka 2.3). The answer did not change either and is read with {@see HeartbeatResponseV1}.
 *
 * @see docs/protocol/2.8.md, sections "Heartbeat API (key 12, v0 to v2)" and "Quotas and throttle time"
 */
final class HeartbeatRequestV1 extends HeartbeatRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
