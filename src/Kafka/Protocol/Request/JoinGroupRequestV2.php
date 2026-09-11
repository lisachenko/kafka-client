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
 * JoinGroup request of version 2 (Kafka 0.11), the frame of version 3 with a lower version field
 *
 * <pre>
 *   JoinGroup Request (Version: 1, 2 and 3) => group_id session_timeout rebalance_timeout member_id protocol_type
 *                                              [group_protocols]
 * </pre>
 *
 * Version 3 (KIP-219, Kafka 2.0) added no field: `JoinGroupRequest.json` @ 2.8.2 introduces nothing between the
 * `rebalance_timeout` of version 1 and the `group_instance_id` of version 5, so this class sends the very same
 * bytes as {@see JoinGroupRequest}. Its answer is unchanged too and is read with {@see JoinGroupResponseV2}.
 *
 * @see docs/protocol/2.8.md, sections "JoinGroup API (key 11, v0 to v6)" and "Quotas and throttle time"
 */
final class JoinGroupRequestV2 extends JoinGroupRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
