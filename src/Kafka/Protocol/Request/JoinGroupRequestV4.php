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
 * JoinGroup request of version 4 (Kafka 2.2, KIP-394), the frame of version 3 with the member id rule of KIP-394
 *
 * <pre>
 *   JoinGroup Request (Version: 1 to 4) => group_id session_timeout rebalance_timeout member_id protocol_type
 *                                          [group_protocols]
 * </pre>
 *
 * The first field the api gains after version 1 is the `group_instance_id` of version 5 (KIP-345, Kafka 2.3),
 * which {@see JoinGroupRequest} sends; this class is the frame without it, and it keeps the behaviour of KIP-394:
 * a request of this version with an empty member id is answered with the error code 79 (`MemberIdRequired`) and
 * the id the coordinator assigned.
 *
 * @see docs/protocol/2.8.md, section "The member id of a first join (v4, KIP-394)"
 */
final class JoinGroupRequestV4 extends JoinGroupRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
