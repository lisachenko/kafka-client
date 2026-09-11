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
 * JoinGroup request of version 3 (Kafka 2.0, KIP-219), the frame of version 4 with a lower version field
 *
 * <pre>
 *   JoinGroup Request (Version: 1 to 4) => group_id session_timeout rebalance_timeout member_id protocol_type
 *                                          [group_protocols]
 * </pre>
 *
 * Version 4 (Kafka 2.2, KIP-394) added no field either - the next one is the `group_instance_id` of version 5 -
 * and changed the **behaviour** of an empty member id instead: a version 4 request that carries one is answered
 * with the error code 79 (`MemberIdRequired`) and the member id the coordinator assigned to the client, which
 * then has to join again with that id. A version 3 request with an empty member id is still added to the group
 * right away, which is what this class does.
 *
 * @see docs/protocol/2.8.md, section "The member id of a first join (v4, KIP-394)"
 */
final class JoinGroupRequestV3 extends JoinGroupRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
