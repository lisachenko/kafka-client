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
 * JoinGroup response, version 2: the throttle time of KIP-124 and the members, the answer of version 3 as well
 *
 * <pre>
 *   JoinGroup Response (Version: 2 and 3) => throttle_time_ms error_code generation_id group_protocol leader_id
 *                                            member_id [members]
 * </pre>
 *
 * @see docs/protocol/2.8.md, sections "JoinGroup API (key 11, v0 to v5)" and "Quotas and throttle time"
 */
final class JoinGroupResponseV2 extends JoinGroupResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
