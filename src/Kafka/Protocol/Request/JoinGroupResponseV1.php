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
 * JoinGroup response, version 1: the answer of Kafka 0.10.1, without the throttle time of version 2
 *
 * <pre>
 *   JoinGroup Response (Version: 0 and 1) => error_code generation_id group_protocol leader_id member_id [members]
 * </pre>
 *
 * `JOIN_GROUP_RESPONSE_V1 = JOIN_GROUP_RESPONSE_V0` in `Protocol.java` @ 0.11.0.3: this layout is the one of
 * version 0 as well ({@see JoinGroupResponseV0}), and the two classes exist separately only so that the class of an
 * answer always names the version of the request that asked for it. Both lower the version constant that
 * {@see JoinGroupResponse::getScheme()} follows, so that the leading `throttle_time_ms` of version 2 is not read out
 * of the `error_code` and the `generation_id` of these versions.
 *
 * @see docs/protocol/2.8.md, section "JoinGroup API (key 11, v0 to v5)"
 */
final class JoinGroupResponseV1 extends JoinGroupResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
