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
 * JoinGroup response, version 0: the answer of Kafka 0.9, byte for byte the one of version 1
 *
 * <pre>
 *   JoinGroup Response (Version: 0 and 1) => error_code generation_id group_protocol leader_id member_id [members]
 * </pre>
 *
 * The `rebalance_timeout` that version 1 of the request added changed nothing here, and the `throttle_time_ms` of
 * version 2 is not in this layout, so this class only lowers the version constant that
 * {@see JoinGroupResponse::getScheme()} follows. It decodes the same bytes as {@see JoinGroupResponseV1}.
 *
 * @see docs/protocol/2.8.md, section "JoinGroup API (key 11, v0 to v3)"
 */
final class JoinGroupResponseV0 extends JoinGroupResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
