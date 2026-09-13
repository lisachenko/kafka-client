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
 * JoinGroup response, version 3: the answer of version 2, which version 4 repeats
 *
 * <pre>
 *   JoinGroup Response (Version: 2 to 4) => throttle_time_ms error_code generation_id group_protocol leader_id
 *                                           member_id [members]
 * </pre>
 *
 * KIP-394 (version 4) changed no field of this answer: what it added is the **79** `MemberIdRequired` that a
 * first join of that version is refused with, and that answer uses the very same layout - the generation -1, an
 * empty group protocol, an empty leader id, an empty member array, and the assigned member id in `member_id`.
 *
 * @see docs/protocol/2.8.md, section "The member id of a first join (v4, KIP-394)"
 */
final class JoinGroupResponseV3 extends JoinGroupResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
