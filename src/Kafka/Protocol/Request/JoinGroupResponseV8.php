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
 * JoinGroup answer of version 8 (Kafka 3.2, KIP-800): the answer of the request that first carries a `reason`
 *
 * The bytes are the ones of version 7 - "Version 8 is the same as version 7" in `JoinGroupResponse.json` @ 3.2.3
 * - because KIP-800 changed the request alone ({@see JoinGroupRequestV8}). What separates this version from
 * {@see JoinGroupResponse} is the `skip_assignment` that version 9 (KIP-814) adds behind the leader id: a member
 * that asks at this version is never told to keep the assignment of its group, so its leader computes one on
 * every join, which is what every client below Kafka 3.2 does.
 *
 * @see docs/protocol/3.9.md, section "The reason of KIP-800 and the skip_assignment of KIP-814 (v8 and v9)"
 * @see docs/protocol/3.9.md, section "JoinGroup API (key 11, v0 to v9)"
 */
final class JoinGroupResponseV8 extends JoinGroupResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 8;
}
