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
 * LeaveGroup request of version 4 (Kafka 2.4, KIP-482): the flexible batch without the `reason` of KIP-800
 *
 * Version 5 (Kafka 3.2, KIP-800) appended a nullable `reason` to every entry of the batch, "the reason why the
 * member left the group" - `LeaveGroupRequest.json` @ 3.2.3 - which {@see LeaveGroupRequest} sends. A batch built
 * by this class carries the entries of {@see \Protocol\Kafka\Protocol\Data\LeaveGroupRequestMemberV3}, so a
 * reason a caller names never reaches the wire.
 *
 * @see docs/protocol/4.3.md, section "The leave reason of KIP-800 (v5)"
 * @see docs/protocol/4.3.md, section "LeaveGroup API (key 13, v0 to v5)"
 */
final class LeaveGroupRequestV4 extends LeaveGroupRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
