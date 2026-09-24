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
 * LeaveGroup response of version 4 (Kafka 2.4, KIP-482): the answer of the batch that names no reason
 *
 * The bytes are the ones of version 5 - "Version 5 is the same as version 4" in `LeaveGroupResponse.json` @
 * 3.2.3, because KIP-800 changed the request alone - so this class and {@see LeaveGroupResponse} decode the same
 * frame, one api version apart, and only {@see LeaveGroupRequestV4} and {@see LeaveGroupRequest} differ.
 *
 * @see docs/protocol/4.3.md, section "The leave reason of KIP-800 (v5)"
 * @see docs/protocol/4.3.md, section "LeaveGroup API (key 13, v0 to v5)"
 */
final class LeaveGroupResponseV4 extends LeaveGroupResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
