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
 * LeaveGroup response, version 3: the per-member answers of the batch, plainly encoded
 *
 * Version 4 (Kafka 2.4, KIP-482) is the same answer in the flexible encoding, see {@see LeaveGroupResponse}.
 *
 * @see docs/protocol/2.8.md, section "The flexible versions of the group apis (Kafka 2.4)"
 * @see docs/protocol/2.8.md, section "LeaveGroup API (key 13, v0 to v4)"
 */
final class LeaveGroupResponseV3 extends LeaveGroupResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
