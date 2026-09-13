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
 * SyncGroup response, version 3: the assignment of this member, plainly encoded
 *
 * Version 4 (Kafka 2.4, KIP-482) is the same answer in the flexible encoding, see {@see SyncGroupResponse}.
 *
 * @see docs/protocol/2.8.md, section "The flexible versions of the group apis (Kafka 2.4)"
 * @see docs/protocol/2.8.md, section "SyncGroup API (key 14, v0 to v5)"
 */
final class SyncGroupResponseV3 extends SyncGroupResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
