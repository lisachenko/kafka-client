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
 * Heartbeat response, version 3: the throttle time and the error code, plainly encoded
 *
 * Version 4 (Kafka 2.4, KIP-482) is the same answer in the flexible encoding, see {@see HeartbeatResponse}.
 *
 * @see docs/protocol/2.8.md, section "The flexible versions of the group apis (Kafka 2.4)"
 * @see docs/protocol/2.8.md, section "Heartbeat API (key 12, v0 to v4)"
 */
final class HeartbeatResponseV3 extends HeartbeatResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
