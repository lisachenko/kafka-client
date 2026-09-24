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
 * ConsumerGroupHeartbeat response of version 0 (Kafka 3.5, KIP-848)
 *
 * The answer of version 1 (Kafka 4.0) is byte for byte this one, which {@see ConsumerGroupHeartbeatResponse} decodes;
 * the class exists so that the frames of the two versions are replayed through a class of their own version.
 *
 * @see docs/protocol/4.3.md, section "ConsumerGroupHeartbeat API (key 68, v0 and v1)"
 */
final class ConsumerGroupHeartbeatResponseV0 extends ConsumerGroupHeartbeatResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
