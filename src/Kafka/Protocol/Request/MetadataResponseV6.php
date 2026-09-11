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
 * Metadata Response of version 6 (key 3)
 *
 * The frame of version 6 (Kafka 2.0, KIP-219) is the frame of version 5, byte for byte; what it states is that the
 * client waits out the throttle time of the answer itself. It is the last version whose partition entries carry no
 * `leader_epoch` - version 7 (Kafka 2.1, KIP-320) inserted that field behind the leader id, see
 * {@see MetadataResponse}.
 *
 * @see docs/protocol/2.8.md, section "Metadata API (key 3, v0 to v9)"
 */
final class MetadataResponseV6 extends MetadataResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 6;
}
