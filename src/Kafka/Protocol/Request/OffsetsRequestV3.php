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
 * Offsets (ListOffset) Request of version 3 (key 2)
 *
 * The frame of version 3 (Kafka 2.0, KIP-219) is the frame of version 2, byte for byte: what it states is that the
 * client waits out the throttle time of the answer itself. It is the last version that carries **no leader epoch**
 * at all - version 4 (Kafka 2.1, KIP-320) put a `current_leader_epoch` into every partition of the request and a
 * `leader_epoch` into every partition of the answer, see {@see OffsetsRequest}.
 *
 * @see docs/protocol/2.8.md, section "Offsets API (key 2, v0 to v4), a.k.a. ListOffset"
 */
final class OffsetsRequestV3 extends OffsetsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
