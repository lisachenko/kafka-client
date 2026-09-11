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
 * Offsets (ListOffset) Request of version 4 (key 2)
 *
 * The frame of version 4 (Kafka 2.1, KIP-320) is the frame of version 5, byte for byte:
 * `ListOffsetsRequest.json` @ 2.8.2 says "Version 5 is the same as version 4", and what version 5 (Kafka 2.2,
 * KIP-207) states lives in the **answer** - that the client understands the error code **78**
 * `OFFSET_NOT_AVAILABLE` of a leader whose high watermark has not caught up with the epoch it was just elected in.
 * A request of this version is answered with **5** `LEADER_NOT_AVAILABLE` in that state instead, see
 * {@see OffsetsRequest}.
 *
 * @see docs/protocol/2.8.md, section "Offsets API (key 2, v0 to v5), a.k.a. ListOffset"
 */
final class OffsetsRequestV4 extends OffsetsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
