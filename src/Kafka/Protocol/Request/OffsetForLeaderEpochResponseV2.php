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
 * OffsetForLeaderEpoch response of version 2 (key 23)
 *
 * The answer of version 2 (Kafka 2.1, KIP-320) is the answer of version 3, field for field:
 * `OffsetForLeaderEpochResponse.json` @ 2.8.2 says "Version 3 is the same as version 2", because what version 3
 * (Kafka 2.3, KIP-392) added lives in the request. The two classes decode the same bytes and differ only in the
 * version of the request they belong to, see {@see OffsetForLeaderEpochResponse}.
 *
 * @see docs/protocol/2.8.md, section "OffsetForLeaderEpoch API (key 23, v0 to v4)"
 */
final class OffsetForLeaderEpochResponseV2 extends OffsetForLeaderEpochResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
