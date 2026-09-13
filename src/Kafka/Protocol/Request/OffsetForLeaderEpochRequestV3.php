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
 * OffsetForLeaderEpoch request of version 3 (key 23)
 *
 * The last **plain** version of the api: the same fields as version 4 - the `replica_id` of KIP-392 included -
 * written with `INT16`-prefixed strings, `INT32`-counted arrays and without a tagged-field section anywhere.
 * Version 4 (Kafka 2.8) is the flexible version of KIP-482, see {@see OffsetForLeaderEpochRequest}.
 *
 * @see docs/protocol/2.8.md, section "OffsetForLeaderEpoch API (key 23, v0 to v4)"
 */
final class OffsetForLeaderEpochRequestV3 extends OffsetForLeaderEpochRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}
