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
 * Offsets (ListOffset) response of version 5 (key 2)
 *
 * The last **plain** version of the api: the same fields as version 6, written with `INT16`-prefixed strings,
 * `INT32`-counted arrays and without a tagged-field section anywhere - version 6 (Kafka 2.8) is the flexible
 * version of KIP-482, see {@see OffsetsResponse}. What version 5 itself states is the error code **78**
 * `OFFSET_NOT_AVAILABLE` of KIP-207.
 *
 * @see docs/protocol/2.8.md, section "Offsets API (key 2, v0 to v6), a.k.a. ListOffset"
 */
final class OffsetsResponseV5 extends OffsetsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 5;
}
