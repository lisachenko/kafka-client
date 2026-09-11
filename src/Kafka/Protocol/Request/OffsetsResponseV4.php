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
 * Offsets (ListOffset) Response of version 4 (key 2)
 *
 * The frame of version 4 (Kafka 2.1, KIP-320) is the frame of version 5, field for field:
 * `ListOffsetsResponse.json` @ 2.8.2 declares no field of version 5 and only notes "Version 5 adds a new error
 * code, OFFSET_NOT_AVAILABLE". The two classes therefore decode the very same bytes and differ only in the
 * version of the request they belong to - and in the **set of error codes** a partition of them may carry, see
 * {@see OffsetsResponse}.
 *
 * @see docs/protocol/2.8.md, section "Offsets API (key 2, v0 to v6), a.k.a. ListOffset"
 */
final class OffsetsResponseV4 extends OffsetsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}
