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
 * OffsetFetch answer of version 8 (Kafka 3.0): the batched answer before KIP-848 gave its codes a member
 *
 * Version 9 (Kafka 3.7) changed no field of this half at all - "the response is the same as version 8 but can
 * return STALE_MEMBER_EPOCH and UNKNOWN_MEMBER_ID errors when the new consumer group protocol is used"
 * (`OffsetFetchResponse.json` @ 3.7.2) - so the bytes of this class and of {@see OffsetFetchResponse} differ in
 * nothing but the version of the request they answer, and what the higher number buys is the **113** and the
 * **25** a group entry may carry. This class is the answer of a broker below Kafka 3.7.
 *
 * @see docs/protocol/4.3.md, section "OffsetFetch API (key 9, v0 to v10)"
 * @see docs/protocol/4.3.md, section "The member id and epoch of KIP-848 (v9)"
 */
final class OffsetFetchResponseV8 extends OffsetFetchResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 8;
}
