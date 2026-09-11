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
 * OffsetFetch request of version 2 (Kafka 0.10.2), the frame of version 3 with a lower version field
 *
 * <pre>
 *   OffsetFetch Request (Version: 2) => group_id [topics]
 *     topics => NULLABLE
 * </pre>
 *
 * `OFFSET_FETCH_REQUEST_V3 = OFFSET_FETCH_REQUEST_V2` in `Protocol.java` @ 0.11.0.3: the nullable topic array of
 * version 2 is what version 3 sends as well, and only the answer differs ({@see OffsetFetchResponseV2}).
 *
 * @see docs/protocol/2.8.md, section "OffsetFetch API (key 9, v0 to v5)"
 */
final class OffsetFetchRequestV2 extends OffsetFetchRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
