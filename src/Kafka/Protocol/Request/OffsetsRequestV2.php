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
 * Offsets API (key 2, v2), a.k.a. ListOffset
 *
 * <pre>
 *   ListOffsets Request (Version: 2) => replica_id isolation_level [topics]
 * </pre>
 *
 * The body of version 2 (Kafka 0.11, KIP-98) is the body of version 3, byte for byte: `ListOffsetsRequest.json`
 * @ 2.8.2 has no field of version 3 and says "Version 3 is the same as version 2". This is the highest version a
 * **Kafka 1.1.1** broker serves and the version the 1.x line of this client sent; a 2.8.2 broker still serves it,
 * and throttles it exactly as it throttles version 3, see {@see OffsetsRequest}.
 *
 * @see docs/protocol/2.8.md, section "Offsets API (key 2, v0 to v3), a.k.a. ListOffset"
 */
final class OffsetsRequestV2 extends OffsetsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;
}
