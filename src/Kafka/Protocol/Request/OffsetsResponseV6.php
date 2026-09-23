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
 * Offsets (ListOffset) response of version 6 (key 2)
 *
 * The first **flexible** answer of the api (Kafka 2.8, KIP-482), and the frame of version 7 byte for byte:
 * `ListOffsetsResponse.json` @ 3.0.2 comments "Version 7 is the same as version 6 (KIP-734)" and declares not a
 * field of it, see {@see OffsetsResponse}. This class decodes the answers of a request that asked with
 * {@see OffsetsRequestV6}, including the **35** `UNSUPPORTED_VERSION` a 3.x broker answers such a request per
 * partition when it asks for {@see OffsetsRequest::MAX_TIMESTAMP}.
 *
 * @see docs/protocol/3.9.md, section "Offsets API (key 2, v0 to v9), a.k.a. ListOffset"
 */
final class OffsetsResponseV6 extends OffsetsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 6;
}
