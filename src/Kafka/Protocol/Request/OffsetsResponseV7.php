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
 * Offsets (ListOffset) response of version 7 (key 2)
 *
 * The answer of the max timestamp of KIP-734 (Kafka 3.0), and the frame of version 8 byte for byte:
 * `ListOffsetsResponse.json` @ 3.5.2 comments "Version 8 enables listing offsets by local log start offset" and
 * declares not a field of it, see {@see OffsetsResponse}. This class decodes the answers of a request that asked
 * with {@see OffsetsRequestV7}, including the **35** `UNSUPPORTED_VERSION` a 3.9.2 broker answers such a request
 * per partition when it asks for {@see OffsetsRequest::EARLIEST_LOCAL_TIMESTAMP}.
 *
 * @see docs/protocol/4.3.md, section "Offsets API (key 2, v0 to v10), a.k.a. ListOffset"
 */
final class OffsetsResponseV7 extends OffsetsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 7;
}
