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
 * Offsets (ListOffset) response of version 8 (key 2)
 *
 * The answer of the local log start offset of KIP-405 (Kafka 3.5), and the frame of version 9 byte for byte:
 * `ListOffsetsResponse.json` @ 3.9.2 comments "Version 9 enables listing offsets by last tiered offset" and
 * declares not a field of it, see {@see OffsetsResponse}. This class decodes the answers of a request that asked
 * with {@see OffsetsRequestV8}, including the **35** `UNSUPPORTED_VERSION` a 3.9.2 broker answers such a request
 * per partition when it asks for {@see OffsetsRequest::LATEST_TIERED_TIMESTAMP}.
 *
 * @see docs/protocol/4.3.md, section "Offsets API (key 2, v0 to v10), a.k.a. ListOffset"
 */
final class OffsetsResponseV8 extends OffsetsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 8;
}
