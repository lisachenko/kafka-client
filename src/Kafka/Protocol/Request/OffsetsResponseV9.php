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
 * Offsets (ListOffset) response of version 9 (key 2)
 *
 * The answer of the last tiered offset of KIP-1005 (Kafka 3.9), and the frame of version 10 byte for byte:
 * `ListOffsetsResponse.json` @ 4.0.0 comments "Version 10 enables async remote list offsets support (KIP-1075)" and
 * declares not a field of it, see {@see OffsetsResponse}. This class decodes the answer of a request that asked with
 * {@see OffsetsRequestV9}.
 *
 * @see docs/protocol/4.3.md, sections "Offsets API (key 2, v0 to v10), a.k.a. ListOffset" and "The timeout of
 *      KIP-1075 (v10)"
 */
final class OffsetsResponseV9 extends OffsetsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 9;
}
