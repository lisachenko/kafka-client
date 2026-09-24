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
 * Offsets (ListOffset) response of version 10 (key 2)
 *
 * The answer of the `timeout_ms` of KIP-1075 (Kafka 4.0), and the frame of version 11 byte for byte:
 * `ListOffsetsResponse.json` @ 4.2.0 comments "Version 11 enables listing offsets by earliest pending upload offset
 * (KIP-1023)" and declares not a field of it, see {@see OffsetsResponse}. This class decodes the answer of a request
 * that asked with {@see OffsetsRequestV10}.
 *
 * @see docs/protocol/4.3.md, sections "Offsets API (key 2, v0 to v11), a.k.a. ListOffset", "The timeout of
 *      KIP-1075 (v10)" and "The earliest pending upload offset of KIP-1023 (v11)"
 */
final class OffsetsResponseV10 extends OffsetsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 10;
}
