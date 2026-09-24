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
 * WriteTxnMarkers answer of version 1 (Kafka 2.8, KIP-482)
 *
 * Kafka 4.2 made the version 2 of the answer only to match the version 2 of the request (*"Version 2 matches
 * WriteTxnMarkersRequest version 2 (KIP-1228)"* in `WriteTxnMarkersResponse.json` @ 4.2.0) and changed no field,
 * so this class only lowers the version constant.
 *
 * @see docs/protocol/4.3.md, section "WriteTxnMarkers API (key 27, v0 to v2)"
 */
final class WriteTxnMarkersResponseV1 extends WriteTxnMarkersResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
