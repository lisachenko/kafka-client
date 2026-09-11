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
 * WriteTxnMarkers request of version 0, the body of version 1 in the encoding before KIP-482
 *
 * Kafka 2.8 made the version 1 the first **flexible** one of this api ("Version 1 enables flexible versions" in
 * `WriteTxnMarkersRequest.json` @ 2.8.2) and changed no field, so this class only lowers the version constant: the version
 * 0 is the frame a broker below Kafka 2.8 speaks.
 *
 * @see docs/protocol/2.8.md, section "WriteTxnMarkers API (key 27, v0 and v1)"
 */
final class WriteTxnMarkersRequestV0 extends WriteTxnMarkersRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}
