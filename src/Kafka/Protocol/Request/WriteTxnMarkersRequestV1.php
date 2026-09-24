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
 * WriteTxnMarkers request of version 1 (Kafka 2.8, KIP-482): the flexible frame, markers without a transaction version
 *
 * The frame of {@see WriteTxnMarkersRequest} with one byte less in every marker: version 2 (Kafka 4.2, KIP-1228)
 * appended the `transaction_version`, and this is the request below it, whose markers are encoded as
 * {@see \Protocol\Kafka\Protocol\Data\WriteTxnMarkersRequestMarkerV1} entries.
 *
 * @see docs/protocol/4.3.md, section "WriteTxnMarkers API (key 27, v0 to v2)"
 */
final class WriteTxnMarkersRequestV1 extends WriteTxnMarkersRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
