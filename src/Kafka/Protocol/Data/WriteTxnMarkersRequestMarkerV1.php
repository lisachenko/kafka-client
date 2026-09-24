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

namespace Protocol\Kafka\Protocol\Data;

/**
 * One marker of a WriteTxnMarkers request of version 0 or 1: the five fields of Kafka 0.11, no transaction version
 *
 * Version 2 of the api (Kafka 4.2, KIP-1228) appended the `transaction_version` to every marker
 * ({@see WriteTxnMarkersRequestMarker}); this is the entry below it, which a request of the version 0 or 1 carries.
 * Its {@see WriteTxnMarkersRequestMarker::$transactionVersion} is not written.
 *
 * @see docs/protocol/4.3.md, section "WriteTxnMarkers API (key 27, v0 to v2)"
 */
final class WriteTxnMarkersRequestMarkerV1 extends WriteTxnMarkersRequestMarker
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}
