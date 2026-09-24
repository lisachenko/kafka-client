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

use Protocol\Kafka\Protocol\Data\WriteTxnMarkersResponseMarker;

/**
 * WriteTxnMarkers response object (key 27, v0 to v2)
 *
 * <pre>
 *   WriteTxnMarkers Response (Version: 0) => [transaction_markers]
 *     transaction_markers => producer_id [topics]
 *       producer_id => INT64
 *       topics      => topic [partitions]
 *         topic      => STRING
 *         partitions => partition error_code
 *           partition  => INT32
 *           error_code => INT16
 * </pre>
 *
 * **The only answer of Kafka 0.11 that does not start with a `throttle_time_ms`**, although the api was added by
 * the same release that KIP-124 landed in: `WRITE_TXN_MARKERS_RESPONSE_V0` in `Protocol.java` @ 0.11.0.3 begins
 * with the array. The reason is that the api is broker-to-broker - the quota machinery of KIP-124 only throttles
 * clients - and it is the same reason it has no top-level error code either.
 *
 * An empty request is answered with an empty array, which is what the api probe of this branch sends to check that
 * the key is served at all.
 *
 * The version 2 of Kafka 4.2 (*"Version 2 matches WriteTxnMarkersRequest version 2 (KIP-1228)"* in
 * `WriteTxnMarkersResponse.json` @ 4.2.0) changed no field of the answer; {@see WriteTxnMarkersResponseV1} keeps
 * the version 1.
 *
 * @see docs/protocol/4.3.md, section "WriteTxnMarkers API (key 27, v0 to v2)"
 */
class WriteTxnMarkersResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;

    /**
     * The version 1 of Kafka 2.8 is the first flexible one of this api (KIP-482)
     */
    public const int FLEXIBLE_VERSION = 1;

    /**
     * Result of every marker of the request, indexed by the producer id it belongs to
     *
     * @var array<int, WriteTxnMarkersResponseMarker>
     */
    public array $transactionMarkers = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'transactionMarkers' => ['producerId' => WriteTxnMarkersResponseMarker::class],
        ];
    }
}
