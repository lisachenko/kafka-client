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

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * The result of writing the markers of one producer, i.e. one entry of the `transaction_markers` array of an answer
 *
 * <pre>
 *   WriteTxnMarkersResponseMarker => producer_id [topics]
 *     producer_id => INT64
 *     topics      => topic [partitions]
 *       topic      => STRING
 *       partitions => partition error_code
 * </pre>
 *
 * `WRITE_TXN_MARKERS_ENTRY_RESPONSE_V0` in `Protocol.java` @ 0.11.0.3. The answer carries **no epoch and no
 * transaction result** - the producer id is enough to match an entry to the one of the request - and one entry per
 * requested partition, each with the error code of appending its control batch.
 *
 * @see docs/protocol/1.1.md, section "WriteTxnMarkers API (key 27, v0)"
 */
class WriteTxnMarkersResponseMarker implements BinarySchemaInterface
{
    /**
     * Producer id whose markers this entry reports
     */
    public int $producerId;

    /**
     * Result of every requested topic, indexed by the topic name
     *
     * @var array<string, WriteTxnMarkersResponseTopic>
     */
    public array $topics;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'producerId' => BinarySchema::TYPE_INT64,
            'topics'     => ['topic' => WriteTxnMarkersResponseTopic::class],
        ];
    }
}
