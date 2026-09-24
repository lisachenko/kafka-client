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
 * A range of records a ShareFetch answer acquired for the member (key 78, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   AcquiredRecords => first_offset last_offset delivery_count
 *     first_offset   => INT64
 *     last_offset    => INT64    -- inclusive
 *     delivery_count => INT16
 * </pre>
 *
 * The records of a share-fetch answer are the log as it lies; which of them belong to THIS member is what the
 * `AcquiredRecords` say (`ShareFetchResponse.json` @ 4.1.0): the member holds a lock on every offset of the ranges
 * for `acquisition_lock_timeout_ms`, and a record of the answer outside them is not its to process. The delivery
 * count is 1 on the first delivery and grows with every redelivery - a record that is released, or whose lock
 * expired - until `share.delivery.count.limit` (5 by default) archives it.
 *
 * @see docs/protocol/4.3.md, section "ShareFetch API (key 78, v1)"
 */
final class ShareAcquiredRecords implements BinarySchemaInterface
{
    /**
     * First offset of the range
     */
    public int $firstOffset = 0;

    /**
     * Last offset of the range, inclusive
     */
    public int $lastOffset = 0;

    /**
     * How many times the records of the range have been delivered, this delivery included
     */
    public int $deliveryCount = 0;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'firstOffset'   => BinarySchema::TYPE_INT64,
            'lastOffset'    => BinarySchema::TYPE_INT64,
            'deliveryCount' => BinarySchema::TYPE_INT16,
        ];
    }
}
