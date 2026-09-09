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
 * One partition of a Fetch request
 *
 * <pre>
 *   FetchRequestTopicPartition => Partition FetchOffset MaxBytes
 *     Partition   => int32
 *     FetchOffset => int64
 *     MaxBytes    => int32
 * </pre>
 *
 * `LogStartOffset` only exists since FetchRequest v5 (Kafka 0.11) and is therefore absent here. The partition entry
 * itself has not changed in any version a 0.10.2.2 broker serves.
 *
 * @see docs/protocol/0.11.0.md, section "Fetch API (key 1, v0 to v3)"
 */
class FetchRequestTopicPartition implements BinarySchemaInterface
{
    /**
     * Id of the partition to fetch from
     */
    public int $partition;

    /**
     * Offset of the first message to fetch
     */
    public int $fetchOffset;

    /**
     * Maximum number of bytes of the message set that the broker may put into the response for this partition.
     *
     * This is `max.partition.fetch.bytes`, and it keeps that per-partition meaning next to the request-level
     * `MaxBytes` that version 3 of the api added ({@see \Protocol\Kafka\Protocol\Request\FetchRequest::$maxBytes}).
     * Up to version 2 the broker cuts the message set of a partition off at this limit without caring about message
     * boundaries, so a message that is bigger comes back as an incomplete set and the partition makes no progress;
     * from version 3 on the first non-empty partition of an answer ignores the limit and returns at least one
     * complete message.
     */
    public int $maxBytes;

    public function __construct(int $partition, int $fetchOffset, int $maxBytes)
    {
        $this->partition   = $partition;
        $this->fetchOffset = $fetchOffset;
        $this->maxBytes    = $maxBytes;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partition'   => BinarySchema::TYPE_INT32,
            'fetchOffset' => BinarySchema::TYPE_INT64,
            'maxBytes'    => BinarySchema::TYPE_INT32,
        ];
    }
}
