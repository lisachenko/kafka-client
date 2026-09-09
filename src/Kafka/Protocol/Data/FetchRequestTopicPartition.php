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
 * One partition of a Fetch request v0
 *
 * <pre>
 *   FetchRequestTopicPartition => Partition FetchOffset MaxBytes
 *     Partition   => int32
 *     FetchOffset => int64
 *     MaxBytes    => int32
 * </pre>
 *
 * `LogStartOffset` only exists since FetchRequest v5 (Kafka 0.11) and is therefore absent here.
 *
 * @see docs/protocol/0.10.2.md, section "Fetch API (key 1, v0 and v1)"
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
     * A 0.9.0.1 broker answers with an empty message set when the first message at `fetchOffset` is bigger than this
     * limit; the guaranteed progress of the later protocol versions does not exist yet.
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
