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
/**
 * @author Alexander.Lisachenko
 * @date 14.07.2016
 */

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * Produce request Topic-Partition DTO
 *
 * <pre>
 *   Partition RecordSetSize RecordSet
 *     Partition     => int32
 *     RecordSetSize => int32
 * </pre>
 *
 * A record set is a raw byte region, not a length-prefixed array of structures, therefore it travels as a BYTEARRAY
 * whose int32 length prefix is exactly the `MessageSetSize`/`RecordSetSize` field of the spec. Which message format
 * those bytes are in is decided by the producer, not by the field: a version 3 request carries a
 * {@see \Protocol\Kafka\Common\Record\RecordBatch} of the message format v2 here, a lower one a
 * {@see \Protocol\Kafka\Common\Record\MessageSet} of the format v0 or v1, and
 * {@see \Protocol\Kafka\Common\Record\MemoryRecords} wraps either of them. The property keeps the name
 * `messageSet` of the lower protocol lines, whose vectors are replayed against this class.
 *
 * @see docs/protocol/2.8.md, sections "Produce API (key 0, v0 to v5)", "MessageSet and Message" and
 *      "RecordBatch (message format v2)"
 */
class ProduceRequestPartition implements BinarySchemaInterface
{
    /**
     * The partition this request entry corresponds to.
     */
    public int $partition = 0;

    /**
     * Encoded record set for this topic-partition.
     */
    public string $messageSet = '';

    /**
     * @param int                $partition  Number of the partition to produce to
     * @param string|\Stringable $messageSet Encoded record set, typically a Common\Record\MemoryRecords instance
     */
    public function __construct(int $partition = 0, string|\Stringable $messageSet = '')
    {
        $this->partition  = $partition;
        $this->messageSet = (string) $messageSet;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partition'  => BinarySchema::TYPE_INT32,
            'messageSet' => BinarySchema::TYPE_BYTEARRAY,
        ];
    }
}
