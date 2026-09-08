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
 * One partition of an Offsets (ListOffset) request v0
 *
 * <pre>
 *   OffsetsRequestPartition => Partition Time MaxNumberOfOffsets
 *     Partition          => int32
 *     Time               => int64
 *     MaxNumberOfOffsets => int32
 * </pre>
 *
 * `MaxNumberOfOffsets` only exists in v0: the timestamp-based v1 of the API (Kafka 0.10.1) returns exactly one offset
 * per partition and dropped the field.
 *
 * @see docs/protocol/0.8.2.md, section "Offsets API (key 2, v0), a.k.a. ListOffset"
 */
class OffsetsRequestPartition implements BinarySchemaInterface
{
    /**
     * Id of the partition to list the offsets of
     */
    public int $partition;

    /**
     * Target time in milliseconds, or one of the special values `OffsetsRequest::LATEST` / `OffsetsRequest::EARLIEST`.
     *
     * For an ordinary timestamp the broker answers with the start offsets of the log segments that were last modified
     * before that time, which is a much coarser answer than the offset-for-timestamp lookup of the later versions.
     *
     * @see \Protocol\Kafka\Protocol\Request\OffsetsRequest::LATEST
     * @see \Protocol\Kafka\Protocol\Request\OffsetsRequest::EARLIEST
     */
    public int $timestamp;

    /**
     * Maximum number of offsets that the broker may return for this partition
     */
    public int $maxNumberOfOffsets;

    public function __construct(int $partition, int $timestamp, int $maxNumberOfOffsets = 1)
    {
        $this->partition          = $partition;
        $this->timestamp          = $timestamp;
        $this->maxNumberOfOffsets = $maxNumberOfOffsets;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partition'          => BinarySchema::TYPE_INT32,
            'timestamp'          => BinarySchema::TYPE_INT64,
            'maxNumberOfOffsets' => BinarySchema::TYPE_INT32,
        ];
    }
}
