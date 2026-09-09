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
use Protocol\Kafka\Protocol\Request\OffsetsRequest;

/**
 * One partition of an Offsets (ListOffset) request, version 1
 *
 * <pre>
 *   OffsetsRequestPartition => partition timestamp
 *     partition => INT32
 *     timestamp => INT64
 * </pre>
 *
 * `MaxNumberOfOffsets` only exists in version 0 (`LIST_OFFSET_REQUEST_PARTITION_V0` in `Protocol.java` @ 0.10.2.2):
 * the timestamp-based version 1 of the api, which Kafka 0.10.1 added with KIP-79, answers with exactly one offset
 * per partition and dropped the field. The odd version out therefore lives in {@see OffsetsRequestPartitionV0} and
 * the scheme is selected by {@see OffsetsRequestPartition::VERSION}.
 *
 * @see docs/protocol/0.11.0.md, section "Offsets API (key 2, v0, v1 and v2), a.k.a. ListOffset"
 */
class OffsetsRequestPartition implements BinarySchemaInterface
{
    /**
     * Version of the Offsets API that this DTO is packed for
     */
    public const int VERSION = 1;

    /**
     * Id of the partition to list the offsets of
     */
    public int $partition;

    /**
     * Target time in milliseconds, or one of the special values {@see OffsetsRequest::LATEST} /
     * {@see OffsetsRequest::EARLIEST}.
     *
     * In version 1 an ordinary timestamp asks for the offset of the first message whose own timestamp is `>= t`,
     * which the time index of the log resolves. In version 0 the broker knew nothing about the timestamps of the
     * messages and answered with the start offsets of the log segments that were last modified before that time.
     */
    public int $timestamp;

    /**
     * Maximum number of offsets that the broker may return for this partition
     *
     * @deprecated Since version 1 of the api, which always answers with exactly one offset per partition
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
        $scheme = [
            'partition' => BinarySchema::TYPE_INT32,
            'timestamp' => BinarySchema::TYPE_INT64,
        ];
        if (static::VERSION === 0) {
            $scheme['maxNumberOfOffsets'] = BinarySchema::TYPE_INT32;
        }

        return $scheme;
    }
}
