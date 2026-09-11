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
 * One partition of an OffsetForLeaderEpoch answer, version 0 (key 23)
 *
 * <pre>
 *   OffsetForLeaderEpochResponsePartition => error_code partition_id end_offset
 *     error_code   => INT16
 *     partition_id => INT32
 *     end_offset   => INT64
 * </pre>
 *
 * Note the order: unlike every other partition answer of the protocol this one puts the **error code first** and
 * the partition id behind it (`OFFSET_FOR_LEADER_EPOCH_RESPONSE_PARTITION_V0` in `Protocol.java` @ 0.11.0.3).
 *
 * `end_offset` is the "log end offset of the requested epoch", i.e. the first offset that a LATER epoch owns:
 * `Log.endOffsetForEpoch` @ 0.11.0.3 answers the start offset of the first epoch above the requested one, and
 * {@see self::UNDEFINED_EPOCH_OFFSET} (`-1`) when the leader has no epoch cache entry that could answer - a
 * partition whose log was written by a broker below 0.11, or an epoch the leader has never heard of. That is the
 * offset a follower truncates to in KIP-101, instead of trusting the high watermark it had.
 *
 * @see docs/protocol/2.8.md, section "OffsetForLeaderEpoch API (key 23, v0)"
 */
class OffsetForLeaderEpochResponsePartition implements BinarySchemaInterface
{
    /**
     * Answer of a leader that cannot resolve the epoch, `EpochEndOffset.UNDEFINED_EPOCH_OFFSET` @ 0.11.0.3
     */
    public const int UNDEFINED_EPOCH_OFFSET = -1;

    /**
     * Error code of this partition
     */
    public int $errorCode;

    /**
     * Id of the partition
     */
    public int $partition;

    /**
     * First offset that an epoch above the requested one owns, {@see self::UNDEFINED_EPOCH_OFFSET} when unknown
     */
    public int $endOffset;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'errorCode' => BinarySchema::TYPE_INT16,
            'partition' => BinarySchema::TYPE_INT32,
            'endOffset' => BinarySchema::TYPE_INT64,
        ];
    }
}
