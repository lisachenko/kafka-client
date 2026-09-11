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
 * One partition of an OffsetForLeaderEpoch answer, version 1 (key 23)
 *
 * <pre>
 *   OffsetForLeaderEpochResponsePartition => error_code partition_id leader_epoch end_offset
 *     error_code   => INT16
 *     partition_id => INT32
 *     leader_epoch => INT32     -- since version 1
 *     end_offset   => INT64
 * </pre>
 *
 * Note the order: unlike every other partition answer of the protocol this one puts the **error code first** and
 * the partition id behind it (`OFFSET_FOR_LEADER_EPOCH_RESPONSE_PARTITION_V0` in `Protocol.java` @ 0.11.0.3).
 *
 * **Version 1 (Kafka 2.0, KIP-279) inserted `leader_epoch` between the partition id and the end offset**, see
 * {@see self::$leaderEpoch}: the epoch that the answered `end_offset` belongs to. Without it a follower that is
 * told "epoch 5 ends at offset 100" can not tell whether 100 is the end of epoch 5 itself or of an epoch the
 * leader has no cache entry for, which is the divergence KIP-279 closed; {@see OffsetForLeaderEpochResponsePartitionV0}
 * is the entry of version 0, which carries no epoch at all.
 *
 * `end_offset` is the "log end offset of the requested epoch", i.e. the first offset that a LATER epoch owns:
 * `Log.endOffsetForEpoch` @ 0.11.0.3 answers the start offset of the first epoch above the requested one, and
 * {@see self::UNDEFINED_EPOCH_OFFSET} (`-1`) when the leader has no epoch cache entry that could answer - a
 * partition whose log was written by a broker below 0.11, or an epoch the leader has never heard of. That is the
 * offset a follower truncates to in KIP-101, instead of trusting the high watermark it had.
 *
 * @see docs/protocol/2.8.md, section "OffsetForLeaderEpoch API (key 23, v0 to v3)"
 */
class OffsetForLeaderEpochResponsePartition implements BinarySchemaInterface
{
    /**
     * Version of the OffsetForLeaderEpoch API that this DTO is unpacked from
     */
    public const int VERSION = 1;

    /**
     * Answer of a leader that cannot resolve the epoch, `EpochEndOffset.UNDEFINED_EPOCH_OFFSET` @ 0.11.0.3
     */
    public const int UNDEFINED_EPOCH_OFFSET = -1;

    /**
     * Epoch of an answer that resolves no epoch at all, `OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH` @ 2.8.2
     *
     * The same `-1` as `RecordBatch.NO_PARTITION_LEADER_EPOCH`, and the value an answer of version 0 leaves in
     * {@see self::$leaderEpoch}, because that version does not carry the field.
     */
    public const int UNDEFINED_EPOCH = -1;

    /**
     * Error code of this partition
     */
    public int $errorCode;

    /**
     * Id of the partition
     */
    public int $partition;

    /**
     * Epoch that the answered `end_offset` belongs to, the field version 1 added (Kafka 2.0, KIP-279)
     *
     * `Log.endOffsetForEpoch` @ 2.8.2 answers the **largest epoch that is smaller than or equal to** the requested
     * one together with the offset that epoch ends at, so this is not necessarily the epoch that was asked for: a
     * leader that has no entry for the requested epoch reports the last one it does know about. It is
     * {@see self::UNDEFINED_EPOCH} next to an {@see self::UNDEFINED_EPOCH_OFFSET}, and it is that value in every
     * answer of version 0, which has no such field, see {@see OffsetForLeaderEpochResponsePartitionV0}.
     *
     * @since Version 1 of protocol
     */
    public int $leaderEpoch = self::UNDEFINED_EPOCH;

    /**
     * First offset that an epoch above the requested one owns, {@see self::UNDEFINED_EPOCH_OFFSET} when unknown
     */
    public int $endOffset;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $scheme = [
            'errorCode' => BinarySchema::TYPE_INT16,
            'partition' => BinarySchema::TYPE_INT32,
        ];
        if (static::VERSION >= 1) {
            $scheme['leaderEpoch'] = BinarySchema::TYPE_INT32;
        }
        $scheme['endOffset'] = BinarySchema::TYPE_INT64;

        return $scheme;
    }
}
