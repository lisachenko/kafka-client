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
 * OffsetCommitRequestPartition DTO, version 6 of the OffsetCommit API
 *
 * <pre>
 *   OffsetCommitRequestPartition => partition offset leader_epoch metadata
 *     partition    => INT32
 *     offset       => INT64
 *     leader_epoch => INT32           -- since version 6
 *     metadata     => NULLABLE_STRING
 * </pre>
 *
 * The per-partition `timestamp` exists in **version 1 only**: version 0 never had it and version 2 replaced it with
 * the single `retention_time` field of the request (`OFFSET_COMMIT_REQUEST_PARTITION_V2` in `Protocol.java`
 * @ 0.10.2.2, `OffsetCommitRequest.readFrom` reads it for `versionId == 1`). The layout of version 2 is therefore
 * the layout of version 0 again, and the odd one out lives in {@see OffsetCommitRequestPartitionV1}; the scheme is
 * selected by {@see OffsetCommitRequestPartition::VERSION}.
 *
 * **Version 6 (Kafka 2.1, KIP-320) inserted `committed_leader_epoch` between the offset and the metadata**: the
 * epoch of the leader the committed offset was read from, so that a consumer which resumes from it can be told
 * that the log was truncated behind its back. A client that does not know the epoch sends
 * {@see self::UNKNOWN_LEADER_EPOCH}, which is what `OffsetCommitRequestPartitionV2` - the layout of the versions
 * 2 to 5 - carries implicitly, because it has no such field at all.
 *
 * @see docs/protocol/2.8.md, section "OffsetCommit API (key 8, v0 to v6)"
 */
class OffsetCommitRequestPartition implements BinarySchemaInterface
{
    /**
     * Version of the OffsetCommit API that this DTO is packed for
     */
    public const int VERSION = 6;

    /**
     * The `committed_leader_epoch` of a partition whose leader epoch the client does not know
     *
     * `RecordBatch.NO_PARTITION_LEADER_EPOCH` in the Java client, and the `default` of the field in
     * `OffsetCommitRequest.json` @ 2.8.2.
     *
     * @since Version 6 of protocol
     */
    public const int UNKNOWN_LEADER_EPOCH = -1;

    /**
     * Asks the broker to stamp the commit with its own receive time.
     *
     * Version 1 of the request carries this value per partition; version 2 has no such field any more, the broker
     * always stamps its own receive time and computes the expiry from `retention_time` instead.
     */
    public const int BROKER_TIMESTAMP = -1;

    /**
     * The partition this request entry corresponds to.
     */
    public int $partition;

    /**
     * The offset to commit for this partition.
     */
    public int $offset;

    /**
     * Commit timestamp in milliseconds, or {@see self::BROKER_TIMESTAMP} for the receive time of the broker.
     *
     * @since Version 1 of protocol
     * @deprecated Since version 2 of protocol, which replaced it with the `retentionTime` of the request
     */
    public int $timestamp;

    /**
     * Epoch of the leader this offset was read from, {@see self::UNKNOWN_LEADER_EPOCH} when it is not known.
     *
     * @since Version 6 of protocol
     */
    public int $leaderEpoch;

    /**
     * Any associated metadata the client wants to keep.
     */
    public ?string $metadata;

    public function __construct(
        int $partition,
        int $offset,
        ?string $metadata = null,
        int $timestamp = self::BROKER_TIMESTAMP,
        int $leaderEpoch = self::UNKNOWN_LEADER_EPOCH
    ) {
        $this->partition   = $partition;
        $this->offset      = $offset;
        $this->metadata    = $metadata;
        $this->timestamp   = $timestamp;
        $this->leaderEpoch = $leaderEpoch;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $scheme = [
            'partition' => BinarySchema::TYPE_INT32,
            'offset'    => BinarySchema::TYPE_INT64,
        ];
        if (static::VERSION >= 6) {
            $scheme['leaderEpoch'] = BinarySchema::TYPE_INT32;
        }
        if (static::VERSION === 1) {
            $scheme['timestamp'] = BinarySchema::TYPE_INT64;
        }
        $scheme['metadata'] = BinarySchema::TYPE_NULLABLE_STRING;

        return $scheme;
    }
}
