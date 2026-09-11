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

use Protocol\Kafka\Consumer\OffsetAndMetadata;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * OffsetFetchResponsePartition DTO, version 5 of the OffsetFetch API
 *
 * <pre>
 *   OffsetFetchResponsePartition => partition offset leader_epoch metadata error_code
 *     partition    => INT32
 *     offset       => INT64
 *     leader_epoch => INT32            -- since version 5
 *     metadata     => NULLABLE_STRING
 *     error_code   => INT16
 * </pre>
 *
 * **Version 5 (Kafka 2.1, KIP-320) inserted `committed_leader_epoch` between the offset and the metadata**: the
 * epoch of the leader the offset was committed from, as an OffsetCommit v6 sent it, or
 * {@see self::UNKNOWN_LEADER_EPOCH} for an offset that was committed without one - which is every offset a client
 * below Kafka 2.1 wrote. {@see OffsetFetchResponsePartitionV0} is the entry of the versions 0 to 4, which have no
 * such field at all.
 *
 * A topic-partition without a committed offset is not an error: the broker answers with the offset `-1`, empty
 * metadata and the error code 0 (v1 and v2); v0 reads from ZooKeeper and reports 3 (UnknownTopicOrPartition)
 * instead.
 *
 * The metadata is a `NULLABLE_STRING` on the wire, but a 0.10.2.2 broker never sends `null` for it: an offset that
 * was committed without metadata is stored as `OffsetMetadata.NoMetadata`, the empty string, and comes back as
 * `00 00`. A capture from an older broker can still carry `ff ff`, so both have to be handled.
 *
 * @see docs/protocol/2.8.md, section "OffsetFetch API (key 9, v0 to v5)"
 */
class OffsetFetchResponsePartition implements BinarySchemaInterface
{
    /**
     * Version of the OffsetFetch API that this DTO decodes an entry of
     */
    public const int VERSION = 5;

    /**
     * The `committed_leader_epoch` of an offset that was committed without one
     *
     * @since Version 5 of protocol
     */
    public const int UNKNOWN_LEADER_EPOCH = -1;

    /**
     * The partition this response entry corresponds to.
     */
    public int $partition;

    /**
     * The offset that was committed for this partition, or -1 if there is none.
     */
    public int $offset;

    /**
     * Epoch of the leader this offset was committed from, {@see self::UNKNOWN_LEADER_EPOCH} when it is not known.
     *
     * @since Version 5 of protocol
     */
    public int $leaderEpoch = self::UNKNOWN_LEADER_EPOCH;

    /**
     * Any associated metadata the client asked the broker to keep.
     */
    public ?string $metadata;

    /**
     * The error from this partition, if any.
     *
     * Errors are given on a per-partition basis because a given partition may be unavailable or maintained on a
     * different host, while others may have successfully accepted the request.
     */
    public int $errorCode;

    /**
     * Returns this entry as the value object a caller commits with, leader epoch included
     *
     * The epoch is `null` - the empty `Optional` of the Java `OffsetAndMetadata.leaderEpoch()` - for an entry that
     * carries {@see self::UNKNOWN_LEADER_EPOCH}, which is every offset committed before Kafka 2.1 and every answer
     * of a version below 5.
     */
    public function toOffsetAndMetadata(): OffsetAndMetadata
    {
        return new OffsetAndMetadata(
            $this->offset,
            $this->metadata,
            $this->leaderEpoch === self::UNKNOWN_LEADER_EPOCH ? null : $this->leaderEpoch
        );
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
        if (static::VERSION >= 5) {
            $scheme['leaderEpoch'] = BinarySchema::TYPE_INT32;
        }
        $scheme['metadata']  = BinarySchema::TYPE_NULLABLE_STRING;
        $scheme['errorCode'] = BinarySchema::TYPE_INT16;

        return $scheme;
    }
}
