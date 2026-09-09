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
 * OffsetCommitRequestPartition DTO, version 2 of the OffsetCommit API
 *
 * <pre>
 *   OffsetCommitRequestPartition => partition offset metadata
 *     partition => INT32
 *     offset    => INT64
 *     metadata  => NULLABLE_STRING
 * </pre>
 *
 * The per-partition `timestamp` exists in **version 1 only**: version 0 never had it and version 2 replaced it with
 * the single `retention_time` field of the request (`OFFSET_COMMIT_REQUEST_PARTITION_V2` in `Protocol.java`
 * @ 0.10.2.2, `OffsetCommitRequest.readFrom` reads it for `versionId == 1`). The layout of version 2 is therefore
 * the layout of version 0 again, and the odd one out lives in {@see OffsetCommitRequestPartitionV1}; the scheme is
 * selected by {@see OffsetCommitRequestPartition::VERSION}.
 *
 * @see docs/protocol/0.11.0.md, section "OffsetCommit API (key 8, v0 to v3)"
 */
class OffsetCommitRequestPartition implements BinarySchemaInterface
{
    /**
     * Version of the OffsetCommit API that this DTO is packed for
     */
    public const int VERSION = 2;

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
     * Any associated metadata the client wants to keep.
     */
    public ?string $metadata;

    public function __construct(
        int $partition,
        int $offset,
        ?string $metadata = null,
        int $timestamp = self::BROKER_TIMESTAMP
    ) {
        $this->partition = $partition;
        $this->offset    = $offset;
        $this->metadata  = $metadata;
        $this->timestamp = $timestamp;
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
        if (static::VERSION === 1) {
            $scheme['timestamp'] = BinarySchema::TYPE_INT64;
        }
        $scheme['metadata'] = BinarySchema::TYPE_NULLABLE_STRING;

        return $scheme;
    }
}
