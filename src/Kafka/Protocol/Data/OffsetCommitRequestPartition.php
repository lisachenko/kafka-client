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
 * OffsetCommitRequestPartition DTO, version 1 of the OffsetCommit API
 *
 * <pre>
 *   OffsetCommitRequestPartition => partition offset timestamp metadata
 *     partition => INT32
 *     offset    => INT64
 *     timestamp => INT64  (version 1 only)
 *     metadata  => NULLABLE_STRING
 * </pre>
 *
 * Version 0 of the request has no `timestamp` field, so the scheme is selected by {@see OffsetCommitRequestPartition::VERSION}
 * and {@see OffsetCommitRequestPartitionV0} only lowers that constant.
 *
 * @see docs/protocol/0.8.2.md, section "OffsetCommit API (key 8, v0 and v1)"
 */
class OffsetCommitRequestPartition implements BinarySchemaInterface
{
    /**
     * Version of the OffsetCommit API that this DTO is packed for
     */
    public const int VERSION = 1;

    /**
     * Asks the broker to stamp the commit with its own receive time.
     *
     * The offset is then retained for `offsets.retention.minutes` counted from the moment the broker received it.
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
        if (static::VERSION >= 1) {
            $scheme['timestamp'] = BinarySchema::TYPE_INT64;
        }
        $scheme['metadata'] = BinarySchema::TYPE_NULLABLE_STRING;

        return $scheme;
    }
}
