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
 * OffsetFetchResponsePartition DTO
 *
 * <pre>
 *   OffsetFetchResponsePartition => partition offset metadata error_code
 *     partition  => INT32
 *     offset     => INT64
 *     metadata   => NULLABLE_STRING
 *     error_code => INT16
 * </pre>
 *
 * A topic-partition without a committed offset is not an error: the broker answers with the offset `-1`, empty
 * metadata and the error code 0 (v1 and v2); v0 reads from ZooKeeper and reports 3 (UnknownTopicOrPartition)
 * instead.
 *
 * The metadata is a `NULLABLE_STRING` on the wire, but a 0.10.2.2 broker never sends `null` for it: an offset that
 * was committed without metadata is stored as `OffsetMetadata.NoMetadata`, the empty string, and comes back as
 * `00 00`. A capture from an older broker can still carry `ff ff`, so both have to be handled.
 *
 * @see docs/protocol/0.10.2.md, section "OffsetFetch API (key 9, v0, v1 and v2)"
 */
class OffsetFetchResponsePartition implements BinarySchemaInterface
{
    /**
     * The partition this response entry corresponds to.
     */
    public int $partition;

    /**
     * The offset that was committed for this partition, or -1 if there is none.
     */
    public int $offset;

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
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'partition' => BinarySchema::TYPE_INT32,
            'offset'    => BinarySchema::TYPE_INT64,
            'metadata'  => BinarySchema::TYPE_NULLABLE_STRING,
            'errorCode' => BinarySchema::TYPE_INT16,
        ];
    }
}
