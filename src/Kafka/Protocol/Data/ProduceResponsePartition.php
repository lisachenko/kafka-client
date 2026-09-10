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
 * Produce response partition DTO
 *
 * <pre>
 *   Partition ErrorCode Offset LogAppendTime
 *     Partition     => int32
 *     ErrorCode     => int16
 *     Offset        => int64
 *     LogAppendTime => int64
 * </pre>
 *
 * `LogAppendTime` arrived with version 2 of this API (Kafka 0.10.0, message format v1) and is absent from the
 * answer of a version 0 or 1 request, which is what {@see ProduceResponsePartitionV0} decodes. Version 3 of the
 * api (Kafka 0.11.0) left the partition entry untouched - `PRODUCE_RESPONSE_V3` **is** `PRODUCE_RESPONSE_V2` in
 * `Protocol.java` @ 0.11.0.3, verified against the broker - so there is no partition class of version 3: the
 * `LogStartOffset` that the Produce answer eventually got belongs to Kafka 1.0 (Produce v5).
 *
 * @see docs/protocol/1.1.md, section "Produce API (key 0, v0 to v3)"
 */
class ProduceResponsePartition implements BinarySchemaInterface
{
    /**
     * Version of the Produce API that this DTO is unpacked from
     */
    public const int VERSION = 2;

    /**
     * Value of `LogAppendTime` for a topic that stamps its records with a `CreateTime`, i.e. "no append time"
     */
    public const int NO_LOG_APPEND_TIME = -1;

    /**
     * The partition this response entry corresponds to.
     */
    public int $partition = 0;

    /**
     * The error from this partition, if any.
     *
     * Errors are given on a per-partition basis because a given partition may be unavailable or maintained on a
     * different host, while others may have successfully accepted the produce request.
     */
    public int $errorCode = 0;

    /**
     * The offset assigned to the first message in the message set appended to this partition.
     */
    public int $baseOffset = 0;

    /**
     * Time the broker assigned to every message of the appended set, or -1 when it kept the producer's timestamps.
     *
     * With `message.timestamp.type=LogAppendTime` on the topic the broker overwrites the timestamp of every message
     * it appends with its own clock and reports that value here, once for the whole set - every message of the set
     * carries it. With the default `CreateTime` the field is `-1` ({@see self::NO_LOG_APPEND_TIME}) and the producer
     * may assume that the timestamps it sent have been stored as they were.
     *
     * Unit is milliseconds since the beginning of the epoch (midnight Jan 1, 1970 UTC).
     *
     * @since Version 2 of protocol
     *
     * @see \Protocol\Kafka\Producer\RecordMetadata::$timestamp
     */
    public int $logAppendTime = self::NO_LOG_APPEND_TIME;

    /**
     * Milliseconds the broker delayed the answer this partition arrived in, because of a produce quota.
     *
     * This is **not** a field of the wire format - the Produce API reports its `ThrottleTime` once per response,
     * behind the topics array - and it is therefore not part of {@see self::getScheme()}. The client copies the
     * value of an answer onto every partition of it, because a batch is split by partition leaders and each of
     * those answers carries a throttle time of its own.
     *
     * @see \Protocol\Kafka\Producer\RecordMetadata::$throttleTimeMs
     * @see docs/protocol/1.1.md, section "Quotas and throttle time"
     */
    public int $throttleTimeMs = 0;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $scheme = [
            'partition'  => BinarySchema::TYPE_INT32,
            'errorCode'  => BinarySchema::TYPE_INT16,
            'baseOffset' => BinarySchema::TYPE_INT64,
        ];
        if (static::VERSION >= 2) {
            $scheme['logAppendTime'] = BinarySchema::TYPE_INT64;
        }

        return $scheme;
    }
}
