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

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Consumer\OffsetAndMetadata;
use Protocol\Kafka\Protocol\Data\OffsetCommitRequestPartition;

/**
 * OffsetCommit, version 1: the offsets are stored in the `__consumer_offsets` topic, with a per-partition timestamp.
 *
 * <pre>
 *   OffsetCommit Request (Version: 1) => group_id generation_id member_id [topics]
 *     group_id      => STRING
 *     generation_id => INT32
 *     member_id     => STRING
 *     topics        => topic [partitions]
 *       topic      => STRING
 *       partitions => partition offset timestamp metadata
 *         partition => INT32
 *         offset    => INT64
 *         timestamp => INT64
 *         metadata  => NULLABLE_STRING
 * </pre>
 *
 * This is the version Kafka 0.8.2 introduced and the only one with a commit timestamp per partition: the broker
 * counts the retention of an offset from that timestamp instead of from its own receive time, unless the timestamp
 * is left at {@see OffsetCommitRequestPartition::BROKER_TIMESTAMP}. Version 2 replaced it with one `retention_time`
 * for the whole request, so this class lowers the version constant and drops that field again.
 *
 * @see docs/protocol/2.8.md, section "OffsetCommit API (key 8, v0 to v8)"
 */
final class OffsetCommitRequestV1 extends OffsetCommitRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

    /**
     * @param string $consumerGroup The consumer group id
     * @param int    $generationId  The generation of the group, {@see self::DEFAULT_GENERATION_ID} without one
     * @param string $memberName    The member id assigned by the coordinator, empty without one
     * @param array<string, array<int, int|OffsetAndMetadata|OffsetCommitRequestPartition>> $topicPartitions Offsets
     * @param string $clientId      Unique client identifier
     * @param int    $correlationId Correlated request id
     */
    public function __construct(
        string $consumerGroup,
        int $generationId,
        string $memberName,
        array $topicPartitions,
        string $clientId = '',
        int $correlationId = 0
    ) {
        parent::__construct(
            $consumerGroup,
            $generationId,
            $memberName,
            self::DEFAULT_RETENTION_TIME,
            $topicPartitions,
            $clientId,
            $correlationId
        );
    }
}
