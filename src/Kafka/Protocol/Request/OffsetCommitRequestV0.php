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
 * OffsetCommit, version 0: the offsets are stored in ZooKeeper, as they were in Kafka 0.8.1.
 *
 * <pre>
 *   OffsetCommit Request (Version: 0) => group_id [topics]
 *     group_id => STRING
 *     topics   => topic [partitions]
 *       topic      => STRING
 *       partitions => partition offset metadata
 *         partition => INT32
 *         offset    => INT64
 *         metadata  => NULLABLE_STRING
 * </pre>
 *
 * The version 0 request has neither a generation id nor a consumer id, no retention time and no per-partition
 * timestamp; the scheme that {@see OffsetCommitRequest::getScheme()} builds follows the version constant below.
 * Unlike the later versions, this request does not have to be sent to the coordinator of the group: any broker of
 * the cluster answers it, because it only writes to ZooKeeper.
 *
 * @see docs/protocol/0.9.0.md, section "OffsetCommit API (key 8, v0, v1 and v2)"
 */
final class OffsetCommitRequestV0 extends OffsetCommitRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @param string $consumerGroup The consumer group id
     * @param array<string, array<int, int|OffsetAndMetadata|OffsetCommitRequestPartition>> $topicPartitions Offsets
     * @param string $clientId      Unique client identifier
     * @param int    $correlationId Correlated request id
     */
    public function __construct(
        string $consumerGroup,
        array $topicPartitions,
        string $clientId = '',
        int $correlationId = 0
    ) {
        parent::__construct(
            $consumerGroup,
            self::DEFAULT_GENERATION_ID,
            self::DEFAULT_MEMBER_NAME,
            self::DEFAULT_RETENTION_TIME,
            $topicPartitions,
            $clientId,
            $correlationId
        );
    }
}
