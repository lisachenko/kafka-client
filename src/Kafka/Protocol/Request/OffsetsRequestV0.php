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

use Protocol\Kafka\Common\TopicPartition;
use Protocol\Kafka\Protocol\Data\OffsetsRequestTopic;
use Protocol\Kafka\Protocol\Data\OffsetsRequestTopicV0;

/**
 * Offsets API (key 2, v0), a.k.a. ListOffset
 *
 * <pre>
 *   OffsetRequest => ReplicaId [TopicName [Partition Time MaxNumberOfOffsets]]
 *     ReplicaId          => int32
 *     TopicName          => string
 *     Partition          => int32
 *     Time               => int64
 *     MaxNumberOfOffsets => int32
 * </pre>
 *
 * The version that every broker before Kafka 0.10.1 spoke, and which a 0.11.0.3 broker still serves. It knows
 * nothing about the timestamps of the messages: an ordinary `Time` asks for the start offsets of the log segments
 * that were last modified before it, of which the broker returns up to `MaxNumberOfOffsets`, newest first. Only
 * {@see OffsetsRequest::LATEST} and {@see OffsetsRequest::EARLIEST} mean the same thing here as in version 1.
 *
 * `MaxNumberOfOffsets` is the reason this version has a class of its own: it lives in the partition entry, so it is
 * a parameter of the constructor instead of a field of the request, and the scheme of
 * {@see \Protocol\Kafka\Protocol\Data\OffsetsRequestPartitionV0} carries it. The `isolation_level` that version 2
 * added is not on the wire here either; a broker treats this version as `read_uncommitted`.
 *
 * @see docs/protocol/2.8.md, section "Offsets API (key 2, v0 to v3), a.k.a. ListOffset"
 */
final class OffsetsRequestV0 extends OffsetsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @param array<string, array<int, int>|OffsetsRequestTopic> $topicPartitions Target time of every partition, as
     *        topic => partition => time, where the time is a timestamp in milliseconds,
     *        {@see OffsetsRequest::LATEST} or {@see OffsetsRequest::EARLIEST}
     * @param int    $maxNumberOfOffsets Maximum number of offsets to return per partition
     * @param int    $replicaId          The node id of the replica that initiates this request
     * @param string $clientId           Unique client identifier
     * @param int    $correlationId      Correlated request id
     */
    public function __construct(
        array $topicPartitions,
        int $maxNumberOfOffsets = 1,
        int $replicaId = self::CONSUMER_REPLICA_ID,
        string $clientId = '',
        int $correlationId = 0
    ) {
        $packedTopicPartitions = [];
        foreach ($topicPartitions as $topic => $partitionTimestamps) {
            $packedTopicPartitions[$topic] = $partitionTimestamps instanceof OffsetsRequestTopic
                ? $partitionTimestamps
                : new OffsetsRequestTopicV0((string) $topic, $partitionTimestamps, $maxNumberOfOffsets);
        }

        parent::__construct(
            $packedTopicPartitions,
            $replicaId,
            FetchRequest::READ_UNCOMMITTED,
            $clientId,
            $correlationId
        );
    }

    /**
     * Builds a request that asks for the same target time for each of the given topic partitions
     *
     * `$maxNumberOfOffsets` comes last, because the parameters before it are the ones of
     * {@see OffsetsRequest::fromTopicPartitions()}, which this method overrides.
     *
     * @param iterable<TopicPartition> $topicPartitions    Partitions to list the offsets of
     * @param int                      $timestamp          Timestamp in ms, or one of the two special values
     * @param int                      $isolationLevel     Ignored: version 0 does not carry the field
     * @param int                      $maxNumberOfOffsets Maximum number of offsets to return per partition
     */
    public static function fromTopicPartitions(
        iterable $topicPartitions,
        int $timestamp = self::LATEST,
        int $replicaId = self::CONSUMER_REPLICA_ID,
        int $isolationLevel = FetchRequest::READ_UNCOMMITTED,
        string $clientId = '',
        int $correlationId = 0,
        int $maxNumberOfOffsets = 1
    ): static {
        return new self(
            self::partitionTimestamps($topicPartitions, $timestamp),
            $maxNumberOfOffsets,
            $replicaId,
            $clientId,
            $correlationId
        );
    }
}
