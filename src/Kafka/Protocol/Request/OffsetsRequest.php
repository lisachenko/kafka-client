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
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\OffsetsRequestTopic;

/**
 * Offsets API (key 2, v0), a.k.a. ListOffset
 *
 * This API describes the valid offset range available for a set of topic-partitions. As with the produce and fetch
 * APIs requests must be directed to the broker that is currently the leader for the partitions in question. This can
 * be determined using the metadata API.
 *
 * The response contains the starting offset of each segment for the requested partition as well as the "log end
 * offset" i.e. the offset of the next message that would be appended to the given partition.
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
 * @see docs/protocol/0.8.2.md, section "Offsets API (key 2, v0), a.k.a. ListOffset"
 */
class OffsetsRequest extends AbstractRequest
{
    /**
     * Special value for the offset of the next coming message
     */
    public const int LATEST = -1;

    /**
     * Special value for receiving the earliest available offset
     */
    public const int EARLIEST = -2;

    /**
     * Topics to list the offsets of, indexed by the topic name
     *
     * @var array<string, OffsetsRequestTopic>
     */
    private readonly array $topicPartitions;

    /**
     * @param array<string, array<int, int>> $topicPartitions    Target time of every partition, as topic => partition
     *                                                           => time, where the time is a timestamp in
     *                                                           milliseconds, self::LATEST or self::EARLIEST
     * @param int                            $maxNumberOfOffsets Maximum number of offsets to return per partition
     * @param int                            $replicaId          The node id of the replica that initiates this
     *                                                           request. Ordinary consumers always send -1 as they
     *                                                           have no node id.
     */
    public function __construct(
        array $topicPartitions,
        int $maxNumberOfOffsets = 1,
        private readonly int $replicaId = -1,
        string $clientId = '',
        int $correlationId = 0
    ) {
        $packedTopicPartitions = [];
        foreach ($topicPartitions as $topic => $partitionTimestamps) {
            $packedTopicPartitions[$topic] = new OffsetsRequestTopic($topic, $partitionTimestamps, $maxNumberOfOffsets);
        }
        $this->topicPartitions = $packedTopicPartitions;

        parent::__construct(ApiKeys::OFFSETS, $clientId, $correlationId);
    }

    /**
     * Builds a request that asks for the same target time for each of the given topic partitions
     *
     * @param iterable<TopicPartition> $topicPartitions    Partitions to list the offsets of
     * @param int                      $timestamp          Timestamp in milliseconds, self::LATEST or self::EARLIEST
     * @param int                      $maxNumberOfOffsets Maximum number of offsets to return per partition
     */
    public static function fromTopicPartitions(
        iterable $topicPartitions,
        int $timestamp = self::LATEST,
        int $maxNumberOfOffsets = 1,
        int $replicaId = -1,
        string $clientId = '',
        int $correlationId = 0
    ): self {
        $partitionTimestamps = [];
        foreach ($topicPartitions as $topicPartition) {
            $partitionTimestamps[$topicPartition->topic][$topicPartition->partition] = $timestamp;
        }

        return new self($partitionTimestamps, $maxNumberOfOffsets, $replicaId, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'replicaId'       => BinarySchema::TYPE_INT32,
            'topicPartitions' => ['topic' => OffsetsRequestTopic::class],
        ];
    }
}
