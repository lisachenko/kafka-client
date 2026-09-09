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
use Protocol\Kafka\Protocol\Data\OffsetsRequestTopicV0;

/**
 * Offsets API (key 2, v1), a.k.a. ListOffset
 *
 * This API describes the valid offset range available for a set of topic-partitions. As with the produce and fetch
 * APIs requests must be directed to the broker that is currently the leader for the partitions in question. This can
 * be determined using the metadata API.
 *
 * <pre>
 *   ListOffsets Request (Version: 1) => replica_id [topics]
 *     replica_id => INT32
 *     topics     => topic [partitions]
 *       topic      => STRING
 *       partitions => partition timestamp
 *         partition => INT32
 *         timestamp => INT64
 * </pre>
 *
 * Kafka 0.10.1 added this version with KIP-79, on top of the message timestamps of the format v1: the broker now
 * knows when every message was written, keeps a time index next to the offset index of each log segment, and can
 * therefore answer "the offset of the first message whose timestamp is `>= t`" instead of the segment start offsets
 * that {@see OffsetsRequestV0} asks for. One partition gets one offset, so the `max_num_offsets` of version 0 is
 * gone, and the answer names the timestamp of the message it found.
 *
 * The two special values keep their meaning in both versions: {@see self::LATEST} (`-1`) asks for the log end
 * offset - the offset the next produced message will get - and {@see self::EARLIEST} (`-2`) for the first offset
 * that is still on disk. Neither of them reads a message, so their answer carries the timestamp -1.
 *
 * @see docs/protocol/0.10.2.md, section "Offsets API (key 2, v0 and v1), a.k.a. ListOffset"
 */
class OffsetsRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::OFFSETS;

    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

    /**
     * Special value for the offset of the next coming message, `ListOffsetRequest.LATEST_TIMESTAMP` @ 0.10.2.2
     */
    public const int LATEST = -1;

    /**
     * Special value for receiving the earliest available offset, `ListOffsetRequest.EARLIEST_TIMESTAMP` @ 0.10.2.2
     */
    public const int EARLIEST = -2;

    /**
     * Replica id of an ordinary consumer, `ListOffsetRequest.CONSUMER_REPLICA_ID` @ 0.10.2.2.
     *
     * A consumer never sees an offset above the high watermark of the partition: the broker caps the answer of such
     * a request at it, because everything above it is not replicated yet.
     */
    public const int CONSUMER_REPLICA_ID = -1;

    /**
     * Replica id that lets a non-broker read like a follower, `ListOffsetRequest.DEBUGGING_REPLICA_ID` @ 0.10.2.2.
     *
     * The answer is then the state of the local log, uncapped by the high watermark, and the request is served by
     * any replica of the partition instead of its leader alone.
     */
    public const int DEBUGGING_REPLICA_ID = -2;

    /**
     * Topics to list the offsets of, indexed by the topic name
     *
     * @var array<string, OffsetsRequestTopic>
     */
    protected readonly array $topicPartitions;

    /**
     * A value of the `$topicPartitions` map is either a plain target time or an already built topic DTO.
     *
     * @param array<string, array<int, int>|OffsetsRequestTopic> $topicPartitions Target time of every partition, as
     *        topic => partition => time, where the time is a timestamp in milliseconds, {@see self::LATEST} or
     *        {@see self::EARLIEST}
     * @param int    $replicaId     The node id of the replica that initiates this request. Ordinary consumers send
     *                              {@see self::CONSUMER_REPLICA_ID}, as they have no node id.
     * @param string $clientId      Unique client identifier
     * @param int    $correlationId Correlated request id
     */
    public function __construct(
        array $topicPartitions,
        protected readonly int $replicaId = self::CONSUMER_REPLICA_ID,
        string $clientId = '',
        int $correlationId = 0
    ) {
        $topicClass            = static::topicClass();
        $packedTopicPartitions = [];
        foreach ($topicPartitions as $topic => $partitionTimestamps) {
            $packedTopicPartitions[$topic] = $partitionTimestamps instanceof OffsetsRequestTopic
                ? $partitionTimestamps
                : new $topicClass((string) $topic, $partitionTimestamps);
        }
        $this->topicPartitions = $packedTopicPartitions;

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * Builds a request that asks for the same target time for each of the given topic partitions
     *
     * @param iterable<TopicPartition> $topicPartitions Partitions to list the offsets of
     * @param int                      $timestamp       Timestamp in ms, {@see self::LATEST} or {@see self::EARLIEST}
     */
    public static function fromTopicPartitions(
        iterable $topicPartitions,
        int $timestamp = self::LATEST,
        int $replicaId = self::CONSUMER_REPLICA_ID,
        string $clientId = '',
        int $correlationId = 0
    ): static {
        return new static(
            self::partitionTimestamps($topicPartitions, $timestamp),
            $replicaId,
            $clientId,
            $correlationId
        );
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'replicaId'       => BinarySchema::TYPE_INT32,
            'topicPartitions' => ['topic' => static::topicClass()],
        ];
    }

    /**
     * Returns the class of a topic entry for the version of the API that this class sends
     *
     * @return class-string<OffsetsRequestTopic>
     */
    protected static function topicClass(): string
    {
        return static::VERSION >= 1 ? OffsetsRequestTopic::class : OffsetsRequestTopicV0::class;
    }

    /**
     * Spreads one target time over every one of the given topic partitions
     *
     * @param iterable<TopicPartition> $topicPartitions Partitions to list the offsets of
     *
     * @return array<string, array<int, int>> Target time as topic => partition => time
     */
    protected static function partitionTimestamps(iterable $topicPartitions, int $timestamp): array
    {
        $partitionTimestamps = [];
        foreach ($topicPartitions as $topicPartition) {
            $partitionTimestamps[$topicPartition->topic][$topicPartition->partition] = $timestamp;
        }

        return $partitionTimestamps;
    }
}
