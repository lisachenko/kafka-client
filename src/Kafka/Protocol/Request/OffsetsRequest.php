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
 * Offsets API (key 2, v2), a.k.a. ListOffset
 *
 * This API describes the valid offset range available for a set of topic-partitions. As with the produce and fetch
 * APIs requests must be directed to the broker that is currently the leader for the partitions in question. This can
 * be determined using the metadata API.
 *
 * <pre>
 *   ListOffsets Request (Version: 2) => replica_id isolation_level [topics]
 *     replica_id      => INT32
 *     isolation_level => INT8       -- since version 2
 *     topics          => topic [partitions]
 *       topic      => STRING
 *       partitions => partition timestamp
 *         partition => INT32
 *         timestamp => INT64
 * </pre>
 *
 * Kafka 0.10.1 added version 1 with KIP-79, on top of the message timestamps of the format v1: the broker now
 * knows when every message was written, keeps a time index next to the offset index of each log segment, and can
 * therefore answer "the offset of the first message whose timestamp is `>= t`" instead of the segment start offsets
 * that {@see OffsetsRequestV0} asks for. One partition gets one offset, so the `max_num_offsets` of version 0 is
 * gone, and the answer names the timestamp of the message it found.
 *
 * Version 2 (KIP-98, Kafka 0.11) inserted `isolation_level` between the replica id and the topics, and it is the
 * transactional half of the api: with {@see \Protocol\Kafka\Protocol\Request\FetchRequest::READ_UNCOMMITTED} - the
 * default, and what every version below 2 gets - {@see self::LATEST} is the log end offset, while with
 * `READ_COMMITTED` it is the **last stable offset**, i.e. the offset of the first record that belongs to a
 * transaction that has neither committed nor aborted yet (`KafkaApis.fetchOffsetForTimestamp` @ 0.11.0.3 passes
 * the isolation level down to `Log.fetchOffsetsByTimestamp`). A consumer that reads committed data must therefore
 * ask for its end offsets with the same isolation level it fetches with, or it waits for records it will never be
 * shown. {@see OffsetsRequestV1} and {@see OffsetsRequestV0} do not put the field on the wire at all.
 *
 * The two special values keep their meaning in every version: {@see self::LATEST} (`-1`) asks for the end of the
 * log - the offset the next produced message will get, capped as the isolation level prescribes - and
 * {@see self::EARLIEST} (`-2`) for the first offset that is still on disk. Neither of them reads a message, so
 * their answer carries the timestamp -1.
 *
 * @see docs/protocol/1.1.md, section "Offsets API (key 2, v0, v1 and v2), a.k.a. ListOffset"
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
    public const int VERSION = 2;

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
     * @param int    $replicaId      The node id of the replica that initiates this request. Ordinary consumers send
     *                               {@see self::CONSUMER_REPLICA_ID}, as they have no node id.
     * @param int    $isolationLevel {@see FetchRequest::READ_UNCOMMITTED} or {@see FetchRequest::READ_COMMITTED},
     *                               not on the wire below version 2
     * @param string $clientId       Unique client identifier
     * @param int    $correlationId  Correlated request id
     */
    public function __construct(
        array $topicPartitions,
        protected readonly int $replicaId = self::CONSUMER_REPLICA_ID,
        /**
         * Whether the answer may name offsets of records that belong to an open transaction.
         *
         * @since Version 2 of protocol
         */
        protected readonly int $isolationLevel = FetchRequest::READ_UNCOMMITTED,
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
        int $isolationLevel = FetchRequest::READ_UNCOMMITTED,
        string $clientId = '',
        int $correlationId = 0
    ): static {
        return new static(
            self::partitionTimestamps($topicPartitions, $timestamp),
            $replicaId,
            $isolationLevel,
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
        $body   = [
            'replicaId' => BinarySchema::TYPE_INT32,
        ];
        if (static::VERSION >= 2) {
            $body['isolationLevel'] = BinarySchema::TYPE_INT8;
        }
        $body['topicPartitions'] = ['topic' => static::topicClass()];

        return $header + $body;
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
