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
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\OffsetCommitRequestPartition;
use Protocol\Kafka\Protocol\Data\OffsetCommitRequestTopic;
use Protocol\Kafka\Protocol\Data\OffsetCommitRequestTopicV0;
use Protocol\Kafka\Protocol\Data\OffsetCommitRequestTopicV1;
use Protocol\Kafka\Protocol\Data\OffsetCommitRequestTopicV2;

/**
 * OffsetCommit, version 6: the offsets are stored in the `__consumer_offsets` topic of the cluster.
 *
 * This api saves out the consumer's position in the stream for one or more partitions. In the scala API this happens
 * when the consumer calls commit() or in the background if "autocommit" is enabled. This is the position the consumer
 * will pick up from if it crashes before its next commit().
 *
 * <pre>
 *   OffsetCommit Request (Version: 6) => group_id generation_id member_id [topics]
 *     group_id       => STRING
 *     generation_id  => INT32
 *     member_id      => STRING
 *     topics         => topic [partitions]
 *       topic      => STRING
 *       partitions => partition offset leader_epoch metadata
 *         partition    => INT32
 *         offset       => INT64
 *         leader_epoch => INT32            -- since version 6
 *         metadata     => NULLABLE_STRING
 *
 *   OffsetCommit Request (Version: 2, 3 and 4) => group_id generation_id member_id retention_time [topics]
 *     retention_time => INT64              -- version 2 to 4 only
 * </pre>
 *
 * Version 2 replaced the per-partition `timestamp` of version 1 with one `retention_time` for the whole request
 * (`OFFSET_COMMIT_REQUEST_V2` in `Protocol.java` @ 0.11.0.3). With {@see self::DEFAULT_RETENTION_TIME} the broker
 * keeps the offsets for `offsets.retention.minutes`, otherwise for the given number of milliseconds counted from
 * the moment it received the commit, see `KafkaApis.handleOffsetCommitRequest`. A 2.8.2 broker still honours that
 * field at the versions 2 to 4 and writes the offset with the `__consumer_offsets` value schema **v1**, the only
 * one that has an `expire_timestamp`; a commit that leaves it at -1 is written with the value schema **v3**, which
 * has none at all (KIP-211).
 *
 * Version 3 (KIP-124, Kafka 0.11) left the request alone - `OFFSET_COMMIT_REQUEST_V3 = OFFSET_COMMIT_REQUEST_V2` -
 * and only added the leading `throttle_time_ms` to the answer ({@see OffsetCommitResponse}).
 *
 * Version 4 (KIP-219, Kafka 2.0) left it alone once more: `OffsetCommitRequest.json` @ 2.8.2 gives every field of
 * this frame the same `versions` for 2, 3 and 4, and the first change afterwards is the **removal** of
 * `retention_time` at version 5 (KIP-211, Kafka 2.1). What the higher number means is the throttling contract of
 * KIP-219: a broker that throttles a version 4 request sends the answer **first** and mutes the channel for the
 * delay afterwards, so a client that sends this version has to wait out `throttle_time_ms` itself. Version 4 is the
 * highest **non-flexible** version of the api - versions 5 to 8 belong to the later releases of the 2.x major - and
 * is what this client sends.
 *
 * The lower versions differ in their scheme, and a scheme is a static property of a class, so each of them has a
 * class of its own that only lowers {@see OffsetCommitRequest::VERSION}: {@see OffsetCommitRequestV3},
 * {@see OffsetCommitRequestV2}, {@see OffsetCommitRequestV1} and {@see OffsetCommitRequestV0}. Everything else -
 * the fields, the class names and the way the topic-partitions are packed - is shared.
 *
 * **Version 5 (Kafka 2.1, KIP-211) removes `retention_time` from the frame** - the field has the versions `2-4` in
 * `OffsetCommitRequest.json` @ 2.8.2, it is not sent as -1 - because the committed offsets of a group expire
 * `offsets.retention.minutes` after the **group** became empty from that release on, not a fixed time after each
 * commit. The `$retentionTime` a caller passes is therefore simply not written by the versions 5 and 6.
 *
 * **Version 6 (Kafka 2.1, KIP-320) gives every partition a `committed_leader_epoch`**, between the offset and the
 * metadata: the epoch of the leader the offset was read from, so that a consumer that resumes from it can be told
 * that the log was truncated behind its back (74 `FencedLeaderEpoch`, 75 `UnknownLeaderEpoch`). A client that does
 * not know the epoch sends {@see OffsetCommitRequestPartition::UNKNOWN_LEADER_EPOCH}, which is what an
 * {@see OffsetAndMetadata} without a `leaderEpoch` produces.
 *
 * @see docs/protocol/2.8.md, section "OffsetCommit API (key 8, v0 to v6)"
 */
class OffsetCommitRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::OFFSET_COMMIT;

    /**
     * Generation id for a consumer that is not a member of a group.
     *
     * A consumer that joined a group through the JoinGroup api (key 11, Kafka 0.9) has to commit with the generation
     * the coordinator assigned to it, otherwise the commit is refused with the error code 22 (IllegalGeneration).
     */
    public const int DEFAULT_GENERATION_ID = -1;

    /**
     * Consumer id of a consumer that is not a member of a group: empty, and never null.
     */
    public const string DEFAULT_MEMBER_NAME = '';

    /**
     * Asks the broker to keep the offsets for `offsets.retention.minutes` instead of a retention of its own.
     *
     * @since Version 2 of protocol
     */
    public const int DEFAULT_RETENTION_TIME = -1;

    /**
     * @inheritdoc
     */
    public const int VERSION = 6;

    /**
     * Offsets to commit, indexed by the topic they belong to.
     *
     * @var array<string, OffsetCommitRequestTopic>
     */
    protected readonly array $topicPartitions;

    /**
     * A value of the `$topicPartitions` map is either a plain offset, an {@see OffsetAndMetadata} or an already
     * built {@see OffsetCommitRequestPartition}.
     *
     * @param string $consumerGroup   The consumer group id
     * @param int    $generationId    The generation of the group, {@see self::DEFAULT_GENERATION_ID} without one
     * @param string $memberName      The member id assigned by the coordinator, empty without one
     * @param int    $retentionTime   How long to keep the offsets, {@see self::DEFAULT_RETENTION_TIME} for the
     *                                retention configured on the broker
     * @param array<string, array<int, int|OffsetAndMetadata|OffsetCommitRequestPartition>> $topicPartitions Offsets
     * @param string $clientId        Unique client identifier
     * @param int    $correlationId   Correlated request id
     */
    public function __construct(
        /**
         * The consumer group id.
         */
        protected readonly string $consumerGroup,
        /**
         * The generation of the group.
         *
         * @since Version 1 of protocol
         */
        protected readonly int $generationId,
        /**
         * The member id assigned by the group coordinator.
         *
         * @since Version 1 of protocol
         */
        protected readonly string $memberName,
        /**
         * Time period in ms to retain the offset.
         *
         * @since Version 2 of protocol
         */
        protected readonly int $retentionTime,
        array $topicPartitions,
        string $clientId = '',
        int $correlationId = 0
    ) {
        $topicClass            = static::topicClass();
        $packedTopicPartitions = [];
        foreach ($topicPartitions as $topic => $partitions) {
            $packedTopicPartitions[$topic] = $partitions instanceof OffsetCommitRequestTopic
                ? $partitions
                : new $topicClass((string) $topic, $partitions);
        }
        $this->topicPartitions = $packedTopicPartitions;

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = [
            'consumerGroup' => BinarySchema::TYPE_STRING,
        ];
        if (static::VERSION >= 1) {
            $body['generationId'] = BinarySchema::TYPE_INT32;
            $body['memberName']   = BinarySchema::TYPE_STRING;
        }
        if (static::VERSION >= 2 && static::VERSION <= 4) {
            $body['retentionTime'] = BinarySchema::TYPE_INT64;
        }
        $body['topicPartitions'] = ['topic' => static::topicClass()];

        return $header + $body;
    }

    /**
     * Returns the class of a topic entry for the version of the API that this class sends
     *
     * @return class-string<OffsetCommitRequestTopic>
     */
    protected static function topicClass(): string
    {
        return match (true) {
            static::VERSION >= 6  => OffsetCommitRequestTopic::class,
            static::VERSION >= 2  => OffsetCommitRequestTopicV2::class,
            static::VERSION === 1 => OffsetCommitRequestTopicV1::class,
            default               => OffsetCommitRequestTopicV0::class,
        };
    }
}
