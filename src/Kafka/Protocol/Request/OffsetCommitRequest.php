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

/**
 * OffsetCommit, version 3: the offsets are stored in the `__consumer_offsets` topic of the cluster.
 *
 * This api saves out the consumer's position in the stream for one or more partitions. In the scala API this happens
 * when the consumer calls commit() or in the background if "autocommit" is enabled. This is the position the consumer
 * will pick up from if it crashes before its next commit().
 *
 * <pre>
 *   OffsetCommit Request (Version: 2 and 3) => group_id generation_id member_id retention_time [topics]
 *     group_id       => STRING
 *     generation_id  => INT32
 *     member_id      => STRING
 *     retention_time => INT64
 *     topics         => topic [partitions]
 *       topic      => STRING
 *       partitions => partition offset metadata
 *         partition => INT32
 *         offset    => INT64
 *         metadata  => NULLABLE_STRING
 * </pre>
 *
 * Version 2 replaced the per-partition `timestamp` of version 1 with one `retention_time` for the whole request
 * (`OFFSET_COMMIT_REQUEST_V2` in `Protocol.java` @ 0.11.0.3). With {@see self::DEFAULT_RETENTION_TIME} the broker
 * keeps the offsets for `offsets.retention.minutes`, otherwise for the given number of milliseconds counted from
 * the moment it received the commit, see `KafkaApis.handleOffsetCommitRequest`. A 0.11.0.3 broker serves the
 * versions 0 to 3 and closes the connection on anything above.
 *
 * Version 3 (KIP-124, Kafka 0.11) left the request alone - `OFFSET_COMMIT_REQUEST_V3 = OFFSET_COMMIT_REQUEST_V2` -
 * and only added the leading `throttle_time_ms` to the answer ({@see OffsetCommitResponse}).
 *
 * The lower versions differ in their scheme, and a scheme is a static property of a class, so each of them has a
 * class of its own that only lowers {@see OffsetCommitRequest::VERSION}: {@see OffsetCommitRequestV2},
 * {@see OffsetCommitRequestV1} and {@see OffsetCommitRequestV0}. Everything else - the fields, the class names and
 * the way the topic-partitions are packed - is shared.
 *
 * @see docs/protocol/0.11.0.md, section "OffsetCommit API (key 8, v0 to v3)"
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
    public const int VERSION = 3;

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
        if (static::VERSION >= 2) {
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
            static::VERSION >= 2  => OffsetCommitRequestTopic::class,
            static::VERSION === 1 => OffsetCommitRequestTopicV1::class,
            default               => OffsetCommitRequestTopicV0::class,
        };
    }
}
