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

/**
 * OffsetCommit, version 1: the offsets are stored in the `__consumer_offsets` topic of the cluster.
 *
 * This api saves out the consumer's position in the stream for one or more partitions. In the scala API this happens
 * when the consumer calls commit() or in the background if "autocommit" is enabled. This is the position the consumer
 * will pick up from if it crashes before its next commit().
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
 * Kafka 0.8.2.2 asserts that the version is either 0 or 1 and closes the connection on anything above, so the
 * `retention_time` field of the version 2 request does not exist here and the per-partition `timestamp` of the
 * version 1 request has not been replaced by it yet.
 *
 * The two versions differ in their scheme, and the scheme is a static property of the class, so version 0 lives in
 * {@see OffsetCommitRequestV0}, which only lowers {@see OffsetCommitRequest::VERSION}; everything else - the fields,
 * the class names and the way the topic-partitions are packed - is shared. That keeps the diff of this class against
 * the version 2 request of the later protocol lines down to the two version-dependent fields.
 *
 * @see docs/protocol/0.9.0.md, section "OffsetCommit API (key 8, v0 and v1)"
 */
class OffsetCommitRequest extends AbstractRequest
{
    /**
     * Generation id for a consumer that is not a member of a group.
     *
     * Kafka 0.8.2.2 has no group membership protocol at all - the API keys 11-14 arrived with 0.9 - so every commit
     * that this branch sends is the commit of a "simple consumer".
     */
    public const int DEFAULT_GENERATION_ID = -1;

    /**
     * Consumer id of a consumer that is not a member of a group: empty, and never null.
     */
    public const string DEFAULT_MEMBER_NAME = '';

    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

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

        parent::__construct(ApiKeys::OFFSET_COMMIT, $clientId, $correlationId);
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
        return static::VERSION >= 1 ? OffsetCommitRequestTopic::class : OffsetCommitRequestTopicV0::class;
    }
}
