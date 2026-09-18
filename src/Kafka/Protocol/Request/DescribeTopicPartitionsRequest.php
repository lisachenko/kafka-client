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

use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\DescribeTopicPartitionsCursor;
use Protocol\Kafka\Protocol\Data\DescribeTopicPartitionsRequestTopic;
use Protocol\Kafka\Protocol\NullableStruct;

/**
 * DescribeTopicPartitions, version 0: the topics of a cluster, page by page (ApiKey 75, Kafka 3.8, KIP-966)
 *
 * <pre>
 *   DescribeTopicPartitions Request (Version: 0) => [topics] response_partition_limit cursor
 *     topics                   => name
 *       name                   => COMPACT_STRING
 *     response_partition_limit => INT32
 *     cursor                   => topic_name partition_index      -- nullable
 *       topic_name             => COMPACT_STRING
 *       partition_index        => INT32
 * </pre>
 *
 * The api KIP-966 gave `kafka-topics.sh --describe`, and the first one of this protocol that **pages**. A
 * {@see MetadataRequest} answers every partition of every topic it was asked for in one frame, which is how a
 * cluster of a hundred thousand partitions produces an answer no broker wants to build; this api answers at most
 * `response_partition_limit` partitions and hands back a `next_cursor` that the next request puts into its
 * `cursor` field. The limit is clamped by the broker into `[1, max.request.partition.size.limit]` (1000 on a
 * default node), so a request for 0 partitions is a request for one and a request for a million is a request for
 * a thousand.
 *
 * **An empty topic array asks for every topic of the cluster**, the internal ones included - the opposite of what
 * an empty array means in most apis of this protocol, and the same as the null topic array of Metadata.
 *
 * Two shapes of the cursor are refused with **42** (`InvalidRequest`) by
 * `DescribeTopicPartitionsRequestHandler.handleDescribeTopicPartitionsRequest` @ 3.9.2: a cursor whose topic is
 * not in the topic array of the same request (unless that array is empty), and a cursor whose partition index is
 * negative. A cursor that names a topic the cluster does not host is **not** an error - the topic is answered with
 * its own 3 - and a cursor whose partition index is past the end of its topic is not one either: the topic is
 * answered with no partition at all.
 *
 * This api is served by **any broker** - it reads the metadata cache, not a coordinator - and it is a
 * `broker`-only api: a KRaft controller does not serve it.
 *
 * @see docs/protocol/3.9.md, section "DescribeTopicPartitions API (key 75, v0)"
 */
class DescribeTopicPartitionsRequest extends AbstractRequest
{
    /**
     * Default of the `response_partition_limit` field, as the specification declares it
     */
    public const int DEFAULT_PARTITION_LIMIT = 2000;

    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::DESCRIBE_TOPIC_PARTITIONS;

    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Topics this request asks for, indexed by the topic name
     *
     * @var array<string, DescribeTopicPartitionsRequestTopic>
     */
    protected readonly array $topics;

    /**
     * @param list<string>                       $topics                 Topics to describe, empty for every topic
     * @param int                                $responsePartitionLimit Most partitions the answer may carry
     * @param DescribeTopicPartitionsCursor|null $cursor                 First topic and partition of the page
     * @param string                             $clientId               An identifier of the client
     * @param int                                $correlationId          A value the broker passes back unmodified
     */
    public function __construct(
        array $topics = [],
        /**
         * Maximum number of partitions the answer may carry, clamped by the broker
         */
        protected readonly int $responsePartitionLimit = self::DEFAULT_PARTITION_LIMIT,
        /**
         * First topic and partition index of the page, `null` to start at the beginning
         */
        protected readonly ?DescribeTopicPartitionsCursor $cursor = null,
        string $clientId = '',
        int $correlationId = 0
    ) {
        $entries = [];
        foreach ($topics as $topic) {
            $entries[$topic] = new DescribeTopicPartitionsRequestTopic($topic);
        }
        $this->topics = $entries;

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'topics'                 => ['name' => DescribeTopicPartitionsRequestTopic::class],
            'responsePartitionLimit' => BinarySchema::TYPE_INT32,
            'cursor'                 => new NullableStruct(DescribeTopicPartitionsCursor::class),
        ];
    }

    /**
     * Returns the names of the topics this request asks for, empty for every topic of the cluster
     *
     * @return list<string>
     */
    public function getTopics(): array
    {
        return array_keys($this->topics);
    }

    /**
     * Returns the maximum number of partitions this request allows in the answer
     */
    public function getResponsePartitionLimit(): int
    {
        return $this->responsePartitionLimit;
    }

    /**
     * Returns the first topic and partition of the page this request asks for, `null` for the beginning
     */
    public function getCursor(): ?DescribeTopicPartitionsCursor
    {
        return $this->cursor;
    }
}
