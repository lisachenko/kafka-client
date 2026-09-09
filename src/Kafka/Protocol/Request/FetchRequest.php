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
use Protocol\Kafka\Protocol\Data\FetchRequestTopic;
use Protocol\Kafka\Protocol\Data\FetchRequestTopicPartition;

/**
 * Fetch API (key 1), version 1
 *
 * The fetch API is used to fetch a chunk of one or more logs for some topic-partitions. Logically one specifies the
 * topics, partitions, and starting offset at which to begin the fetch and gets back a chunk of messages. In general,
 * the return messages will have offsets larger than or equal to the starting offset. However, with compressed
 * messages, it's possible for the returned messages to have offsets smaller than the starting offset. The number of
 * such messages is typically small and the caller is responsible for filtering out those messages.
 *
 * Fetch requests follow a long poll model so they can be made to block for a period of time if sufficient data is not
 * immediately available.
 *
 * As an optimization the server is allowed to return a partial message at the end of the message set. Clients should
 * handle this case.
 *
 * <pre>
 *   FetchRequest (Version: 1) => ReplicaId MaxWaitTime MinBytes [TopicName [Partition FetchOffset MaxBytes]]
 *     ReplicaId   => int32
 *     MaxWaitTime => int32
 *     MinBytes    => int32
 * </pre>
 *
 * `FETCH_REQUEST_V1` of Kafka 0.9.0.1 is `FETCH_REQUEST_V0`: the body did not change, only the answer gained the
 * `ThrottleTimeMs` prefix, see {@see FetchResponse}. The version therefore only selects the layout of the response,
 * and {@see FetchRequestV0} keeps the version 0 pair available.
 *
 * The request-level `MaxBytes` (v3) and `IsolationLevel` (v4) of the later protocol versions do not exist here, the
 * only limit is the per-partition `MaxBytes`.
 *
 * @see docs/protocol/0.9.0.md, section "Fetch API (key 1, v0 and v1)"
 */
class FetchRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::FETCH;

    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

    /**
     * Topics to fetch from, indexed by the topic name
     *
     * @var array<string, FetchRequestTopic>
     */
    protected readonly array $topicPartitions;

    /**
     * @param array<string, array<int, int>> $topicPartitions Fetch offset of every partition, as topic => partition
     *                                                       => offset
     * @param int                            $maxWaitTime     The maximum amount of time in milliseconds to block
     *                                                        waiting if insufficient data is available at the time the
     *                                                        request is issued.
     * @param int                            $minBytes        The minimum number of bytes of messages that must be
     *                                                        available to give a response. With 0 the server always
     *                                                        responds immediately, with 1 as soon as at least one
     *                                                        partition has at least one byte of data, or when
     *                                                        $maxWaitTime is over.
     * @param int                            $maxBytes        The maximum number of bytes to include in the message set
     *                                                        of one partition. This bounds the size of the response,
     *                                                        but a 0.9.0.1 broker returns an empty message set instead
     *                                                        of a single message that is bigger than this limit.
     * @param int                            $replicaId       The node id of the replica that initiates this request.
     *                                                        Ordinary consumers always send -1 as they have no node
     *                                                        id; -2 is accepted from a non-broker that wants to fetch
     *                                                        as if it were a replica, for debugging purposes.
     */
    public function __construct(
        array $topicPartitions,
        protected readonly int $maxWaitTime,
        protected readonly int $minBytes,
        int $maxBytes,
        protected readonly int $replicaId = -1,
        string $clientId = '',
        int $correlationId = 0
    ) {
        $packedTopicPartitions = [];
        foreach ($topicPartitions as $topic => $partitionOffsets) {
            $partitions = [];
            foreach ($partitionOffsets as $partition => $fetchOffset) {
                $partitions[$partition] = new FetchRequestTopicPartition($partition, $fetchOffset, $maxBytes);
            }
            $packedTopicPartitions[$topic] = new FetchRequestTopic($topic, $partitions);
        }
        $this->topicPartitions = $packedTopicPartitions;

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * Builds a request from a list of topic partitions with the offset to start fetching each of them at
     *
     * @param iterable<array{TopicPartition, int}> $partitionOffsets Pairs of a topic partition and its fetch offset
     */
    public static function fromTopicPartitions(
        iterable $partitionOffsets,
        int $maxWaitTime,
        int $minBytes,
        int $maxBytes,
        int $replicaId = -1,
        string $clientId = '',
        int $correlationId = 0
    ): static {
        $topicPartitions = [];
        foreach ($partitionOffsets as [$topicPartition, $fetchOffset]) {
            $topicPartitions[$topicPartition->topic][$topicPartition->partition] = $fetchOffset;
        }

        return new static($topicPartitions, $maxWaitTime, $minBytes, $maxBytes, $replicaId, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'replicaId'       => BinarySchema::TYPE_INT32,
            'maxWaitTime'     => BinarySchema::TYPE_INT32,
            'minBytes'        => BinarySchema::TYPE_INT32,
            'topicPartitions' => ['topic' => FetchRequestTopic::class],
        ];
    }
}
