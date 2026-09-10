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
use Protocol\Kafka\Protocol\Data\DeleteRecordsRequestTopic;

/**
 * DeleteRecords, version 0: deletes the records before an offset of a partition (ApiKey 21, Kafka 0.11, KIP-107)
 *
 * <pre>
 *   DeleteRecords Request (Version: 0) => [topics] timeout
 *     topics  => topic [partitions]
 *       topic      => STRING
 *       partitions => partition offset
 *         partition => INT32
 *         offset    => INT64
 *     timeout => INT32
 * </pre>
 *
 * KIP-107 gave an administrator what the retention settings of a topic could only do by the clock or by the size of
 * the log: it moves the **low watermark** (`logStartOffset`) of a partition forward, and the segments that are then
 * completely below it are deleted asynchronously. Everything BELOW `offset` goes away, the record at `offset` stays,
 * and the offset itself is what the answer reports back as the new low watermark.
 *
 * Like Produce and Fetch this api is served by the **leader** of each partition (`ReplicaManager.deleteRecords` @
 * 0.11.0.3 works on the local `Partition`), so one request has to be built per leader; a partition whose leader is
 * another broker is answered with the error code 6 (NotLeaderForPartition), and a topic that is not in the metadata
 * cache of the broker with 3 (UnknownTopicOrPartition), see `KafkaApis.handleDeleteRecordsRequest`.
 *
 * `timeout` is how long the leader waits for the low watermark to be acknowledged by every follower in the ISR
 * before it answers; nothing is rolled back when it expires, the partition is simply reported with the error code 7
 * (RequestTimedOut).
 *
 * @see docs/protocol/1.1.md, section "DeleteRecords API (key 21, v0)"
 */
class DeleteRecordsRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::DELETE_RECORDS;

    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * Deletes every record up to the high watermark of the partition, `DeleteRecordsRequest.HIGH_WATERMARK`
     */
    public const int HIGH_WATERMARK = -1;

    /**
     * Topics to delete records from, indexed by the topic name
     *
     * @var array<string, DeleteRecordsRequestTopic>
     */
    protected readonly array $topics;

    /**
     * A value of the `$topicPartitionOffsets` map is either a plain offset or an already built topic DTO.
     *
     * @param array<string, array<int, int>|DeleteRecordsRequestTopic> $topicPartitionOffsets Offset to delete
     *        before, as topic => partition => offset, where the offset is {@see self::HIGH_WATERMARK} or the first
     *        offset that has to survive
     * @param int    $timeout       How long the leader waits for the low watermark to be replicated, in ms
     * @param string $clientId      A user specified identifier for the client making the request
     * @param int    $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(
        array $topicPartitionOffsets,
        /**
         * Milliseconds the leader waits for the new low watermark before it answers
         */
        protected readonly int $timeout = 30000,
        string $clientId = '',
        int $correlationId = 0
    ) {
        $packedTopics = [];
        foreach ($topicPartitionOffsets as $topic => $partitionOffsets) {
            $packedTopics[$topic] = $partitionOffsets instanceof DeleteRecordsRequestTopic
                ? $partitionOffsets
                : new DeleteRecordsRequestTopic((string) $topic, $partitionOffsets);
        }
        $this->topics = $packedTopics;

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'topics'  => ['topic' => DeleteRecordsRequestTopic::class],
            'timeout' => BinarySchema::TYPE_INT32,
        ];
    }
}
